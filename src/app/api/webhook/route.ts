import OpenAI from "openai";
import { and, eq, not } from "drizzle-orm";
import { NextRequest, NextResponse } from "next/server";
import { ChatCompletionMessageParam } from "openai/resources/index.mjs";
import {
  MessageNewEvent,
  CallEndedEvent,
  CallTranscriptionReadyEvent,
  CallRecordingReadyEvent,
  CallSessionParticipantLeftEvent,
  CallSessionStartedEvent,
} from "@stream-io/node-sdk";

import { db } from "@/db";
import { agents, meetings } from "@/db/schema";
import { streamVideo } from "@/lib/stream-video";
import { inngest } from "@/inngest/client";
import { generateAvatarUri } from "@/lib/avatar";
import { streamChat } from "@/lib/stream-chat";

const openaiClient = new OpenAI({ apiKey: process.env.OPENAI_API_KEY! });

function verifySignatureWithSDK(body: string, signature: string): boolean {
  return streamVideo.verifyWebhook(body, signature);
}

export async function POST(req: NextRequest) {
  const signature = req.headers.get("x-signature");
  const apiKey = req.headers.get("x-api-key");

  if (!signature || !apiKey) {
    return NextResponse.json(
      { error: "Missing signature or API key" },
      { status: 400 }
    );
  }

  const body = await req.text();

  if (!verifySignatureWithSDK(body, signature)) {
    return NextResponse.json({ error: "Invalid signature" }, { status: 401 });
  }

  let payload: unknown;
  try {
    payload = JSON.parse(body) as Record<string, unknown>;
  } catch {
    return NextResponse.json({ error: "Invalid JSON" }, { status: 400 });
  }

  const eventType = (payload as Record<string, unknown>)?.type;

  if (eventType === "call.session_started") {
    const event = payload as CallSessionStartedEvent;
    const meetingId = event.call.custom?.meetingId;

    if (!meetingId) {
      return NextResponse.json({ error: "Missing meetingId" }, { status: 400 });
    }

    const [existingMeeting] = await db
      .select()
      .from(meetings)
      .where(
        and(
          eq(meetings.id, meetingId),
          not(eq(meetings.status, "completed")),
          not(eq(meetings.status, "active")),
          not(eq(meetings.status, "cancelled")),
          not(eq(meetings.status, "processing"))
        )
      );

    if (!existingMeeting) {
      return NextResponse.json({ error: "Meeting not found" }, { status: 404 });
    }

    await db
      .update(meetings)
      .set({
        status: "active",
        startedAt: new Date(),
      })
      .where(eq(meetings.id, existingMeeting.id));

    const [existingAgent] = await db
      .select()
      .from(agents)
      .where(eq(agents.id, existingMeeting.agentId));

    if (!existingAgent) {
      return NextResponse.json({ error: "Agent not found" }, { status: 404 });
    }

    const call = streamVideo.video.call("default", meetingId);
    
    try {
      // connectOpenAi creates the client and calls connect() internally
      // We'll update session after connection but we need to ensure it's done properly
      const realtimeClient = await streamVideo.video.connectOpenAi({
        call,
        openAiApiKey: process.env.OPENAI_API_KEY!,
        agentUserId: existingAgent.id,
      });

      // Wait for session to be created first
      await realtimeClient.waitForSessionCreated();

      // Update session with agent instructions and enable turn detection
      // turn_detection enables the agent to detect when to speak
      // input_audio_transcription enables the agent to understand speech
      // Add language instruction to ensure agent speaks Turkish from the start
      const instructionsWithLanguage = `You are a Turkish-speaking AI assistant. You MUST always speak in Turkish (Türkçe) from the very beginning. Never speak in Spanish, English, or any other language. All your responses must be in Turkish.

${existingAgent.instructions}`;

      realtimeClient.updateSession({
        instructions: instructionsWithLanguage,
        turn_detection: {
          type: "server_vad",
          threshold: 0.5,
          prefix_padding_ms: 300,
          silence_duration_ms: 500,
        },
        input_audio_transcription: {
          model: "gpt-4o-transcribe" as any, // Stream Video wrapper supports this, but OpenAI types only allow "whisper-1"
          language: "tr", // Turkish language for agent transcription
        } as any, // Stream Video wrapper supports language property
      });

      // Wait a bit to ensure the update is processed
      await new Promise(resolve => setTimeout(resolve, 500));
      
      console.log(`[Webhook] Agent ${existingAgent.id} connected to meeting ${meetingId}, session created, instructions: "${existingAgent.instructions.substring(0, 50)}..."`);
    } catch (error) {
      console.error(`[Webhook] Error connecting agent to meeting ${meetingId}:`, error);
      // Don't throw - we still want to return success to Stream
      // The meeting can continue without the agent
    }
  } else if (eventType === "call.session_participant_left") {
    const event = payload as CallSessionParticipantLeftEvent;
    const meetingId = event.call_cid.split(":")[1]; // call_cid is formatted as "type:id"

    if (!meetingId) {
      return NextResponse.json({ error: "Missing meetingId" }, { status: 400 });
    }

    const call = streamVideo.video.call("default", meetingId);
    await call.end();
  } else if (eventType === "call.session_ended") {
    const event = payload as CallEndedEvent;
    const meetingId = event.call.custom?.meetingId;

    if (!meetingId) {
      return NextResponse.json({ error: "Missing meetingId" }, { status: 400 });
    }

    await db
      .update(meetings)
      .set({
        status: "processing",
        endedAt: new Date(),
      })
      .where(and(eq(meetings.id, meetingId), eq(meetings.status, "active")));
  } else if (eventType === "call.transcription_ready") {
    const event = payload as CallTranscriptionReadyEvent;
    const meetingId = event.call_cid.split(":")[1]; // call_cid is formatted as "type:id"

    const [updatedMeeting] = await db
      .update(meetings)
      .set({
        transcriptUrl: event.call_transcription.url,
      })
      .where(eq(meetings.id, meetingId))
      .returning();

    if (!updatedMeeting) {
      return NextResponse.json({ error: "Meeting not found" }, { status: 404 });
    }

    await inngest.send({
      name: "meetings/processing",
      data: {
        meetingId: updatedMeeting.id,
        transcriptUrl: updatedMeeting.transcriptUrl,
      },
    });
  } else if (eventType === "call.recording_ready") {
    const event = payload as CallRecordingReadyEvent;
    const meetingId = event.call_cid.split(":")[1]; // call_cid is formatted as "type:id"

    await db
      .update(meetings)
      .set({
        recordingUrl: event.call_recording.url,
      })
      .where(eq(meetings.id, meetingId));
  } else if (eventType === "message.new") {
    const event = payload as MessageNewEvent;

    const userId = event.user?.id;
    const channelId = event.channel_id;
    const text = event.message?.text;
    const messageId = event.message?.id;

    if (!userId || !channelId || !text || !messageId) {
      return NextResponse.json(
        { error: "Missing required fields" },
        { status: 400 }
      );
    }

    // Get meeting and agent
    const [existingMeeting] = await db
      .select()
      .from(meetings)
      .where(and(eq(meetings.id, channelId), eq(meetings.status, "completed")));

    if (!existingMeeting) {
      return NextResponse.json({ error: "Meeting not found" }, { status: 404 });
    }

    const [existingAgent] = await db
      .select()
      .from(agents)
      .where(eq(agents.id, existingMeeting.agentId));

    if (!existingAgent) {
      return NextResponse.json({ error: "Agent not found" }, { status: 404 });
    }

    // Early return if this is an agent message (prevent agent from responding to itself)
    if (userId === existingAgent.id) {
      console.log(`[Webhook] Skipping agent's own message ${messageId}`);
      return NextResponse.json({ status: "ok", skipped: "agent_message" });
    }

    // Only respond to user messages
    const instructions = `
      Sen, kullanıcının tamamlanmış bir toplantıyı tekrar gözden geçirmesine yardımcı olan bir AI asistanısın.
      Aşağıda, transkriptten oluşturulmuş toplantı özeti bulunmaktadır:
      
      ${existingMeeting.summary}
      
      Aşağıdakiler, canlı toplantı asistanından gelen orijinal talimatlarındır. Kullanıcıya yardımcı olurken bu davranışsal yönergeleri takip etmeye devam et:
      
      ${existingAgent.instructions}
      
      Kullanıcı toplantı hakkında sorular sorabilir, açıklamalar isteyebilir veya takip eylemleri talep edebilir.
      Her zaman yukarıdaki toplantı özetine dayanarak cevap ver.
      
      Ayrıca sen ve kullanıcı arasındaki son konuşma geçmişine de erişimin var. Önceki mesajların bağlamını kullanarak ilgili, tutarlı ve yardımcı cevaplar ver. Kullanıcının sorusu daha önce tartışılan bir şeye atıfta bulunuyorsa, bunu dikkate al ve konuşmada sürekliliği koru.
      
      Özet bir soruyu cevaplamak için yeterli bilgi içermiyorsa, kullanıcıya nazikçe bildir.
      
      Özlü, yardımcı ol ve toplantıdan ve devam eden konuşmadan doğru bilgilere odaklan.
      
      ÖNEMLİ: Her zaman Türkçe cevap ver. Hiçbir zaman İngilizce veya başka bir dilde cevap verme.
      `;

      const channel = streamChat.channel("messaging", channelId);
      await channel.watch();

      // Check if we've already responded to this exact message to prevent duplicates
      const currentMessageId = event.message?.id;
      const recentAgentMessages = channel.state.messages
        .filter((msg) => 
          msg.user?.id === existingAgent.id && 
          msg.text && 
          msg.text.trim() !== "" &&
          msg.id !== currentMessageId // Exclude current message if it's from agent
        )
        .slice(-5); // Check last 5 agent messages

      // Check if we've already responded recently (within last 10 seconds)
      const now = Date.now();
      const veryRecentResponse = recentAgentMessages.find((msg) => {
        const msgTime = msg.created_at ? new Date(msg.created_at).getTime() : 0;
        return (now - msgTime) < 10000; // Within last 10 seconds
      });

      if (veryRecentResponse) {
        console.log(`[Webhook] Skipping duplicate response - recent response found within 10 seconds for message ${currentMessageId}`);
        return NextResponse.json({ status: "ok", skipped: "duplicate_recent" });
      }

      // Also check if we've already processed this exact message ID (prevent processing same message twice)
      // Check if there's already an agent response that was created right after this user message
      const userMessageTime = event.message?.created_at ? new Date(event.message.created_at).getTime() : 0;
      const agentResponseAfterThis = channel.state.messages.find((msg) => 
        msg.user?.id === existingAgent.id && 
        msg.text && 
        msg.text.trim() !== "" &&
        msg.created_at && 
        new Date(msg.created_at).getTime() > userMessageTime &&
        (new Date(msg.created_at).getTime() - userMessageTime) < 30000 // Within 30 seconds of user message
      );

      if (agentResponseAfterThis) {
        console.log(`[Webhook] Skipping duplicate response - agent already responded to message ${currentMessageId}`);
        return NextResponse.json({ status: "ok", skipped: "duplicate_already_responded" });
      }

      // Get conversation history in chronological order, excluding the current message
      // Remove duplicate consecutive messages to avoid repetition
      const conversationHistory: ChatCompletionMessageParam[] = [];
      const allMessagesSorted = [...channel.state.messages]
        .filter((msg) => 
          msg.text && 
          msg.text.trim() !== "" && 
          msg.id !== event.message?.id // Exclude the current message
        )
        .sort((a, b) => {
          const timeA = a.created_at ? new Date(a.created_at).getTime() : 0;
          const timeB = b.created_at ? new Date(b.created_at).getTime() : 0;
          return timeA - timeB;
        })
        .slice(-12); // Last 12 messages for context

      // Remove duplicate consecutive messages (same user, similar content)
      let lastMessage: { role: string; content: string } | null = null;
      allMessagesSorted.forEach((msg) => {
        const role = msg.user?.id === existingAgent.id ? "assistant" : "user";
        const content = msg.text || "";
        
        // Skip if this is a duplicate of the last message (same role and very similar content)
        if (lastMessage && 
            lastMessage.role === role && 
            lastMessage.content.trim().toLowerCase() === content.trim().toLowerCase()) {
          return; // Skip duplicate
        }
        
        conversationHistory.push({ role, content });
        lastMessage = { role, content };
      });

      // Limit to last 10 messages to avoid token limits
      const finalHistory = conversationHistory.slice(-10);

      const GPTResponse = await openaiClient.chat.completions.create({
        messages: [
          { role: "system", content: instructions },
          ...finalHistory,
          { role: "user", content: text },
        ],
        model: "gpt-4o",
        temperature: 0.7, // Add some variation to responses
      });

      const GPTResponseText = GPTResponse.choices[0].message.content;

      if (!GPTResponseText) {
        return NextResponse.json(
            { error: "No response from GPT" },
            { status: 400 }
        );
      }

      const avatarUrl = generateAvatarUri({
        seed: existingAgent.name,
        variant: "botttsNeutral",
      });

      streamChat.upsertUser({
        id: existingAgent.id,
        name: existingAgent.name,
        image: avatarUrl,
      });

      channel.sendMessage({
        text: GPTResponseText,
        user: {
            id: existingAgent.id,
            name: existingAgent.name,
            image: avatarUrl,
        },
      });
  }

  return NextResponse.json({ status: "ok" });
}
