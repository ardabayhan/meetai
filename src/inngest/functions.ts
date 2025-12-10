import { eq, inArray } from "drizzle-orm";
import JSONL from "jsonl-parse-stringify";
import { createAgent, openai, TextMessage } from "@inngest/agent-kit";

import { db } from "@/db";
import { agents, meetings, user } from "@/db/schema";
import { inngest } from "@/inngest/client";

import { StreamTranscriptItem } from "@/modules/meetings/types";

const summarizer = createAgent({
  name: "summarizer",
  system: `
    Sen uzman bir özetleme asistanısın. Okunabilir, özlü ve basit içerikler yazarsın. Sana bir toplantı transkripti verilir ve bunu özetlemen gerekir.

Her çıktı için aşağıdaki markdown yapısını kullan:

### Genel Bakış
Oturum içeriğinin detaylı ve ilgi çekici bir özetini sun. Ana özellikler, kullanıcı iş akışları ve önemli çıkarımlara odaklan. Hikaye anlatımı tarzında, tam cümlelerle yaz. Ürün, platform veya tartışmanın benzersiz veya güçlü yönlerini vurgula.

### Notlar
Ana içeriği zaman damgalı tematik bölümlere ayır. Her bölüm, ana noktaları, eylemleri veya demoları madde işareti formatında özetlemelidir.

Örnek:
#### Bölüm Adı
- Burada gösterilen ana nokta veya demo
- Başka bir önemli içgörü veya etkileşim
- Sağlanan takip aracı veya açıklama

#### Sonraki Bölüm
- X özelliği otomatik olarak Y yapar
- Z ile entegrasyondan bahsedilir

ÖNEMLİ: Tüm özeti Türkçe yaz. Hiçbir zaman İngilizce veya başka bir dilde yazma.
  `.trim(),
  model: openai({ model: "gpt-4o", apiKey: process.env.OPENAI_API_KEY }),
});

export const meetingsProcessing = inngest.createFunction(
  { id: "meetings/processing" },
  { event: "meetings/processing" },
  async ({ event, step }) => {
    console.log(`[Inngest] Processing meeting ${event.data.meetingId}, transcript URL: ${event.data.transcriptUrl}`);

    const response = await step.fetch(event.data.transcriptUrl);

    if (!response.ok) {
      throw new Error(`Failed to fetch transcript: ${response.status} ${response.statusText}`);
    }

    const transcript = await step.run("parse-transcript", async () => {
      const text = await response.text();
      if (!text || text.trim() === "") {
        throw new Error("Transcript is empty");
      }
      // Burada StreamTranscriptItem tipinin tanımlı olduğunu varsayıyoruz
      return JSONL.parse<StreamTranscriptItem>(text);
    });

    const transcriptWithSpeakers = await step.run("add-speakers", async () => {
      const speakerIds = [
        ...new Set(transcript.map((item) => item.speaker_id))
      ];

      const userSpeakers = await db
        .select()
        .from(user)
        .where(inArray(user.id, speakerIds))
        .then((users) =>
          users.map((user) => ({
            ...user,
          }))
        );

      const agentSpeakers = await db
        .select()
        .from(agents)
        .where(inArray(agents.id, speakerIds))
        .then((agents) =>
          agents.map((agent) => ({
            ...agent,
          }))
        );

      const speakers = [...userSpeakers, ...agentSpeakers];

      return transcript.map((item) => {
        const speaker = speakers.find(
          (speaker) => speaker.id === item.speaker_id
        );

        if (!speaker) {
          return {
            ...item,
            user: {
              name: "Unknown",
            },
          };
        }

        return {
          ...item,
          user: {
            name: speaker.name,
          },
        };
      });
    });

    // Generate summary - summarizer.run() uses step.* internally, so we can't nest it in step.run()
    console.log(`[Inngest] Generating summary for meeting ${event.data.meetingId}`);
    const { output } = await summarizer.run(
      "Aşağıdaki transkripti Türkçe olarak özetle. Tüm özet Türkçe olmalı: " +
        JSON.stringify(transcriptWithSpeakers)
    );

    await step.run("save-summary", async () => {
      const summaryContent = (output[0] as TextMessage).content as string;
      console.log(`[Inngest] Saving summary for meeting ${event.data.meetingId}, length: ${summaryContent.length}`);
      
      await db
        .update(meetings)
        .set({
          summary: summaryContent,
          status: "completed",
        })
        .where(eq(meetings.id, event.data.meetingId));
      
      console.log(`[Inngest] Summary saved successfully for meeting ${event.data.meetingId}`);
    });

  },
);

