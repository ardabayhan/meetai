import { useState } from "react";
import { StreamTheme, useCall } from "@stream-io/video-react-sdk";
import { CallLobby } from "./call-lobby";
import { CallActive } from "./call-active";
import { CallEnded } from "./call-ended";
import { useTRPC } from "@/trpc/client";
import { useMutation } from "@tanstack/react-query";

interface Props {
    meetingName: string;
    meetingId: string;
};

export const CallUI = ({ meetingName, meetingId }: Props) => {
    const call = useCall();
    const [show, setShow] = useState<"lobby" | "call" | "ended">("lobby");
    const trpc = useTRPC();
    const updateMeeting = useMutation(trpc.meetings.update.mutationOptions());
    const handleJoin = async () => {
        if (!call) return;

        await call.join();

        setShow("call");
    };

    const handleLeave = () => {
        if (!call) return;
        updateMeeting.mutate({id: meetingId, status: "completed"})
        call.endCall();
        setShow("ended");
    };

    return (
        <StreamTheme classID="h-full">
            {show === "lobby" && <CallLobby onJoin={handleJoin} />}
            {show === "call" && <CallActive onLeave={handleLeave} meetingName={meetingName} />}
            {show === "ended" && <CallEnded />}
        </StreamTheme>
    )
};
