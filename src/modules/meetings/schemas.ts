import { z } from "zod";

export const meetingsInsertSchema = z.object({
    name: z.string().min(1, { message: "Name is required" }).optional(),
    agentId: z.string().min(1, { message: "Agent is required" }).optional(),
});

export const meetingsUpdateSchema = meetingsInsertSchema.extend({
    id: z.string().min(1, { message: "Id is required" }),
    status: z
    .enum(['upcoming', 'active', 'completed', 'processing', 'cancelled'])
    .optional()
});
