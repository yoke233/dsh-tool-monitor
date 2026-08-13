/** Model-facing job_monitor tool over MonitorableJobRegistry. */
import type { Context } from '@deepseek-ai/cordis';
import z from '@deepseek-ai/schemastery';
export declare const name = "tool-monitor";
export declare const inject: string[];
declare module '@deepseek-ai/dsh-jobs' {
    interface JobKindMap {
        monitor: 'monitor';
    }
}
export interface Config {
    batchWindowMs?: number;
    maxBatchBytes?: number;
    maxBufferedBytes?: number;
    maxConsecutiveWakes?: number;
}
export declare const Config: z<Config>;
/** Register a subscription-only Monitor. No shell command or business process is started. */
export declare function apply(ctx: Context, input?: Config): void;
