/** Incrementally frame LF and CRLF streams without emitting partial lines. */
export declare class LineFramer {
    private remainder;
    push(chunk: string): string[];
    flush(): string[];
}
/** Retain a UTF-8-safe tail within a byte budget. */
export declare function retainUtf8Tail(text: string, maxBytes: number): string;
/** A consuming, UTF-8-safe tail buffer with an explicit loss marker. */
export declare class BoundedTextQueue {
    private readonly maxBytes;
    private text;
    private omitted;
    constructor(maxBytes: number);
    push(chunk: string): void;
    take(): string;
}
