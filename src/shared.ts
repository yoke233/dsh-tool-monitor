/** Incrementally frame LF and CRLF streams without emitting partial lines. */
export class LineFramer {
  private remainder = ''

  push(chunk: string): string[] {
    if (chunk.length === 0) return []
    const parts = `${this.remainder}${chunk}`.split('\n')
    this.remainder = parts.pop() ?? ''
    return parts.map(line => line.endsWith('\r') ? line.slice(0, -1) : line)
  }

  flush(): string[] {
    if (this.remainder.length === 0) return []
    const line = this.remainder.endsWith('\r') ? this.remainder.slice(0, -1) : this.remainder
    this.remainder = ''
    return [line]
  }
}

/** Retain a UTF-8-safe tail within a byte budget. */
export function retainUtf8Tail(text: string, maxBytes: number): string {
  const bytes = Buffer.from(text)
  if (bytes.byteLength <= maxBytes) return text
  let start = bytes.byteLength - maxBytes
  while (start < bytes.byteLength && (bytes[start]! & 0xc0) === 0x80) start += 1
  return bytes.subarray(start).toString('utf8')
}

/** A consuming, UTF-8-safe tail buffer with an explicit loss marker. */
export class BoundedTextQueue {
  private text = ''
  private omitted = false

  constructor(private readonly maxBytes: number) {}

  push(chunk: string): void {
    if (chunk.length === 0) return
    const combined = `${this.text}${chunk}`
    if (Buffer.byteLength(combined) > this.maxBytes) this.omitted = true
    this.text = retainUtf8Tail(combined, this.maxBytes)
  }

  take(): string {
    if (this.text.length === 0 && !this.omitted) return ''
    const value = this.omitted
      ? `[some output was dropped from the monitor tee buffer]\n${this.text}`
      : this.text
    this.text = ''
    this.omitted = false
    return value
  }
}
