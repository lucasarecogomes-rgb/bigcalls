import type { J7SocialEvent } from "./types";

const DEFAULT_TTL_MS = 5 * 60 * 1000;
const MAX_ENTRIES = 2_000;

export class EventDeduplicator {
  private readonly seen = new Map<string, number>();

  constructor(private readonly ttlMs = DEFAULT_TTL_MS) {}

  async shouldSend(event: J7SocialEvent): Promise<boolean> {
    const now = Date.now();
    this.cleanup(now);

    const key = await this.eventKey(event);
    const previous = this.seen.get(key);
    if (previous && now - previous < this.ttlMs) {
      return false;
    }

    this.seen.set(key, now);
    this.trim();
    return true;
  }

  private async eventKey(event: J7SocialEvent): Promise<string> {
    const payload = [
      event.contractAddress || "",
      event.tweetUrl || "",
      event.rawText || ""
    ].join("|");
    const bytes = new TextEncoder().encode(payload);
    const digest = await crypto.subtle.digest("SHA-256", bytes);
    return Array.from(new Uint8Array(digest))
      .map((byte) => byte.toString(16).padStart(2, "0"))
      .join("");
  }

  private cleanup(now: number): void {
    for (const [key, timestamp] of this.seen.entries()) {
      if (now - timestamp >= this.ttlMs) {
        this.seen.delete(key);
      }
    }
  }

  private trim(): void {
    if (this.seen.size <= MAX_ENTRIES) {
      return;
    }

    const entries = Array.from(this.seen.entries())
      .sort((a, b) => a[1] - b[1])
      .slice(0, this.seen.size - MAX_ENTRIES);
    for (const [key] of entries) {
      this.seen.delete(key);
    }
  }
}
