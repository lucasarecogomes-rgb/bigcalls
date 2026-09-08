import type { BridgeClientConfig, J7SocialEvent } from "./types";

const DEFAULT_ENDPOINT = "http://127.0.0.1:8790/webhooks/social/j7";
const DEFAULT_TIMEOUT_MS = 2_500;

declare const chrome:
  | {
      storage?: {
        local?: {
          get(keys: string[]): Promise<Record<string, unknown>>;
        };
      };
    }
  | undefined;

export class BridgeClient {
  async send(event: J7SocialEvent): Promise<void> {
    const config = await this.loadConfig();
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), config.timeoutMs);

    try {
      const headers: Record<string, string> = {
        "content-type": "application/json"
      };
      if (config.authToken) {
        headers.authorization = `Bearer ${config.authToken}`;
        headers["x-social-bridge-token"] = config.authToken;
      }

      const response = await fetch(config.endpoint, {
        method: "POST",
        headers,
        body: JSON.stringify(event),
        signal: controller.signal,
        keepalive: true
      });

      if (!response.ok) {
        throw new Error(`bridge http ${response.status}`);
      }
    } finally {
      window.clearTimeout(timeout);
    }
  }

  private async loadConfig(): Promise<BridgeClientConfig> {
    const stored = await this.safeStorageGet([
      "bigcallsEndpoint",
      "bigcallsAuthToken",
      "bigcallsTimeoutMs"
    ]);

    return {
      endpoint: normalizeEndpoint(stored.bigcallsEndpoint),
      authToken: normalizeText(stored.bigcallsAuthToken),
      timeoutMs: normalizeTimeout(stored.bigcallsTimeoutMs)
    };
  }

  private async safeStorageGet(keys: string[]): Promise<Record<string, unknown>> {
    try {
      if (typeof chrome === "undefined" || !chrome.storage?.local) {
        return {};
      }
      return await chrome.storage.local.get(keys);
    } catch (_error) {
      return {};
    }
  }
}

function normalizeEndpoint(value: unknown): string {
  const text = normalizeText(value);
  if (!text) {
    return DEFAULT_ENDPOINT;
  }

  try {
    const url = new URL(text);
    if (!["127.0.0.1", "localhost"].includes(url.hostname)) {
      return DEFAULT_ENDPOINT;
    }
    if (url.protocol !== "http:" && url.protocol !== "https:") {
      return DEFAULT_ENDPOINT;
    }
    return url.toString();
  } catch (_error) {
    return DEFAULT_ENDPOINT;
  }
}

function normalizeTimeout(value: unknown): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return DEFAULT_TIMEOUT_MS;
  }
  return Math.max(500, Math.min(10_000, parsed));
}

function normalizeText(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}
