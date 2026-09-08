export type J7EventType = "tweet_card_detected" | "token_card_detected";

export interface J7SocialEvent {
  source: "j7tracker";
  eventType: J7EventType;
  author: string | null;
  username: string | null;
  text: string | null;
  rawText: string;
  contractAddress: string | null;
  ticker: string | null;
  tokenName: string | null;
  tweetUrl: string | null;
  dexUrl: string | null;
  tokenUrl: string | null;
  rawLinks: string[];
  detectedAt: string;
}

export interface BridgeClientConfig {
  endpoint: string;
  authToken: string;
  timeoutMs: number;
}

export interface DomObserver {
  start(): void;
  stop(): void;
  scan(root?: ParentNode): void;
}
