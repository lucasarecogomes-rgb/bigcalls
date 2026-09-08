export const J7_HOSTNAME = "j7tracker.io";

export const CARD_SELECTORS = [
  ".tweet-row",
  ".tweet-embed",
  ".tweet-card",
  "[data-tweet-id]",
  "[class*='tweet-card']",
  "[class*='token-card']",
  "[class*='contract-card']"
];

export const CONTRACT_SELECTORS = [
  "span.contract-address",
  "[data-address]",
  "[data-contract]",
  "[data-mint]",
  "a[href*='solscan.io/token/']",
  "a[href*='dexscreener.com/solana/']"
];

export const AUTHOR_SELECTORS = [
  "a.author-text",
  "[class*='author'] a[href*='x.com']",
  "[class*='author'] a[href*='twitter.com']",
  "a[href*='x.com/']",
  "a[href*='twitter.com/']"
];

export const TEXT_SELECTORS = [
  ".tweet-content",
  "[class*='tweet-content']",
  "[class*='tweet-text']",
  "[data-testid='tweetText']"
];

export const TOKEN_NAME_SELECTORS = [
  ".token-name",
  "[class*='token-name']",
  "[class*='coin-name']",
  "[class*='ticker-name']"
];

// TODO: tighten these selectors after validating the current production J7 DOM.
export const TOKEN_URL_HINTS = [
  "j7tracker.io",
  "dexscreener.com",
  "solscan.io/token",
  "birdeye.so/token"
];
