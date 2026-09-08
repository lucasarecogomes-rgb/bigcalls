import {
  AUTHOR_SELECTORS,
  CARD_SELECTORS,
  CONTRACT_SELECTORS,
  TEXT_SELECTORS,
  TOKEN_NAME_SELECTORS,
  TOKEN_URL_HINTS
} from "./selectors";
import type { J7SocialEvent, J7EventType } from "./types";

const SOLANA_CA_REGEX = /\b[1-9A-HJ-NP-Za-km-z]{32,44}\b/g;
const TICKER_REGEX = /\$[A-Za-z0-9_]{2,15}\b/g;
const X_STATUS_REGEX = /^https?:\/\/(?:www\.)?(?:x|twitter)\.com\/([^/?#\s]+)\/status\/(\d+)(?:[/?#].*)?$/i;
const X_PROFILE_REGEX = /^https?:\/\/(?:www\.)?(?:x|twitter)\.com\/([^/?#\s]+)\/?(?:[?#].*)?$/i;

export function parseJ7Card(element: Element): J7SocialEvent | null {
  const card = closestCard(element);
  if (!card) {
    return null;
  }

  const rawText = cleanText(textOf(card));
  const rawLinks = linksOf(card);
  const contractAddress = firstContractAddress(card, rawText, rawLinks);
  const ticker = firstTicker(rawText);
  const dexUrl = firstMatchingUrl(rawLinks, (url) => url.hostname.endsWith("dexscreener.com"));
  const tweetUrl = firstTweetUrl(rawLinks, card);
  const tokenUrl = firstTokenUrl(rawLinks);
  const username = detectUsername(card, rawLinks);
  const author = username ? `@${username}` : detectAuthorText(card);
  const text = detectTweetText(card) || rawText || null;
  const tokenName = detectTokenName(card, rawText, ticker);
  const eventType = detectEventType(card, tweetUrl, contractAddress, dexUrl);

  if (!contractAddress && !ticker && !tweetUrl && !dexUrl) {
    return null;
  }

  return {
    source: "j7tracker",
    eventType,
    author,
    username,
    text,
    rawText,
    contractAddress,
    ticker,
    tokenName,
    tweetUrl,
    dexUrl,
    tokenUrl,
    rawLinks,
    detectedAt: new Date().toISOString()
  };
}

export function collectJ7Candidates(root: ParentNode): Element[] {
  const result = new Set<Element>();
  if (root instanceof Element) {
    addCandidate(root, result);
  }

  for (const selector of CARD_SELECTORS) {
    root.querySelectorAll(selector).forEach((node) => addCandidate(node, result));
  }
  for (const selector of CONTRACT_SELECTORS) {
    root.querySelectorAll(selector).forEach((node) => {
      const card = closestCard(node);
      if (card) {
        result.add(card);
      }
    });
  }

  return Array.from(result);
}

function addCandidate(node: Element, result: Set<Element>): void {
  const card = closestCard(node);
  if (card) {
    result.add(card);
    return;
  }

  const text = cleanText(textOf(node));
  if (SOLANA_CA_REGEX.test(text) || TICKER_REGEX.test(text)) {
    result.add(node);
  }
  SOLANA_CA_REGEX.lastIndex = 0;
  TICKER_REGEX.lastIndex = 0;
}

function closestCard(element: Element): Element | null {
  for (const selector of CARD_SELECTORS) {
    const matched = element.matches(selector) ? element : element.closest(selector);
    if (matched) {
      return matched;
    }
  }
  return null;
}

function firstContractAddress(card: Element, rawText: string, rawLinks: string[]): string | null {
  for (const selector of CONTRACT_SELECTORS) {
    for (const element of Array.from(card.querySelectorAll(selector))) {
      const attributes = [
        element.getAttribute("data-address"),
        element.getAttribute("data-contract"),
        element.getAttribute("data-mint"),
        element.getAttribute("href"),
        textOf(element)
      ];
      for (const value of attributes) {
        const match = firstSolanaAddress(value || "");
        if (match) {
          return match;
        }
      }
    }
  }

  for (const link of rawLinks) {
    const match = firstSolanaAddress(link);
    if (match) {
      return match;
    }
  }

  return firstSolanaAddress(rawText);
}

function firstTicker(rawText: string): string | null {
  TICKER_REGEX.lastIndex = 0;
  const match = TICKER_REGEX.exec(rawText);
  return match ? match[0].toUpperCase() : null;
}

function firstSolanaAddress(value: string): string | null {
  SOLANA_CA_REGEX.lastIndex = 0;
  const match = SOLANA_CA_REGEX.exec(value);
  return match ? match[0] : null;
}

function firstTweetUrl(rawLinks: string[], card: Element): string | null {
  const contextLink = card.querySelector<HTMLAnchorElement>('a.context-link[href*="/status/"]');
  if (contextLink?.href) {
    return contextLink.href;
  }

  for (const raw of rawLinks) {
    const url = safeUrl(raw);
    if (url && X_STATUS_REGEX.test(url.toString())) {
      return url.toString();
    }
  }

  const tweetId = card.getAttribute("data-tweet-id") || card.closest("[data-tweet-id]")?.getAttribute("data-tweet-id") || "";
  const username = detectUsername(card, rawLinks);
  return tweetId && username ? `https://x.com/${username}/status/${tweetId}` : null;
}

function firstTokenUrl(rawLinks: string[]): string | null {
  for (const raw of rawLinks) {
    const url = safeUrl(raw);
    if (!url) {
      continue;
    }
    if (TOKEN_URL_HINTS.some((hint) => url.toString().includes(hint))) {
      return url.toString();
    }
  }
  return null;
}

function firstMatchingUrl(rawLinks: string[], predicate: (url: URL) => boolean): string | null {
  for (const raw of rawLinks) {
    const url = safeUrl(raw);
    if (url && predicate(url)) {
      return url.toString();
    }
  }
  return null;
}

function linksOf(card: Element): string[] {
  const links = new Set<string>();
  card.querySelectorAll<HTMLAnchorElement>("a[href]").forEach((link) => {
    const href = link.href || link.getAttribute("href") || "";
    const normalized = normalizeHref(href);
    if (normalized) {
      links.add(normalized);
    }
  });
  return Array.from(links);
}

function normalizeHref(value: string): string {
  const text = value.trim();
  if (!text || text.startsWith("javascript:")) {
    return "";
  }
  try {
    return new URL(text, window.location.href).toString();
  } catch (_error) {
    return "";
  }
}

function detectUsername(card: Element, rawLinks: string[]): string | null {
  for (const selector of AUTHOR_SELECTORS) {
    const link = card.querySelector<HTMLAnchorElement>(selector);
    const handle = sanitizeHandle(link?.getAttribute("href") || link?.textContent || "");
    if (handle) {
      return handle;
    }
  }

  for (const raw of rawLinks) {
    const url = safeUrl(raw);
    if (!url) {
      continue;
    }
    const statusMatch = X_STATUS_REGEX.exec(url.toString());
    if (statusMatch) {
      return sanitizeHandle(statusMatch[1]);
    }
    const profileMatch = X_PROFILE_REGEX.exec(url.toString());
    if (profileMatch) {
      return sanitizeHandle(profileMatch[1]);
    }
  }

  return null;
}

function detectAuthorText(card: Element): string | null {
  for (const selector of AUTHOR_SELECTORS) {
    const text = cleanText(textOf(card.querySelector(selector)));
    if (text) {
      return text.startsWith("@") ? text : `@${text}`;
    }
  }
  return null;
}

function detectTweetText(card: Element): string | null {
  for (const selector of TEXT_SELECTORS) {
    const text = cleanText(textOf(card.querySelector(selector)));
    if (text) {
      return text;
    }
  }
  return null;
}

function detectTokenName(card: Element, rawText: string, ticker: string | null): string | null {
  for (const selector of TOKEN_NAME_SELECTORS) {
    const text = cleanText(textOf(card.querySelector(selector)));
    if (text && text !== ticker) {
      return text;
    }
  }

  if (!ticker) {
    return null;
  }
  const tickerIndex = rawText.indexOf(ticker);
  if (tickerIndex <= 0) {
    return null;
  }
  const prefix = rawText.slice(Math.max(0, tickerIndex - 48), tickerIndex).trim();
  const words = prefix.split(/\s+/).slice(-4).join(" ");
  return words.length >= 2 ? words : null;
}

function detectEventType(
  card: Element,
  tweetUrl: string | null,
  contractAddress: string | null,
  dexUrl: string | null
): J7EventType {
  const className = String((card as HTMLElement).className || "").toLowerCase();
  if (className.includes("token") || (contractAddress && dexUrl && !tweetUrl)) {
    return "token_card_detected";
  }
  return "tweet_card_detected";
}

function sanitizeHandle(value: string): string | null {
  const match = String(value || "").match(/(?:twitter\.com|x\.com)\/@?([^/?#\s]+)|@([A-Za-z0-9_]{1,30})|^([A-Za-z0-9_]{1,30})$/i);
  const handle = String(match?.[1] || match?.[2] || match?.[3] || "")
    .replace(/^@+/, "")
    .trim();
  if (!handle || /^(home|explore|search|i|notifications|messages|settings|status)$/i.test(handle)) {
    return null;
  }
  return /^[A-Za-z0-9_]{1,30}$/.test(handle) ? handle : null;
}

function safeUrl(value: string): URL | null {
  try {
    return new URL(value);
  } catch (_error) {
    return null;
  }
}

function textOf(element: Element | null): string {
  if (!element) {
    return "";
  }
  return "innerText" in element
    ? String((element as HTMLElement).innerText || element.textContent || "")
    : String(element.textContent || "");
}

function cleanText(value: string): string {
  return String(value || "").replace(/\s+/g, " ").trim();
}
