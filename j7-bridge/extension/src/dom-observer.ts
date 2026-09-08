import { collectJ7Candidates, parseJ7Card } from "./j7-parser";
import type { DomObserver, J7SocialEvent } from "./types";

interface ObserverOptions {
  onEvent(event: J7SocialEvent): void | Promise<void>;
  scanIntervalMs?: number;
}

export function createDomObserver(options: ObserverOptions): DomObserver {
  const seenElements = new WeakSet<Element>();
  let observer: MutationObserver | null = null;
  let scanTimer = 0;
  let stopped = true;

  const scan = (root: ParentNode = document): void => {
    if (stopped) {
      return;
    }

    for (const candidate of collectJ7Candidates(root)) {
      if (seenElements.has(candidate)) {
        continue;
      }
      seenElements.add(candidate);
      const event = parseJ7Card(candidate);
      if (!event) {
        continue;
      }
      Promise.resolve(options.onEvent(event)).catch((error) => {
        console.warn("[j7-bridge] failed to handle event", error);
      });
    }
  };

  const start = (): void => {
    if (!stopped) {
      return;
    }
    stopped = false;
    scan(document);

    observer = new MutationObserver((mutations) => {
      for (const mutation of mutations) {
        for (const node of Array.from(mutation.addedNodes)) {
          if (node instanceof Element) {
            scan(node);
          }
        }
      }
    });
    observer.observe(document.documentElement, {
      childList: true,
      subtree: true
    });

    const intervalMs = Math.max(2_000, options.scanIntervalMs || 5_000);
    scanTimer = window.setInterval(() => scan(document), intervalMs);
  };

  const stop = (): void => {
    stopped = true;
    if (observer) {
      observer.disconnect();
      observer = null;
    }
    if (scanTimer) {
      window.clearInterval(scanTimer);
      scanTimer = 0;
    }
  };

  return { start, stop, scan };
}
