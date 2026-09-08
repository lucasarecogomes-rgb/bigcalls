import { BridgeClient } from "./bridge-client";
import { createDomObserver } from "./dom-observer";
import { EventDeduplicator } from "./event-deduplicator";
import { J7_HOSTNAME } from "./selectors";
import type { J7SocialEvent } from "./types";

const BRIDGE_TAG = "[j7-bridge]";

function isJ7Page(): boolean {
  return window.location.hostname === J7_HOSTNAME;
}

if (isJ7Page()) {
  const client = new BridgeClient();
  const deduplicator = new EventDeduplicator();

  const observer = createDomObserver({
    async onEvent(event: J7SocialEvent) {
      if (!(await deduplicator.shouldSend(event))) {
        return;
      }
      try {
        await client.send(event);
        console.debug(BRIDGE_TAG, "event sent", {
          type: event.eventType,
          username: event.username,
          contractAddress: event.contractAddress,
          ticker: event.ticker
        });
      } catch (error) {
        console.warn(BRIDGE_TAG, "failed to send event", error);
      }
    }
  });

  observer.start();

  window.addEventListener("pagehide", () => observer.stop(), { once: true });
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") {
      observer.scan(document);
    }
  });
}
