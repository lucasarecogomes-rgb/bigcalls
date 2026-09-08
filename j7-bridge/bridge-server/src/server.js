import http from "node:http";
import { createHash } from "node:crypto";

const HOST = process.env.J7_BRIDGE_HOST || "127.0.0.1";
const PORT = Number(process.env.J7_BRIDGE_PORT || "8791");
const FORWARD_URL = process.env.BIGCALLS_FORWARD_URL || "http://127.0.0.1:8790/webhooks/social/j7";
const AUTH_TOKEN = String(process.env.BIGCALLS_AUTH_TOKEN || "").trim();
const MAX_BODY_BYTES = 128 * 1024;
const DEDUPE_TTL_MS = 5 * 60 * 1000;

const dedupe = new Map();

const server = http.createServer(async (request, response) => {
  writeCorsHeaders(response);

  if (request.method === "OPTIONS") {
    response.writeHead(204);
    response.end();
    return;
  }

  if (request.method === "GET" && request.url === "/health") {
    writeJson(response, 200, { ok: true, service: "bigcalls-j7-bridge" });
    return;
  }

  if (request.method !== "POST" || !["/events/j7", "/webhooks/social/j7"].includes(request.url || "")) {
    writeJson(response, 404, { ok: false, error: "not found" });
    return;
  }

  let payload;
  try {
    payload = await readJsonBody(request);
    validatePayload(payload);
  } catch (error) {
    writeJson(response, 400, { ok: false, error: error.message || "invalid payload" });
    return;
  }

  const key = eventKey(payload);
  const now = Date.now();
  cleanupDedupe(now);
  const previous = dedupe.get(key);
  if (previous && now - previous < DEDUPE_TTL_MS) {
    writeJson(response, 202, { ok: true, duplicate: true });
    return;
  }
  dedupe.set(key, now);

  try {
    const forwarded = await forwardPayload(payload);
    writeJson(response, 202, { ok: true, forwarded });
  } catch (error) {
    dedupe.delete(key);
    writeJson(response, 502, { ok: false, error: error.message || "forward failed" });
  }
});

server.listen(PORT, HOST, () => {
  console.log(`[bigcalls-j7] listening on http://${HOST}:${PORT}`);
  console.log(`[bigcalls-j7] forwarding to ${FORWARD_URL}`);
});

function writeCorsHeaders(response) {
  response.setHeader("access-control-allow-origin", "*");
  response.setHeader("access-control-allow-methods", "GET,POST,OPTIONS");
  response.setHeader("access-control-allow-headers", "authorization,content-type,x-social-bridge-token");
}

function writeJson(response, status, payload) {
  response.writeHead(status, { "content-type": "application/json; charset=utf-8" });
  response.end(JSON.stringify(payload));
}

function readJsonBody(request) {
  return new Promise((resolve, reject) => {
    let body = "";
    let size = 0;

    request.setEncoding("utf8");
    request.on("data", (chunk) => {
      size += Buffer.byteLength(chunk);
      if (size > MAX_BODY_BYTES) {
        request.destroy();
        reject(new Error("body too large"));
        return;
      }
      body += chunk;
    });
    request.on("end", () => {
      try {
        resolve(JSON.parse(body || "{}"));
      } catch (error) {
        reject(error);
      }
    });
    request.on("error", reject);
  });
}

function validatePayload(payload) {
  if (!payload || typeof payload !== "object") {
    throw new Error("payload must be an object");
  }
  if (payload.source !== "j7tracker") {
    throw new Error("source must be j7tracker");
  }
  if (typeof payload.eventType !== "string" || !payload.eventType) {
    throw new Error("eventType is required");
  }
  if (typeof payload.rawText !== "string") {
    throw new Error("rawText is required");
  }
}

function eventKey(payload) {
  return createHash("sha256")
    .update(String(payload.contractAddress || ""))
    .update("|")
    .update(String(payload.tweetUrl || ""))
    .update("|")
    .update(String(payload.rawText || ""))
    .digest("hex");
}

function cleanupDedupe(now) {
  for (const [key, timestamp] of dedupe.entries()) {
    if (now - timestamp >= DEDUPE_TTL_MS) {
      dedupe.delete(key);
    }
  }
}

async function forwardPayload(payload) {
  const headers = { "content-type": "application/json" };
  if (AUTH_TOKEN) {
    headers.authorization = `Bearer ${AUTH_TOKEN}`;
    headers["x-social-bridge-token"] = AUTH_TOKEN;
  }

  const response = await fetch(FORWARD_URL, {
    method: "POST",
    headers,
    body: JSON.stringify(payload)
  });

  if (!response.ok) {
    throw new Error(`BIGCALLS http ${response.status}`);
  }

  return true;
}
