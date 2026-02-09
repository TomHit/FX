export const onRequestPost = async (context: any) => {
  try {
    const { request, env } = context;

    const body = await request.json().catch(() => ({}));
    const codeRaw = String(body?.code || "").trim();
    if (!codeRaw) return j({ ok: false, error: "missing_code" }, 400);

    const code = normalizeCode(codeRaw);

    const kv = env?.CODES_KV;
    if (!kv) return j({ ok: false, error: "kv_not_bound" }, 500);

    const key = `code:${code}`;
    const raw = await kv.get(key);
    if (!raw) return j({ ok: false, error: "invalid_code" }, 401);

    let rec: any = null;
    try {
      rec = JSON.parse(raw);
    } catch {
      rec = null;
    }
    if (!rec) return j({ ok: false, error: "bad_record" }, 500);

    const nowMs = Date.now();

    // IMPORTANT: your issue endpoint might store expires_ms OR exp_ms (support both)
    const expMs = Number(rec.expires_ms || rec.exp_ms || 0);
    if (expMs > 0 && nowMs > expMs) {
      await kv.delete(key);
      return j({ ok: false, error: "code_expired" }, 401);
    }

    // Support both max_uses and uses_left styles
    const used = Number(rec.used_count || 0);

    // If record is "uses_left" based:
    if (typeof rec.uses_left === "number") {
      if (rec.uses_left <= 0) return j({ ok: false, error: "code_already_used" }, 401);
      rec.uses_left = Number(rec.uses_left) - 1;
    } else {
      // Else "max_uses" based:
      const maxUses = Number(rec.max_uses || 1);
      if (used >= maxUses) return j({ ok: false, error: "code_already_used" }, 401);
      rec.used_count = used + 1;
    }

    // ---- mint preview token (HMAC, base64url) ----
    const secret = String(env?.PREVIEW_TOKEN_SECRET || "").trim();
    if (!secret || secret.length < 16) {
      return j({ ok: false, error: "secret_not_set" }, 500);
    }

    const nowSec = Math.floor(nowMs / 1000);
    const expSec = expMs > 0 ? Math.floor(expMs / 1000) : nowSec + 3600;

    const scope =
      Array.isArray(rec.scope) && rec.scope.length
        ? rec.scope
        : ["preview:dashboard"];

    const payloadObj = {
      sub: `trial_${crypto.randomUUID()}`,
      plan: String(rec.plan || rec.type || "preview"),
      scope,
      exp: expSec,
      iat: nowSec,
      code,
    };

    const payloadJson = JSON.stringify(payloadObj);
    const payloadB64 = b64url(new TextEncoder().encode(payloadJson));
    const sig = await hmacSign(secret, payloadB64);
    const token = `${payloadB64}.${sig}`;

    // Persist updated usage counters (and optionally delete if fully used)
    // If uses_left reaches 0 -> delete; else store updated record.
    if (typeof rec.uses_left === "number" && rec.uses_left <= 0) {
      await kv.delete(key);
    } else {
      await kv.put(key, JSON.stringify(rec));
    }

    return j({ ok: true, token, exp: expSec, scope });
  } catch (e: any) {
    return j({ ok: false, error: e?.message || "server_error" }, 500);
  }
};

function normalizeCode(x: string) {
  return x.trim().toUpperCase().replace(/\s+/g, "");
}

function b64url(input: Uint8Array) {
  let str = "";
  for (const b of input) str += String.fromCharCode(b);
  return btoa(str).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

async function hmacSign(secret: string, payload: string) {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const sig = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(payload));
  return b64url(new Uint8Array(sig));
}

function j(obj: any, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}
