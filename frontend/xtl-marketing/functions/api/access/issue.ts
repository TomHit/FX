export const onRequestPost = async (context: any) => {
  try {
    const { request, env } = context;

    // ---- auth (admin-only) ----
    const adminKey = request.headers.get("x-admin-key") || "";
    if (!env?.ADMIN_ISSUE_KEY || adminKey !== env.ADMIN_ISSUE_KEY) {
      return j({ ok: false, error: "unauthorized" }, 401);
    }

    const kv = env?.CODES_KV; // MUST match Cloudflare KV binding name
    if (!kv) return j({ ok: false, error: "kv_not_bound" }, 500);

    const body = await request.json().catch(() => ({}));
    const email = String(body.email || "").trim().toLowerCase() || null;
    const plan = String(body.plan || "beta").trim();
    const days = Number(body.days || 30);
    const maxUses = Number(body.max_uses || 1);
    const prefix = String(body.prefix || "XTL-").trim();

    const code = await issueUniqueCode(kv, prefix);

    const nowMs = Date.now();
    const expiresMs = nowMs + Math.max(1, days) * 24 * 60 * 60 * 1000;

    const rec = {
      code,
      email,
      plan,
      created_ms: nowMs,
      expires_ms: expiresMs,
      max_uses: maxUses,
      used_count: 0,
    };

    // Auto-cleanup after expiry
    const ttl = Math.max(60, Math.floor((expiresMs - nowMs) / 1000));
    await kv.put(`code:${code}`, JSON.stringify(rec), { expirationTtl: ttl });

    return j({ ok: true, code, expires_ms: expiresMs });
  } catch (e: any) {
    return j({ ok: false, error: e?.message || "server_error" }, 500);
  }
};

async function issueUniqueCode(kv: any, prefix: string) {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  const rand = () => {
    let s = "";
    for (let i = 0; i < 6; i++) s += chars[Math.floor(Math.random() * chars.length)];
    return s;
  };

  for (let i = 0; i < 10; i++) {
    const code = `${prefix}${rand()}`;
    const exists = await kv.get(`code:${code}`);
    if (!exists) return code;
  }
  throw new Error("failed_to_issue_code");
}

function j(obj: any, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}
