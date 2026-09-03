from __future__ import annotations

import json
import os
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extras


FINNHUB_BASE = "https://finnhub.io/api/v1/news"


# ---------------------------------------------------------------------------
# XTL macro relevance rules
#
# IMPORTANT:
# - Phrase/token matching, not naive substring matching.
# - Prevents examples such as:
#       "USDA"        -> USD
#       "risk-reward" -> war
# - This is classification only. It has ZERO trading authority.
# ---------------------------------------------------------------------------

TOPICS: dict[str, tuple[str, ...]] = {
    "FED_RATES": (
        r"\bfederal reserve\b",
        r"\bfed\b",
        r"\bfomc\b",
        r"\bpowell\b",
        r"\binterest rates?\b",
        r"\brate cuts?\b",
        r"\brate hikes?\b",
    ),

    "US_INFLATION": (
        r"\binflation\b",
        r"\bcpi\b",
        r"\bpce\b",
        r"\bconsumer prices?\b",
        r"\bcore inflation\b",
    ),

    "US_LABOR": (
        r"\bnonfarm payrolls?\b",
        r"\bnfp\b",
        r"\bpayrolls?\b",
        r"\bunemployment\b",
        r"\bjobless claims?\b",
        r"\bemployment\b",
    ),

    "US_ACTIVITY": (
        r"\bism\b",
        r"\bpmi\b",
        r"\bgdp\b",
        r"\brecession\b",
        r"\beconomic growth\b",
    ),

    "USD": (
        r"\bu\.?s\.? dollar\b",
        r"\bus dollar\b",
        r"\bdollar index\b",
        r"\bdxy\b",
        r"\busd\b",
    ),

    "US_RATES_BONDS": (
        r"\bu\.?s\.? treasur(?:y|ies)\b",
        r"\btreasury yields?\b",
        r"\bbond yields?\b",
        r"\b10-year yield\b",
        r"\b2-year yield\b",
        r"\breal yields?\b",
    ),

    "GOLD": (
        r"\bgold\b",
        r"\bxauusd\b",
        r"\bxau/usd\b",
        r"\bbullion\b",
    ),

    "OIL": (
        r"\bcrude oil\b",
        r"\bbrent\b",
        r"\bwti\b",
        r"\boil prices?\b",
        r"\boil\b",
    ),

    "GEOPOLITICAL": (
        r"\bmiddle east\b",
        r"\biran\b",
        r"\bisrael\b",
        r"\bukraine\b",
        r"\brussia\b",
        r"\bgeopolitical\b",
        r"\bwar\b",
        r"\bmilitary attacks?\b",
        r"\bsanctions?\b",
    ),

    "ECB_EUR": (
        r"\beuropean central bank\b",
        r"\becb\b",
        r"\beurozone\b",
        r"\beuro area\b",
        r"\beur/usd\b",
        r"\beurusd\b",
    ),

    "BOE_GBP": (
        r"\bbank of england\b",
        r"\bboe\b",
        r"\bsterling\b",
        r"\bgbp/usd\b",
        r"\bgbpusd\b",
    ),

    "BOJ_JPY": (
        r"\bbank of japan\b",
        r"\bboj\b",
        r"\byen\b",
        r"\busd/jpy\b",
        r"\busdjpy\b",
    ),

    "BOC_CAD": (
        r"\bbank of canada\b",
        r"\bboc\b",
        r"\bcanadian dollar\b",
        r"\busd/cad\b",
        r"\busdcad\b",
    ),

    "SNB_CHF": (
        r"\bswiss national bank\b",
        r"\bsnb\b",
        r"\bswiss franc\b",
        r"\busd/chf\b",
        r"\busdchf\b",
    ),

    "RBA_AUD": (
        r"\breserve bank of australia\b",
        r"\brba\b",
        r"\baustralian dollar\b",
        r"\baud/usd\b",
        r"\baudusd\b",
    ),
}


COMPILED_TOPICS = {
    topic: tuple(re.compile(pattern, re.IGNORECASE) for pattern in patterns)
    for topic, patterns in TOPICS.items()
}


# Broad words by themselves are weak evidence.
WEAK_ONLY_TOPICS = {
    "US_ACTIVITY",
    "OIL",
    "GEOPOLITICAL",
}


def utc_iso_from_epoch(value: Any) -> str | None:
    try:
        ts = int(value)
        if ts <= 0:
            return None
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    except Exception:
        return None


def age_minutes(value: Any) -> float | None:
    try:
        ts = int(value)
        if ts <= 0:
            return None
        return round((time.time() - ts) / 60.0, 1)
    except Exception:
        return None


def fetch_news(token: str, category: str) -> tuple[list[dict], dict]:
    params = urllib.parse.urlencode({
        "category": category,
        "token": token,
    })

    req = urllib.request.Request(
        FINNHUB_BASE + "?" + params,
        headers={"User-Agent": "XauTrendLab-Finnhub-Shadow/1.0"},
    )

    with urllib.request.urlopen(req, timeout=20) as r:
        payload = json.loads(r.read().decode("utf-8"))

        headers = {
            "limit": r.headers.get("X-Ratelimit-Limit"),
            "remaining": r.headers.get("X-Ratelimit-Remaining"),
            "reset": r.headers.get("X-Ratelimit-Reset"),
        }

        if not isinstance(payload, list):
            raise RuntimeError(
                f"Unexpected Finnhub response for {category}: "
                f"{type(payload).__name__}"
            )

        return payload, headers


def classify(article: dict) -> tuple[list[str], int]:
    headline = str(article.get("headline") or "")
    summary = str(article.get("summary") or "")
    category = str(article.get("category") or "")
    text = " ".join([headline, summary, category])

    topics: list[str] = []

    for topic, patterns in COMPILED_TOPICS.items():
        if any(pattern.search(text) for pattern in patterns):
            topics.append(topic)

    text_l = text.lower()

    # ---------------------------------------------------------------
    # Scope correction:
    # Generic GDP / PMI / growth must not automatically become
    # US_ACTIVITY. Require explicit US context.
    # ---------------------------------------------------------------
    if "US_ACTIVITY" in topics:
        us_context = bool(re.search(
            r"\b("
            r"u\.?s\.?|united states|american|"
            r"ism|federal reserve|fed|"
            r"us economy|u\.s\. economy"
            r")\b",
            text_l,
            re.IGNORECASE,
        ))

        if not us_context:
            topics.remove("US_ACTIVITY")

    # Generic inflation references also need explicit US/Fed/USD context.
    if "US_INFLATION" in topics:
        us_inflation_context = bool(re.search(
            r"\b("
            r"u\.?s\.?|united states|american|"
            r"federal reserve|fed|fomc|powell|"
            r"usd|dollar|pce|cpi"
            r")\b",
            text_l,
            re.IGNORECASE,
        ))

        if not us_inflation_context:
            topics.remove("US_INFLATION")

    direct_topics = {
        "FED_RATES",
        "US_INFLATION",
        "US_LABOR",
        "US_ACTIVITY",
        "USD",
        "US_RATES_BONDS",
        "GOLD",
        "ECB_EUR",
        "BOE_GBP",
        "BOJ_JPY",
        "BOC_CAD",
        "SNB_CHF",
        "RBA_AUD",
    }

    direct_hits = [x for x in topics if x in direct_topics]

    has_oil = "OIL" in topics
    has_geo = "GEOPOLITICAL" in topics

    # Geopolitical/oil stories require an observable market linkage.
    market_link = bool(
        direct_hits
        or re.search(
            r"\b("
            r"yield|yields|bond|bonds|"
            r"dollar|usd|gold|bullion|"
            r"inflation|interest rates?|"
            r"stocks?|equities|markets?|"
            r"oil prices?|crude|brent|wti|"
            r"supply|hormuz|energy prices?"
            r")\b",
            text_l,
            re.IGNORECASE,
        )
    )

    if has_geo and not direct_hits and not market_link:
        return topics, 0

    contextual = (
        (has_oil and market_link)
        or (has_geo and market_link)
    )

    if len(direct_hits) >= 2:
        score = 3
    elif len(direct_hits) == 1:
        score = 2
    elif contextual:
        score = 1
    else:
        score = 0

    return topics, score



def persist_relevant_articles(
    database_url: str,
    articles: list[dict],
    seen_at_ms: int,
) -> dict[str, int]:
    """
    Idempotently persist relevant Finnhub articles.

    first_seen_at_ms is intentionally immutable on conflict.
    It represents the first moment XTL actually knew about an article.
    """
    if not articles:
        return {
            "attempted": 0,
            "inserted": 0,
            "updated": 0,
        }

    sql = """
        INSERT INTO xtl_news_articles (
            provider,
            provider_article_id,
            source,
            headline,
            summary,
            article_url,
            feed_categories,
            topics,
            relevance_score,
            published_at_ms,
            first_seen_at_ms,
            last_seen_at_ms,
            collected_at_ms,
            raw_payload
        )
        VALUES (
            'FINNHUB',
            %(provider_article_id)s,
            %(source)s,
            %(headline)s,
            %(summary)s,
            %(article_url)s,
            %(feed_categories)s::jsonb,
            %(topics)s::jsonb,
            %(relevance_score)s,
            %(published_at_ms)s,
            %(first_seen_at_ms)s,
            %(last_seen_at_ms)s,
            %(collected_at_ms)s,
            %(raw_payload)s::jsonb
        )
        ON CONFLICT (provider, provider_article_id)
        DO UPDATE SET
            source = EXCLUDED.source,
            headline = EXCLUDED.headline,
            summary = EXCLUDED.summary,
            article_url = EXCLUDED.article_url,
            feed_categories = EXCLUDED.feed_categories,
            topics = EXCLUDED.topics,
            relevance_score = EXCLUDED.relevance_score,
            published_at_ms = EXCLUDED.published_at_ms,

            -- CRITICAL:
            -- Never rewrite the causal first-observation timestamp.
            first_seen_at_ms = xtl_news_articles.first_seen_at_ms,

            last_seen_at_ms = EXCLUDED.last_seen_at_ms,
            collected_at_ms = EXCLUDED.collected_at_ms,
            raw_payload = EXCLUDED.raw_payload,
            updated_at = NOW()

        RETURNING (xmax = 0) AS inserted
    """

    conn = psycopg2.connect(database_url)

    inserted = 0
    updated = 0

    try:
        with conn.cursor() as cur:
            for article in articles:
                published_s = int(article.get("datetime") or 0)

                if published_s <= 0:
                    continue

                provider_article_id = str(
                    article.get("id")
                    or article.get("url")
                    or (
                        str(article.get("datetime"))
                        + "|"
                        + str(article.get("headline"))
                    )
                )

                raw_payload = {
                    k: v
                    for k, v in article.items()
                    if not str(k).startswith("_")
                }

                cur.execute(
                    sql,
                    {
                        "provider_article_id": provider_article_id,
                        "source": str(article.get("source") or ""),
                        "headline": str(article.get("headline") or ""),
                        "summary": str(article.get("summary") or ""),
                        "article_url": str(article.get("url") or ""),
                        "feed_categories": json.dumps(
                            article.get("_feed_categories", []),
                            ensure_ascii=False,
                        ),
                        "topics": json.dumps(
                            article.get("_xtl_topics", []),
                            ensure_ascii=False,
                        ),
                        "relevance_score": int(
                            article.get("_xtl_relevance_score") or 0
                        ),
                        "published_at_ms": published_s * 1000,
                        "first_seen_at_ms": seen_at_ms,
                        "last_seen_at_ms": seen_at_ms,
                        "collected_at_ms": seen_at_ms,
                        "raw_payload": json.dumps(
                            raw_payload,
                            ensure_ascii=False,
                        ),
                    },
                )

                row = cur.fetchone()

                if row and bool(row[0]):
                    inserted += 1
                else:
                    updated += 1

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()

    return {
        "attempted": len(articles),
        "inserted": inserted,
        "updated": updated,
    }



def main() -> None:
    token = os.environ.get("FINNHUB_TOKEN", "").strip()

    if not token:
        raise SystemExit(
            "FINNHUB_TOKEN is not set. "
            "Use: read -s -p 'Finnhub API key: ' FINNHUB_TOKEN; export FINNHUB_TOKEN"
        )

    all_articles: dict[str, dict] = {}
    fetch_info: dict[str, dict] = {}

    for category in ("forex", "general"):
        articles, rate = fetch_news(token, category)

        fetch_info[category] = {
            "returned": len(articles),
            "rate_limit": rate,
        }

        for article in articles:
            if not isinstance(article, dict):
                continue

            article_id = str(
                article.get("id")
                or article.get("url")
                or (
                    str(article.get("datetime"))
                    + "|"
                    + str(article.get("headline"))
                )
            )

            existing = all_articles.get(article_id)

            if existing is None:
                item = dict(article)
                item["_feed_categories"] = [category]
                all_articles[article_id] = item
            else:
                cats = existing.setdefault("_feed_categories", [])
                if category not in cats:
                    cats.append(category)

    # Causal observation timestamp:
    # articles are considered known to XTL only after all requested
    # Finnhub responses for this collection cycle have been received.
    collected_dt = datetime.now(timezone.utc)
    collected_at = collected_dt.isoformat()
    collected_at_ms = int(collected_dt.timestamp() * 1000)

    relevant = []

    for article in all_articles.values():
        topics, score = classify(article)

        if score < 1:
            continue

        age_min = age_minutes(article.get("datetime"))

        # LIVE shadow context: ignore articles older than 24 hours.
        if age_min is None or age_min > 1440:
            continue

        # Preserve the frozen classifier result associated with this
        # collector observation. These private fields are not written
        # into raw_payload.
        article["_xtl_topics"] = list(topics)
        article["_xtl_relevance_score"] = int(score)

        relevant.append({
            "id": article.get("id"),
            "datetime_utc": utc_iso_from_epoch(article.get("datetime")),
            "age_minutes": age_minutes(article.get("datetime")),
            "source": article.get("source"),
            "headline": article.get("headline"),
            "feed_categories": article.get("_feed_categories", []),
            "topics": topics,
            "relevance_score": score,
        })

    relevant.sort(
        key=lambda x: (
            x.get("datetime_utc") or "",
            x.get("relevance_score") or 0,
        ),
        reverse=True,
    )

    score_counts = {
        "score_3": sum(x["relevance_score"] == 3 for x in relevant),
        "score_2": sum(x["relevance_score"] == 2 for x in relevant),
        "score_1": sum(x["relevance_score"] == 1 for x in relevant),
    }

    database_url = os.environ.get("DATABASE_URL", "").strip()

    if not database_url:
        raise SystemExit(
            "DATABASE_URL is not set; refusing to run persistence"
        )

    relevant_ids = {
        str(item.get("id"))
        for item in relevant
        if item.get("id") is not None
    }

    relevant_articles = []

    for article in all_articles.values():
        article_id = str(
            article.get("id")
            or article.get("url")
            or (
                str(article.get("datetime"))
                + "|"
                + str(article.get("headline"))
            )
        )

        # ID-backed Finnhub articles are the normal case.
        # For fallback IDs, use the classifier marker as the authority.
        if (
            article_id in relevant_ids
            or int(article.get("_xtl_relevance_score") or 0) >= 1
        ):
            relevant_articles.append(article)

    persistence = persist_relevant_articles(
        database_url,
        relevant_articles,
        collected_at_ms,
    )

    print("XTL_FINNHUB_SHADOW")
    print("COLLECTED_AT_UTC =", collected_at)
    print("FETCH_INFO =", json.dumps(fetch_info, ensure_ascii=False))
    print("UNIQUE_ARTICLES =", len(all_articles))
    print("RELEVANT_ARTICLES =", len(relevant))
    print("SCORE_COUNTS =", json.dumps(score_counts))
    print(
        "POSTGRES_PERSIST =",
        json.dumps(persistence, ensure_ascii=False),
    )
    print()

    for item in relevant[:40]:
        print(json.dumps(item, ensure_ascii=False))


if __name__ == "__main__":
    main()
