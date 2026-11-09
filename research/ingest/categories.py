"""Map events to a single top-level category from Gamma tags (+ sports league slug prefixes as fallback)."""
import json, urllib.request

# first match wins
RULES = [
    ("esports", {"esports", "counter-strike-2", "league-of-legends", "dota-2", "valorant"}),
    ("sports", {"sports"}),
    ("weather", {"weather", "daily-temperature", "highest-temperature", "lowest-temperature", "climate"}),
    ("crypto", {"crypto", "crypto-prices", "bitcoin", "ethereum", "solana", "xrp"}),
    ("finance", {"finance", "equities", "stocks", "economy", "fed", "fed-rates", "economic-policy", "earnings", "indicies", "commodities", "business"}),
    ("geopolitics", {"geopolitics", "world", "middle-east", "ukraine", "israel", "china"}),
    ("politics", {"politics", "elections", "trump", "us-politics", "global-elections", "world-elections"}),
    ("tech", {"tech", "ai", "openai", "big-tech", "science"}),
    ("culture", {"pop-culture", "movies", "music", "awards", "celebrities", "culture", "mentions", "tweet-markets", "entertainment"}),
]


def sports_prefixes():
    req = urllib.request.Request("https://gamma-api.polymarket.com/sports", headers={"User-Agent": "research-pipeline"})
    return {s["sport"] for s in json.load(urllib.request.urlopen(req, timeout=60))}


def categorize(tags, event_slug, prefixes):
    tags = set(tags or [])
    for cat, keys in RULES:
        if tags & keys:
            return cat
    if event_slug and event_slug.split("-")[0] in prefixes:
        return "sports"
    return "other"
