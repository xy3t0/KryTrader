# Kalshi public fee payload capture — October 30, 2026

All API requests were unauthenticated GETs to https://api.elections.kalshi.com/trade-api/v2. No credentials, account reads, writes, or trades were used.

API JSON payload files are unmodified response bodies. The two manifests record URLs, status, timestamps, Date/Age/Cache-Control headers, and SHA-256. The first manifest records request starts; the supplemental manifest records request start and response completion. Local clock and server Date are not identical. Do not treat local response completion as an authoritative snapshot effective time.

The series-list response contains 15,550 series, without a cursor. Observed fee types: quadratic 15,387; quadratic_with_maker_fees 160; quadratic_with_combo_maker_fees 3; flat 0. This describes this capture, not all possible future responses. Multiplier JSON token spellings: 1 (15,517), 0 (14), 0.5 (19).

BTC15M reports quadratic / multiplier 1. KXBTCY reports quadratic / multiplier 0. The bare KXMVE prefix returns 404; successful combo captures use actual tickers listed in observations.json.

The default series fee-change response is {"series_fee_change_arr":[]}. With show_historical=true, 107 records are returned, dated 2025-10-04 through 2026-09-03. This does not certify complete history or an initial schedule for every series.

The event fee-change response is {"cursor":"","event_fee_changes":[]}. No nonempty event-change payload was observed. Do not infer historical-retention semantics from an empty result. The BTC15M event omits both fee override fields; its last_updated_ts is 0001-01-01T00:00:00Z. The open-event capture contains 200 events and a remaining cursor; none had non-null fee overrides.

The BTC15M market capture contains one open market, status active, a tapered_deci_cent grid, and no fee_waiver_expiration_time key. Five additional pages containing 5,000 open non-combo markets were scanned. No non-null waiver expiry was found; a remaining cursor exists. The search was bounded and incomplete.

openapi.yaml was downloaded from https://docs.kalshi.com/openapi.yaml. It documents EventData fee_type_override and fee_multiplier_override in addition to event change records. The waiver field description states an expiry time, without establishing role coverage, historical start, or precedence against event overrides.

observations.json and selected-history-derived.json are derived summaries, not untouched API payloads. Missing waiver and mixed-null cases must be labeled synthetic in tests.
