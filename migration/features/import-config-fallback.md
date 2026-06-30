# FEATURE(import-config-fallback) — `import` reads --url / --api-key from config.toml

GOAL: `eds import` must NOT require `--url` / `--api-key` on the command line when those values are already in
`config.toml` (written by `enroll` → `token`, and the server/configure flow → `url`). Use them silently; only error
when a value is absent from BOTH the flag/env AND config.toml. Behavior is cross-port-IDENTICAL.

INTENTIONAL DIVERGENCE: Go's `import` REQUIRES both flags (`mustFlagString(...,true)` — import.go:368 url / :372
api-key). Go's `server`, however, reads them from config via viper (server.go:481 `viper.GetString("url")`, :483
`viper.GetString("token")`). This FEATURE extends the server's existing config-fallback to `import`. Mark every new
site with `FEATURE(import-config-fallback)`.

RESOLUTION — mirror the SERVER command's existing resolution EXACTLY:
- url:     explicit `--url`  →  config.toml `"url"`  →  error (keep the existing "required flag url not set")
- api-key: explicit `--api-key` (whose flag default is `$SM_APIKEY`)  →  config.toml `"token"`  →  error (keep the
           existing missing-api-key error). Net precedence: flag > $SM_APIKEY env > config `"token"`.

CONFIG KEYS (identical to what the server reads — do NOT invent new keys): driver url = `"url"`; api-key = `"token"`.
Load via the existing config loader for the import's data-dir.

REFERENCE (the server resolution to copy):
- Python `eds/cmd/server.py:199-200`: `api_key = args.api_key or config.get_string("token")`;
  `driver_url = args.url or config.get_string("url")`.
- C#: `EdsConfig.GetUrl(dataDir)` (`"url"`) + `EdsConfig.Read(dataDir).Token` (`"token"`); apply as the import flag
  fallback before the required-value check.

SECURITY: `url` and `api-key` are SENSITIVE. The config fallback is SILENT — NEVER log the resolved url or api-key
(no new log line should echo them; preserve the existing non-logging behavior).

ERROR (value absent in flag/env AND config): keep the existing usage error + the existing exit code. You MAY extend
the message text to note that config.toml is an accepted source. Do NOT add an interactive prompt — `import` has no
interactive url/api-key prompt today; this is purely about the required-flag/error path.

TESTS (both ports, written first):
1. flag supplied → flag value used; config NOT consulted (flag wins) — for url AND api-key.
2. flag/env absent + config.toml has `"url"`/`"token"` → those values used, NO error.
3. flag/env absent + config.toml absent/empty → the existing error fires (exit code unchanged).
4. the resolved url/api-key do NOT appear in any captured log output.

NON-GOALS: `--api-url` (separately derived from the api-key JWT / required — out of scope); the destructive-import
`_confirm`/`--no-confirm` prompt (unrelated). Copy this file VERBATIM to
`migration/features/import-config-fallback.md` in your repo.
