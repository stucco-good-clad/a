// Usage this way ONLY:
//   deno run --allow-all index.ts --start <slot> --end <slot> [--batch 100] [--concurrent 20] [--rps 10] [--tx full|signatures|none] [--out path] [--endpoints url1 url2 ...] [--api-key <KEY_1>]

import http from "node:http";
import https from "node:https";
import fs from "node:fs";
import zlib from "node:zlib";
