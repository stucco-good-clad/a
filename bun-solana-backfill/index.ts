// Bun Solana backfill — batched getBlock, async pool, zero deps
// bun run index.ts --start 370000000 --end 370000999 --api-key <key> [--rpc url]

interface Args {
  start: number;
  end: number;
  key: string;
  rpc: string[];
  batch: number;
  concurrency: number;
  tx: 'full' | 'signatures' | 'none';
  out: string;
}

const HELP = `
Usage: bun run index.ts --start <slot> --end <slot> --api-key <key>
  --rpc        RPC URL (repeat for multi), default: http://la.rpc.orbitflare.com
  --batch      slots per batch request, default: 20
  --concur     max concurrent batch requests, default: 20
  --tx         full | signatures | none, default: full
  --out        output path, default: ./blocks.ndjson.gz
`;

function parseArgs(argv: string[]): Args {
  const a: any = {
    key: process.env.KEY_1 || '',
    rpc: ['http://la.rpc.orbitflare.com'],
    batch: 20,
    concurrency: 20,
    tx: 'full',
    out: './blocks.ndjson.gz',
  };
  for (let i = 2; i < argv.length; i++) {
    const v = argv[i];
    if (v === '--start' && argv[++i]) a.start = parseInt(argv[i], 10);
    else if (v === '--end' && argv[++i]) a.end = parseInt(argv[i], 10);
    else if (v === '--api-key' && argv[++i]) a.key = argv[i];
    else if (v === '--rpc') { while (argv[i+1] && !argv[i+1].startsWith('--')) a.rpc.push(argv[++i]); }
    else if (v === '--batch' && argv[++i]) a.batch = parseInt(argv[i], 10);
    else if (v === '--concur' && argv[++i]) a.concurrency = parseInt(argv[i], 10);
    else if (v === '--tx' && argv[++i]) a.tx = argv[i];
    else if (v === '--out' && argv[++i]) a.out = argv[i];
  }
  if (a.start == null || a.end == null) { console.error(HELP); process.exit(2); }
  if (!a.key) { console.error('ERROR: --api-key required or set KEY_1 env'); process.exit(2); }
  return a;
}

async function sendBatch(url: string, slots: number[], tx: string, key: string): Promise<string[]> {
  // Build batch: array of JSON-RPC getBlock calls, one per slot (matching Rust approach)
  const batch = slots.map((slot, i) => ({
    jsonrpc: '2.0',
    id: i,
    method: 'getBlock',
    params: [slot, { encoding: 'json', transactionDetails: tx, maxSupportedTransactionVersion: 0, rewards: false }],
  }));

  const res = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-api-key': key },
    body: JSON.stringify(batch),
    signal: AbortSignal.timeout(120_000),
  });
  const json: any = await res.json();

  if (!Array.isArray(json)) {
    if (json.error) throw new Error(json.error.message || JSON.stringify(json.error));
    throw new Error('unexpected response: ' + JSON.stringify(json).slice(0, 200));
  }

  // Map responses back to slots by id
  const lines: string[] = [];
  for (const item of json) {
    const idx = item.id;
    if (idx === undefined || idx < 0 || idx >= slots.length) continue;
    if (item.error) {
      // Individual slot error, not whole batch
      continue;
    }
    if (item.result == null) continue; // skipped slot
    lines.push(JSON.stringify({ slot: slots[idx], block: item.result }) + '\n');
  }
  return lines;
}

async function run() {
  const args = parseArgs(process.argv);
  const { start, end, key, rpc, batch, concurrency, tx, out } = args;
  const totalSlots = end - start + 1;

  console.error(`Range ${start}..${end} (${totalSlots} slots, batch=${batch}, concur=${concurrency}, tx=${tx})`);
  console.error(`Endpoints: ${rpc.join(', ')}`);

  // Generate slot list and slice into batches
  const batches: number[][] = [];
  for (let s = start; s <= end; s += batch) {
    const chunk: number[] = [];
    for (let i = 0; i < batch && s + i <= end; i++) chunk.push(s + i);
    if (chunk.length > 0) batches.push(chunk);
  }

  console.error(`Total batches: ${batches.length}`);

  const enc = new TextEncoder();
  const output: Uint8Array[] = [];
  let epIdx = 0;
  let errCount = 0;
  let okBlocks = 0;

  async function processBatch(slots: number[]): Promise<void> {
    const ep = rpc[epIdx++ % rpc.length];
    for (let retry = 0; retry < 3; retry++) {
      try {
        const lines = await sendBatch(ep, slots, tx, key);
        for (const line of lines) output.push(enc.encode(line));
        okBlocks += lines.length;
        return;
      } catch (e: any) {
        if (retry === 2) throw e;
        await Bun.sleep(200 * (retry + 1));
      }
    }
  }

  const startMs = performance.now();
  let nextBatch = 0;
  let active = 0;

  // This matches Rust's concurrency: multiple batch requests in flight simultaneously
  async function worker() {
    while (nextBatch < batches.length) {
      const idx = nextBatch++;
      const slots = batches[idx];
      try {
        await processBatch(slots);
        process.stdout.write('.');
        active--;
      } catch (e: any) {
        errCount++;
        console.error(`\nERR batch ${slots[0]}..${slots[slots.length-1]}: ${e.message}`);
        process.stdout.write('E');
        active--;
      }
    }
  }

  // Spawn concurrency workers
  await Promise.all(Array.from({ length: concurrency }, () => worker()));

  const elapsed = ((performance.now() - startMs) / 1000).toFixed(1);

  // Gzip + write
  const raw = Buffer.concat(output);
  const compressed = Bun.gzipSync(raw);
  Bun.write(out, compressed);
  const rawMb = (raw.byteLength / (1024*1024)).toFixed(1);
  const compMb = (compressed.byteLength / (1024*1024)).toFixed(1);

  console.error(`\n${okBlocks} blocks from ${batches.length} batches, ${errCount} batch errors in ${elapsed}s`);
  console.error(`Raw ${rawMb}MB, gzipped ${compMb}MB -> ${out}`);
  if (errCount > 0) process.exit(1);
}

run().catch(e => { console.error('\nFATAL:', e.message); process.exit(1); });
