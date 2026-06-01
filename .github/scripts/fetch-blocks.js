const rpc = 'http://sg.rpc.orbitflare.com';
const key = process.env.KEY_1;
const TARGET = 5000;
const BATCH_SIZE = 10;
const CONCURRENCY = 20;

if (!key) {
  console.error('Error: KEY_1 env var not set');
  process.exit(1);
}

async function call(method, params) {
  const res = await fetch(rpc, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-api-key': key },
    body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }),
  });
  const json = await res.json();
  if (json.error) throw new Error(json.error.message);
  return json.result;
}

// 1. Get tip
const tip = await call('getSlot', []);
console.log('Current slot:', tip);

// 2. Scan backward for 5K block slots
let end = tip - 1;
let allBlocks = [];
const window = 50000;
while (allBlocks.length < TARGET && end > 0) {
  const start = Math.max(0, end - window);
  console.log('Scanning', start, 'to', end);
  const blocks = await call('getBlocks', [start, end]);
  allBlocks = blocks.concat(allBlocks);
  end = start - 1;
  if (end <= 0) break;
}

const count = Math.min(TARGET, allBlocks.length);
const slots = allBlocks.slice(-count);
console.log('Found', slots.length, 'block slots');
console.log('Oldest:', slots[0], 'Newest:', slots[slots.length - 1]);

// 3. Fetch full blocks: batch of 10, rate-limited 10 RPS shared across pool
console.log(`Fetching full blocks (batch=${BATCH_SIZE}, concurrency=${CONCURRENCY})...`);

const total = slots.length;
let fetched = 0;
let errors = 0;
const startTime = Date.now();

// Split into batches
const batches = [];
for (let i = 0; i < total; i += BATCH_SIZE) {
  batches.push(slots.slice(i, i + BATCH_SIZE));
}

// Send one batch (10 getBlock calls in one HTTP request)
async function sendBatch(batchSlots) {
  const requests = batchSlots.map((slot, i) => ({
    jsonrpc: '2.0',
    id: i + 1,
    method: 'getBlock',
    params: [slot, { encoding: 'json', maxSupportedTransactionVersion: 0 }],
  }));

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 30000);
  try {
    const res = await fetch(rpc, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-api-key': key },
      body: JSON.stringify(requests),
      signal: controller.signal,
    });
    clearTimeout(timer);
    const results = await res.json();
    for (const r of results) {
      if (r.error) {
        if (errors < 5) console.log('  ERROR:', r.error.message, '(slot:', batchSlots[r.id - 1], ')');
        errors++;
      } else {
        fetched++;
      }
    }
  } catch (e) {
    clearTimeout(timer);
    if (errors < 5) console.log('  FETCH ERROR:', e.message);
    errors += batchSlots.length;
  }
}

// Worker pool — 20 workers, all always busy
let idx = 0;
let lastLog = Date.now();
async function worker() {
  while (idx < batches.length) {
    const batch = batches[idx++];
    await sendBatch(batch);
    const now = Date.now();
    if (now - lastLog >= 2000) {
      const elapsed = (now - startTime) / 1000;
      const rate = elapsed > 0 ? (fetched / elapsed).toFixed(1) : '-';
      console.log(`Progress: ${fetched}/${total}  errors:${errors}  ${elapsed.toFixed(1)}s  rate:${rate} blk/s`);
      lastLog = now;
    }
  }
}

const pool = Array.from({ length: CONCURRENCY }, () => worker());
await Promise.all(pool);

const totalTime = (Date.now() - startTime) / 1000;
console.log('');
console.log(`Done! Fetched ${fetched} full blocks in ${totalTime.toFixed(1)}s`);
console.log(`Average rate: ${(fetched / totalTime).toFixed(1)} blk/s, errors: ${errors}`);
