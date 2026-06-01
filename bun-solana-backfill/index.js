// Use: node index.js --start <slot> --end <slot> --api-key <KEY_1> [--rpc url] [--batch 100] [--concurrent 20] [--rps 10] [--tx full] [--out ./backfill.ndjson.gz]
const http = require('node:http');
const https = require('node:https');
const fs = require('node:fs');
const zlib = require('node:zlib');

const HELP = `Usage: node index.js --start <slot> --end <slot> --api-key <KEY_1> [--rpc url] [--batch 100] [--concurrent 20] [--rps 10] [--tx full] [--out ./backfill.ndjson.gz]`;

function parseArgs(argv) {
  const args = {
    start: null,
    end: null,
    apiKey: process.env.API_KEY || process.env.KEY_1 || '',
    rpc: process.env.RPC_URL || 'http://la.rpc.orbitflare.com',
    batch: 100,
    concurrent: 20,
    rps: 10,
    tx: 'full',
    out: './backfill.ndjson.gz',
  };
  const positional = [];
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--start' && argv[i+1]) { args.start = parseInt(argv[++i], 10); continue; }
    if (a === '--end' && argv[i+1]) { args.end = parseInt(argv[++i], 10); continue; }
    if (a === '--api-key' && argv[i+1]) { args.apiKey = argv[++i]; continue; }
    if (a === '--rpc' && argv[i+1]) { args.rpc = argv[++i]; continue; }
    if (a === '--batch' && argv[i+1]) { args.batch = parseInt(argv[++i], 10); continue; }
    if (a === '--concurrent' && argv[i+1]) { args.concurrent = parseInt(argv[++i], 10); continue; }
    if (a === '--rps' && argv[i+1]) { args.rps = parseInt(argv[++i], 10); continue; }
    if (a === '--tx' && argv[i+1]) { args.tx = argv[++i]; continue; }
    if (a === '--out' && argv[i+1]) { args.out = argv[++i]; continue; }
    if (!a.startsWith('-')) positional.push(a);
  }
  if (args.start === null || args.end === null) { console.error(HELP); process.exit(2); }
  return args;
}

function post(url, body, apiKey) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const transport = u.protocol === 'https:' ? https : http;
    const headers = {
      'content-type': 'application/json',
      'accept-encoding': 'gzip, deflate',
      'content-length': Buffer.byteLength(body),
    };
    if (apiKey) headers.authorization = 'Bearer ' + apiKey;
    const req = transport.request({ hostname: u.hostname, port: u.port || (u.protocol==='https:'?443:80), path: u.pathname + u.search, method: 'POST', headers, timeout: 120000 }, res => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        const text = Buffer.concat(chunks).toString('utf-8');
        try { resolve(JSON.parse(text)); } catch (e) { reject(new Error('Invalid json: ' + text.slice(0,500))); }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
    req.write(body);
    req.end();
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function getSlot(url, apiKey) {
  const body = JSON.stringify({ jsonrpc:'2.0', id:1, method:'getSlot', params:[] });
  const res = await post(url, body, apiKey);
  if (res.error) throw new Error(JSON.stringify(res.error));
  return res.result;
}

async function *batches(args) {
  const slots = [];
  for (let slot = args.start; slot <= args.end; slot++) slots.push(slot);
  for (let i = 0; i < slots.length; i += args.batch) {
    const chunk = slots.slice(i, i + args.batch);
    yield chunk;
  }
}

async function run() {
  const args = parseArgs(process.argv);
  const gz = zlib.createGzip();
  const out = fs.createWriteStream(args.out);
  gz.pipe(out);

  let done = 0, errs = 0;
  let inflight = 0;
  const sem = new Array(args.concurrent).fill(null);
  for await (const chunk of batches(args)) {
    while (inflight >= args.concurrent) await sleep(2);
    inflight += 1;
    (async () => {
      const body = JSON.stringify({ jsonrpc:'2.0', id:1, method:'getBlocks', params:[chunk, { txDetail: args.tx }] });
      let lastErr = null;
      for (let tries = 0; tries < 3; tries++) {
        try {
          const res = await post(args.rpc, body, args.apiKey);
          if (res.error) throw new Error(JSON.stringify(res.error));
          const payload = JSON.stringify({ slot_start: chunk[0], slot_end: chunk[chunk.length-1], blocks: res.result }) + '\n';
          if (!gz.write(payload)) {
            await new Promise((resolve) => gz.once('drain', resolve));
          }
          process.stdout.write('#');
          lastErr = null;
          break;
        } catch (e) {
          lastErr = e;
          await sleep(100 * (tries + 1));
        }
      }
      if (lastErr) {
        errs += 1;
        process.stderr.write('E');
      }
      done += 1;
      inflight -= 1;
    })();
  }
  while (inflight > 0) await sleep(10);
  gz.end();
  out.end();
  process.stderr.write(`\nDone: ${done} batches, ${errs} errors -> ${args.out}\n`);
}

run().catch(e => { console.error(e); process.exit(1); });