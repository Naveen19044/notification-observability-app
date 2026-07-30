const redis = require('redis');
const { Client } = require('pg');
const client = require('prom-client');
const http = require('http');

const REDIS_HOST = process.env.REDIS_HOST || 'redis';
const DB_HOST = process.env.DB_HOST || 'postgres';
const DB_NAME = process.env.DB_NAME || 'notifdb';
const DB_USER = process.env.DB_USER || 'notifuser';
const DB_PASSWORD = process.env.DB_PASSWORD || 'notifpass';

// ---------- structured JSON logging ----------
function log(message, extra = {}) {
  console.log(JSON.stringify({
    timestamp: new Date().toISOString(),
    level: 'INFO',
    service: 'notification-worker',
    message,
    ...extra
  }));
}

// ---------- prometheus metrics ----------
const register = new client.Registry();
client.collectDefaultMetrics({ register });

const jobsProcessed = new client.Counter({
  name: 'worker_jobs_processed_total',
  help: 'Total jobs processed by the worker',
  labelNames: ['result'],
  registers: [register]
});
const jobDuration = new client.Histogram({
  name: 'worker_job_duration_seconds',
  help: 'Time taken to process a job',
  registers: [register]
});
const queueDepthGauge = new client.Gauge({
  name: 'worker_queue_depth',
  help: 'Current queue depth as seen by the worker',
  registers: [register]
});

// ---------- tiny metrics HTTP server ----------
const metricsServer = http.createServer(async (req, res) => {
  if (req.url === '/metrics') {
    res.setHeader('Content-Type', register.contentType);
    res.end(await register.metrics());
  } else if (req.url === '/health') {
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify({ status: 'ok', service: 'notification-worker' }));
  } else {
    res.writeHead(404);
    res.end();
  }
});
metricsServer.listen(5002, () => log('Metrics server listening on 5002'));

// ---------- redis + postgres ----------
const redisClient = redis.createClient({ url: `redis://${REDIS_HOST}:6379` });
redisClient.on('error', (err) => log('Redis error', { error: err.message }));

const pgClient = new Client({
  host: DB_HOST,
  database: DB_NAME,
  user: DB_USER,
  password: DB_PASSWORD,
});

async function connectWithRetry() {
  for (let i = 0; i < 10; i++) {
    try {
      await redisClient.connect();
      await pgClient.connect();
      log('Connected to Redis and Postgres');
      return;
    } catch (e) {
      log('Waiting for dependencies...', { attempt: i + 1 });
      await new Promise((r) => setTimeout(r, 3000));
    }
  }
  throw new Error('Could not connect to dependencies');
}

async function updateStatus(id, status) {
  await pgClient.query('UPDATE notifications SET status = $1, updated_at = NOW() WHERE id = $2', [status, id]);
}

async function processLoop() {
  while (true) {
    const paused = await redisClient.get('chaos:worker_paused');
    if (paused === '1') {
      await new Promise((r) => setTimeout(r, 1000));
      continue;
    }

    const depth = await redisClient.lLen('notification_queue');
    queueDepthGauge.set(depth);

    const raw = await redisClient.lPop('notification_queue');
    if (!raw) {
      await new Promise((r) => setTimeout(r, 500));
      continue;
    }

    const job = JSON.parse(raw);
    const end = jobDuration.startTimer();
    log('Processing job', { id: job.id, to: job.to });

    const slowMode = (await redisClient.get('chaos:slow_mode')) === '1';
    const failRate = parseFloat((await redisClient.get('chaos:fail_rate')) || '0');

    // simulate work: normal ~200-500ms, slow mode ~3-5s
    const baseDelay = slowMode ? 3000 + Math.random() * 2000 : 200 + Math.random() * 300;
    await new Promise((r) => setTimeout(r, baseDelay));

    const shouldFail = Math.random() < failRate;

    try {
      if (shouldFail) {
        throw new Error('Simulated delivery failure');
      }
      await updateStatus(job.id, 'sent');
      jobsProcessed.inc({ result: 'success' });
      log('Job sent successfully', { id: job.id });
    } catch (e) {
      await updateStatus(job.id, 'failed');
      jobsProcessed.inc({ result: 'failure' });
      log('Job failed', { id: job.id, error: e.message });
    } finally {
      end();
    }
  }
}

connectWithRetry().then(processLoop).catch((e) => {
  log('Fatal error starting worker', { error: e.message });
  process.exit(1);
});
