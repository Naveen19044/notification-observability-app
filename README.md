# Notification practice app

A small 4-service app built to practice observability and SRE concepts:
API -> Redis queue -> Worker -> Postgres, with Prometheus/Grafana-ready metrics
and built-in chaos controls to simulate real incidents.

## Run it

```
docker compose up -d --build
```

Then open:
- UI: http://localhost:8080
- API health: http://localhost:5001/api/health
- API metrics: http://localhost:5001/metrics
- Worker metrics: http://localhost:5002/metrics

## Try it manually (no UI)

```
curl -X POST http://localhost:5001/api/notifications \
  -H "Content-Type: application/json" \
  -d '{"to":"test@example.com","subject":"Hello","body":"Test message"}'
```

Copy the returned `id`, then check its status:

```
curl http://localhost:5001/api/notifications/<id>
```

## Chaos controls

```
# Make 50% of jobs fail
curl -X POST http://localhost:5001/chaos/fail-rate -H "Content-Type: application/json" -d '{"rate":0.5}'

# Slow the worker down
curl -X POST http://localhost:5001/chaos/slow-mode -H "Content-Type: application/json" -d '{"enabled":true}'

# Pause the worker entirely (watch queue depth climb)
curl -X POST http://localhost:5001/chaos/worker-pause -H "Content-Type: application/json" -d '{"paused":true}'
```

## What each service does

- **api/** — Flask. Accepts notification requests, writes to Postgres, pushes jobs to Redis.
- **worker/** — Node.js. Pulls jobs from Redis, simulates sending, updates Postgres.
- **ui/** — Static HTML/JS served by nginx. Buttons that call the same API endpoints above.
- **postgres** — stores every notification and its status.
- **redis** — the queue between api and worker.
- **prometheus.yml** — scrape config to point Prometheus at api:5001 and worker:5002 (add this later).
