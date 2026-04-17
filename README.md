# AI Commerce Sync Worker

Background worker that syncs all Shopify stores without timeout limits.

## Features
- Full sync: fetches ALL products (no 10k limit)
- Incremental sync: only fetches changed products (fast)
- Auto-runs every 4 hours
- Full sync daily at 2am
- Manual trigger via HTTP endpoints

## Deploy to Railway

1. Create a new Railway service from this folder
2. Set these environment variables:
   - `SUPABASE_URL` = your Supabase project URL
   - `SUPABASE_SERVICE_ROLE_KEY` = your service role key
   - `SYNC_SECRET` = any random secret string (e.g. "mysecret123")

## Manual triggers

```bash
# Incremental sync
curl -X POST https://your-worker.railway.app/sync \
  -H "x-sync-secret: YOUR_SECRET"

# Full sync (all products)
curl -X POST https://your-worker.railway.app/sync/full \
  -H "x-sync-secret: YOUR_SECRET"

# Health check
curl https://your-worker.railway.app/health
```
