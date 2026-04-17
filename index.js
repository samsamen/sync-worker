import { createClient } from "@supabase/supabase-js";
import cron from "node-cron";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const PORT = process.env.PORT || 3001;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// ── Helpers ──────────────────────────────────────────────────────────────────

async function fetchAllShopifyPages(url, key, headers) {
  const results = [];
  let nextUrl = url;
  let page = 0;
  while (nextUrl) {
    page++;
    const res = await fetch(nextUrl, { headers });
    if (!res.ok) {
      console.error(`  Shopify API error ${res.status} on page ${page}`);
      break;
    }
    const data = await res.json();
    const batch = data[key] || [];
    results.push(...batch);
    console.log(`  Page ${page}: ${batch.length} ${key}, total: ${results.length}`);
    if (batch.length === 0) break;
    const link = res.headers.get("Link") || "";
    const next = link.match(/<([^>]+)>;\s*rel="next"/);
    nextUrl = next ? next[1] : null;
  }
  return results;
}

async function upsertBatch(table, rows, conflictKey) {
  const BATCH = 200;
  let inserted = 0;
  for (let i = 0; i < rows.length; i += BATCH) {
    const { error } = await supabase.from(table).upsert(rows.slice(i, i + BATCH), { onConflict: conflictKey });
    if (error) console.error(`  Upsert error on ${table}:`, error.message);
    else inserted += Math.min(BATCH, rows.length - i);
  }
  return inserted;
}

// ── Core sync function ────────────────────────────────────────────────────────

async function syncStore(store, fullSync = false) {
  console.log(`\n${"=".repeat(60)}`);
  console.log(`Syncing: ${store.name} (${store.shopify_domain})`);
  console.log(`Mode: ${fullSync ? "FULL" : "INCREMENTAL"}`);
  console.log(`${"=".repeat(60)}`);

  const base = `https://${store.shopify_domain}/admin/api/2024-01`;
  const headers = {
    "X-Shopify-Access-Token": store.shopify_access_token,
    "Content-Type": "application/json",
  };

  // Determine updated_at_min for incremental sync
  let updatedAtMin = "2020-01-01T00:00:00Z";
  if (!fullSync) {
    const { data: lastSync } = await supabase
      .from("sync_logs")
      .select("completed_at")
      .eq("store_id", store.id)
      .eq("integration", "shopify")
      .eq("status", "success")
      .order("completed_at", { ascending: false })
      .limit(1)
      .single();
    if (lastSync?.completed_at) {
      updatedAtMin = new Date(new Date(lastSync.completed_at).getTime() - 10 * 60000).toISOString();
    } else {
      updatedAtMin = new Date(Date.now() - 48 * 3600000).toISOString(); // last 48h if no previous sync
    }
  }

  console.log(`Updated since: ${updatedAtMin}`);

  // Create sync log
  const { data: syncLog } = await supabase
    .from("sync_logs")
    .insert({ store_id: store.id, integration: "shopify", status: "running" })
    .select().single();

  let totalProducts = 0;
  let totalOrders = 0;

  try {
    // ── PRODUCTS ──────────────────────────────────────────────────────────────
    console.log("\n[Products]");
    // Fetch ALL products in one call — published_status=any gets everything
    console.log(`  Fetching all products (published + unpublished + draft + archived)...`);
    const allProducts = await fetchAllShopifyPages(
      `${base}/products.json?limit=250&published_status=any&updated_at_min=${updatedAtMin}`,
      "products", headers
    );

    console.log(`  Total: ${allProducts.length} products`);

    const productRows = allProducts.map(p => ({
      store_id: store.id,
      external_id: String(p.id),
      name: p.title,
      sku: p.variants?.[0]?.sku || null,
      price: parseFloat(p.variants?.[0]?.price || "0"),
      image_url: p.image?.src || null,
    }));

    totalProducts = await upsertBatch("products", productRows, "store_id,external_id");
    console.log(`  Saved: ${totalProducts} products`);

    // ── ORDERS ────────────────────────────────────────────────────────────────
    console.log("\n[Orders]");
    const since = new Date(Date.now() - 90 * 86400000).toISOString();
    const allOrders = await fetchAllShopifyPages(
      `${base}/orders.json?status=any&created_at_min=${since}&limit=250`,
      "orders", headers
    );
    console.log(`  Total: ${allOrders.length} orders`);

    // Save full order data
    const orderRows = allOrders.map(order => ({
      store_id: store.id,
      shopify_order_id: String(order.id),
      order_number: order.order_number ? `#${order.order_number}` : null,
      email: order.email || null,
      customer_name: order.customer
        ? `${order.customer.first_name || ""} ${order.customer.last_name || ""}`.trim()
        : null,
      financial_status: order.financial_status || null,
      fulfillment_status: order.fulfillment_status || "unfulfilled",
      total_price: parseFloat(order.total_price || "0"),
      subtotal_price: parseFloat(order.subtotal_price || "0"),
      total_tax: parseFloat(order.total_tax || "0"),
      total_discounts: parseFloat(order.total_discounts || "0"),
      currency: order.currency || "EUR",
      city: order.shipping_address?.city || null,
      country: order.shipping_address?.country || null,
      shopify_created_at: order.created_at,
    }));

    totalOrders = await upsertBatch("orders", orderRows, "store_id,shopify_order_id");
    console.log(`  Saved: ${totalOrders} orders`);

    // ── DAILY METRICS ─────────────────────────────────────────────────────────
    console.log("\n[Daily Metrics]");
    const dailyMap = {};
    for (const order of allOrders) {
      const date = order.created_at.split("T")[0];
      if (!dailyMap[date]) dailyMap[date] = { revenue: 0, orders: 0 };
      dailyMap[date].revenue += parseFloat(order.total_price || "0");
      dailyMap[date].orders += 1;
    }
    const metricRows = Object.entries(dailyMap).map(([date, d]) => ({
      store_id: store.id, date, revenue: d.revenue, orders_count: d.orders,
    }));
    await upsertBatch("daily_metrics", metricRows, "store_id,date");
    console.log(`  Saved: ${metricRows.length} days`);

    // ── PRODUCT REVENUE ───────────────────────────────────────────────────────
    console.log("\n[Product Revenue]");
    const productStats = {};
    for (const order of allOrders) {
      for (const item of order.line_items || []) {
        const pid = String(item.product_id);
        if (!productStats[pid]) productStats[pid] = { revenue: 0, sales: 0 };
        productStats[pid].revenue += parseFloat(item.price || "0") * (item.quantity || 1);
        productStats[pid].sales += item.quantity || 1;
      }
    }
    const updates = Object.entries(productStats);
    let updatedProducts = 0;
    for (let i = 0; i < updates.length; i += 100) {
      await Promise.all(
        updates.slice(i, i + 100).map(([external_id, stats]) =>
          supabase.from("products")
            .update({ total_revenue: stats.revenue, total_sales: stats.sales })
            .eq("store_id", store.id)
            .eq("external_id", external_id)
        )
      );
      updatedProducts += Math.min(100, updates.length - i);
    }
    console.log(`  Updated: ${updatedProducts} products with revenue data`);

    // ── REFUNDS ───────────────────────────────────────────────────────────────
    console.log("\n[Refunds]");
    let refundCount = 0;
    for (const order of allOrders) {
      for (const refund of order.refunds || []) {
        for (const lineItem of refund.refund_line_items || []) {
          await supabase.from("refunds").upsert({
            store_id: store.id,
            date: refund.created_at.split("T")[0],
            order_id: String(order.id),
            product_name: lineItem.line_item?.title || "Unknown",
            amount: parseFloat(lineItem.subtotal || "0"),
            reason: refund.note || null,
            customer_email: order.email || null,
            source: "shopify",
          });
          refundCount++;
        }
      }
    }
    console.log(`  Saved: ${refundCount} refunds`);

    // Mark success
    if (syncLog) {
      await supabase.from("sync_logs").update({
        status: "success",
        records_synced: totalProducts + totalOrders,
        completed_at: new Date().toISOString(),
      }).eq("id", syncLog.id);
    }

    console.log(`\n✓ Done: ${store.name} — ${totalProducts} products, ${totalOrders} orders, ${refundCount} refunds`);

  } catch (err) {
    console.error(`\n✗ Error syncing ${store.name}:`, err.message);
    if (syncLog) {
      await supabase.from("sync_logs").update({
        status: "error",
        error_message: err.message,
        completed_at: new Date().toISOString(),
      }).eq("id", syncLog.id);
    }
  }
}

// ── Sync all stores ───────────────────────────────────────────────────────────

async function syncAllStores(fullSync = false) {
  console.log(`\n${"#".repeat(60)}`);
  console.log(`Starting ${fullSync ? "FULL" : "INCREMENTAL"} sync — ${new Date().toISOString()}`);
  console.log(`${"#".repeat(60)}`);

  const { data: stores, error } = await supabase
    .from("stores")
    .select("*")
    .eq("is_active", true);

  if (error) { console.error("Failed to fetch stores:", error.message); return; }

  const active = (stores || []).filter(s => s.shopify_access_token && s.shopify_domain);
  console.log(`\nFound ${active.length} active stores`);

  for (const store of active) {
    await syncStore(store, fullSync);
  }

  console.log(`\n${"#".repeat(60)}`);
  console.log(`Sync complete — ${new Date().toISOString()}`);
  console.log(`${"#".repeat(60)}\n`);
}

// ── HTTP Server (for manual triggers) ────────────────────────────────────────

import { createServer } from "http";

const server = createServer(async (req, res) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);

  // Health check
  if (url.pathname === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", time: new Date().toISOString() }));
    return;
  }

  // Verify secret
  const secret = req.headers["x-sync-secret"];
  if (secret !== process.env.SYNC_SECRET) {
    res.writeHead(401);
    res.end(JSON.stringify({ error: "Unauthorized" }));
    return;
  }

  // Trigger incremental sync
  if (url.pathname === "/sync" && req.method === "POST") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ message: "Incremental sync started" }));
    syncAllStores(false).catch(console.error);
    return;
  }

  // Trigger full sync
  if (url.pathname === "/sync/full" && req.method === "POST") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ message: "Full sync started" }));
    syncAllStores(true).catch(console.error);
    return;
  }

  res.writeHead(404);
  res.end(JSON.stringify({ error: "Not found" }));
});

server.listen(PORT, () => {
  console.log(`\nSync worker running on port ${PORT}`);
  console.log(`Health: http://localhost:${PORT}/health`);
});

// ── Cron jobs ─────────────────────────────────────────────────────────────────

// Incremental sync every 4 hours
cron.schedule("0 */4 * * *", () => {
  console.log("Cron: Starting scheduled incremental sync");
  syncAllStores(false).catch(console.error);
});

// Full sync once a day at 2am
cron.schedule("0 2 * * *", () => {
  console.log("Cron: Starting scheduled full sync");
  syncAllStores(true).catch(console.error);
});

console.log("Cron jobs scheduled:");
console.log("  - Incremental sync: every 4 hours");
console.log("  - Full sync: daily at 2am");

// Run initial incremental sync on startup
console.log("\nRunning initial sync on startup...");
syncAllStores(false).catch(console.error);
