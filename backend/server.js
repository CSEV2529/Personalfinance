/**
 * OBSIDIAN — Backend Server
 * Express + PostgreSQL (Supabase) + Plaid
 */

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const path = require('path');
const { Configuration, PlaidApi, PlaidEnvironments, Products, CountryCode } = require('plaid');
const Anthropic = require('@anthropic-ai/sdk');

const app = express();
app.use(express.json());
app.use(cors({ origin: '*' }));

// ─── DATABASE ────────────────────────────────────────────────────
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('supabase') ? { rejectUnauthorized: false } : false,
});

// Force NUMERIC/DECIMAL types to return as JavaScript numbers, not strings
const pg = require('pg');
pg.types.setTypeParser(1700, val => parseFloat(val)); // NUMERIC
pg.types.setTypeParser(20, val => parseInt(val)); // INT8/BIGINT

console.log('  DB: PostgreSQL pool created');

// ─── HELPER: convert SQLite ?-style params to $1,$2,... ─────────
function sqliteToPostgres(sql, params = []) {
  // If the query already uses $1-style params, don't convert
  if (/\$\d/.test(sql)) return { text: sql, values: params };
  let i = 0;
  const text = sql.replace(/\?/g, () => `$${++i}`);
  return { text, values: params };
}

async function run(sql, params = []) {
  const { text, values } = sqliteToPostgres(sql, params);
  const result = await pool.query(text, values);
  return { changes: result.rowCount };
}

async function get(sql, params = []) {
  const { text, values } = sqliteToPostgres(sql, params);
  const result = await pool.query(text, values);
  return result.rows[0] || null;
}

async function all(sql, params = []) {
  const { text, values } = sqliteToPostgres(sql, params);
  const result = await pool.query(text, values);
  return result.rows;
}

async function initDB() {
  await pool.query(`CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY, name TEXT NOT NULL,
    household TEXT NOT NULL DEFAULT 'default',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    health_score INTEGER DEFAULT 0,
    health_updated TEXT,
    streak_count INTEGER DEFAULT 0,
    streak_best INTEGER DEFAULT 0,
    last_active_date TEXT
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS plaid_items (
    item_id TEXT PRIMARY KEY, user_id TEXT NOT NULL,
    access_token TEXT NOT NULL, institution TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS transactions (
    id TEXT PRIMARY KEY, household TEXT NOT NULL DEFAULT 'default',
    user_id TEXT, "desc" TEXT NOT NULL, amount NUMERIC(12,2) NOT NULL,
    type TEXT NOT NULL, cat TEXT NOT NULL, date TEXT NOT NULL,
    pending BOOLEAN DEFAULT FALSE, account_id TEXT,
    source TEXT DEFAULT 'manual',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    notes TEXT,
    is_recurring BOOLEAN DEFAULT FALSE,
    recurring_group_id TEXT,
    reviewed BOOLEAN DEFAULT FALSE,
    original_cat TEXT,
    original_sign INTEGER DEFAULT -1,
    status TEXT DEFAULT 'confirmed'
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS accounts (
    account_id TEXT PRIMARY KEY, user_id TEXT,
    household TEXT NOT NULL DEFAULT 'default',
    name TEXT, mask TEXT, type TEXT, subtype TEXT,
    balance NUMERIC(12,2), updated_at TIMESTAMPTZ DEFAULT NOW()
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS budgets (
    household TEXT NOT NULL, category TEXT NOT NULL, amount NUMERIC(12,2) NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW(), PRIMARY KEY (household, category)
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS vendor_rules (
    household TEXT NOT NULL, vendor TEXT NOT NULL, category TEXT NOT NULL,
    type TEXT NOT NULL DEFAULT 'expense', updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (household, vendor)
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS category_icons (
    household TEXT NOT NULL, category TEXT NOT NULL, icon TEXT NOT NULL,
    PRIMARY KEY (household, category)
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS transaction_tags (
    id SERIAL PRIMARY KEY,
    transaction_id TEXT NOT NULL, tag TEXT NOT NULL, household TEXT NOT NULL,
    UNIQUE(transaction_id, tag)
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS categories (
    id TEXT NOT NULL, household TEXT NOT NULL,
    icon TEXT DEFAULT '📌', color TEXT DEFAULT '#7a78a0',
    type TEXT DEFAULT 'expense', sort_order INTEGER DEFAULT 0, is_active BOOLEAN DEFAULT TRUE,
    budget_amount NUMERIC(12,2) DEFAULT 0,
    PRIMARY KEY (id, household)
  )`);
  // Ensure budget_amount column exists (for existing DBs)
  try { await pool.query('ALTER TABLE categories ADD COLUMN budget_amount NUMERIC(12,2) DEFAULT 0'); } catch(e) {}
  await pool.query(`CREATE TABLE IF NOT EXISTS recurring_rules (
    id TEXT PRIMARY KEY, household TEXT NOT NULL, vendor TEXT NOT NULL,
    category TEXT, expected_amount NUMERIC(12,2), frequency TEXT,
    is_subscription BOOLEAN DEFAULT FALSE, last_seen TEXT, is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS challenges (
    id TEXT PRIMARY KEY, household TEXT NOT NULL, month TEXT NOT NULL,
    title TEXT NOT NULL, description TEXT, target_value NUMERIC(12,2),
    current_value NUMERIC(12,2) DEFAULT 0, challenge_type TEXT NOT NULL, category TEXT,
    is_completed BOOLEAN DEFAULT FALSE, created_at TIMESTAMPTZ DEFAULT NOW()
  )`);

  // ── SEED CATEGORIES ──
  const defaultCats = {
    Housing: { icon: '🏠', color: '#ffd700' }, Food: { icon: '🍽️', color: '#34d399' },
    Transport: { icon: '🚗', color: '#60a5fa' }, Health: { icon: '💊', color: '#f472b6' },
    Entertainment: { icon: '🎬', color: '#a78bfa' }, Shopping: { icon: '🛍️', color: '#fb923c' },
    Utilities: { icon: '⚡', color: '#2dd4bf' }, Income: { icon: '💰', color: '#34d399' },
    Transfer: { icon: '🔁', color: '#a78bfa' }, Other: { icon: '📌', color: '#6e6c8e' },
    Vacation: { icon: '🏖️', color: '#f59e0b' }, Subscriptions: { icon: '🔄', color: '#818cf8' },
    Pets: { icon: '🐾', color: '#fb7185' }, Personal: { icon: '💆', color: '#c084fc' },
    Education: { icon: '🎓', color: '#38bdf8' }, Insurance: { icon: '🛡️', color: '#94a3b8' }
  };
  for (const [name, info] of Object.entries(defaultCats)) {
    await pool.query(
      `INSERT INTO categories (id, household, icon, color) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`,
      [name, 'spenziero', info.icon, info.color]
    );
  }

  // Migrate existing category_icons
  try {
    const existingIcons = await all('SELECT category, icon FROM category_icons WHERE household = ?', ['spenziero']);
    for (const row of existingIcons) {
      await pool.query('UPDATE categories SET icon = $1 WHERE id = $2 AND household = $3', [row.icon, row.category, 'spenziero']);
    }
  } catch(e) {}

  // ── MIGRATE REFUND → INCOME (Phase 4) ──
  await run("UPDATE transactions SET type = 'income' WHERE type = 'refund'");
  await run("UPDATE vendor_rules SET type = 'income' WHERE type = 'refund'");
  // Ensure all amounts are positive (Math.abs) — undo any negative amount migration
  await run("UPDATE transactions SET amount = ABS(amount) WHERE amount < 0");

  await pool.query(`INSERT INTO users (id, name, household) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, ['christian', 'Christian', 'spenziero']);
  await pool.query(`INSERT INTO users (id, name, household) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, ['wife', 'Marisol', 'spenziero']);
  const defaultBudgets = {Housing:2000,Food:800,Transport:400,Health:300,Entertainment:200,Shopping:400,Utilities:250};
  for (const [cat, amt] of Object.entries(defaultBudgets)) {
    await pool.query('INSERT INTO budgets (household, category, amount) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING', ['spenziero', cat, amt]);
  }
  console.log('  DB: schema ready');
}

// ─── PLAID ────────────────────────────────────────────────────────
const plaidConfig = new Configuration({
  basePath: PlaidEnvironments[process.env.PLAID_ENV || 'sandbox'],
  baseOptions: {
    headers: {
      'PLAID-CLIENT-ID': process.env.PLAID_CLIENT_ID,
      'PLAID-SECRET':    process.env.PLAID_SECRET,
    },
  },
});
const plaid = new PlaidApi(plaidConfig);

async function getHousehold(userId) {
  const user = await get('SELECT household FROM users WHERE id = ?', [userId]);
  return user?.household || 'default';
}

function mapCategory(plaidCat) {
  if (!plaidCat) return 'Other';
  const c = plaidCat.toUpperCase();
  if (c.includes('RENT') || c.includes('MORTGAGE') || c.includes('HOME'))         return 'Housing';
  if (c.includes('FOOD') || c.includes('RESTAURANT') || c.includes('GROCERY'))    return 'Food';
  if (c.includes('TRANSPORT') || c.includes('TRAVEL') || c.includes('GAS') || c.includes('AUTO')) return 'Transport';
  if (c.includes('MEDICAL') || c.includes('HEALTH') || c.includes('PHARMACY'))    return 'Health';
  if (c.includes('ENTERTAINMENT') || c.includes('RECREATION'))                    return 'Entertainment';
  if (c.includes('SHOPS') || c.includes('SHOPPING') || c.includes('MERCHANDISE')) return 'Shopping';
  if (c.includes('UTILITIES') || c.includes('TELECOM') || c.includes('INTERNET')) return 'Utilities';
  if (c.includes('TRANSFER_OUT') || c.includes('TRANSFER_IN'))                    return 'Transfer';
  if (c.includes('PAYROLL') || c.includes('INCOME') || c.includes('DEPOSIT'))     return 'Income';
  return 'Other';
}

async function fetchAndStorePlaidTransactions(accessToken, userId, itemId) {
  const household = await getHousehold(userId);
  const today = new Date();
  const start = new Date();
  start.setDate(today.getDate() - 365); // 12 MONTHS — NOT 90 DAYS
  const fmt = d => d.toISOString().split('T')[0];

  // ── PAGINATED FETCH ──
  let allTransactions = [];
  let allAccounts = [];
  let offset = 0;
  const batchSize = 500;
  let totalAvailable = Infinity;

  while (offset < totalAvailable) {
    const resp = await plaid.transactionsGet({
      access_token: accessToken,
      start_date: fmt(start),
      end_date: fmt(today),
      options: { count: batchSize, offset },
    });
    allTransactions.push(...resp.data.transactions);
    if (offset === 0) allAccounts = resp.data.accounts;
    totalAvailable = resp.data.total_transactions;
    offset += batchSize;
  }

  console.log(`  Fetched ${allTransactions.length} of ${totalAvailable} transactions (${Math.ceil(totalAvailable / batchSize)} pages)`);

  // ── LOAD VENDOR RULES ──
  const vendorRules = {};
  (await all('SELECT vendor, category, type FROM vendor_rules WHERE household = ?', [household]))
    .forEach(r => { vendorRules[r.vendor] = { cat: r.category, type: r.type }; });

  // ── UPSERT WITH THREE-TIER CATEGORIZATION (inside a transaction) ──
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    for (const t of allTransactions) {
      const rule = vendorRules[t.name];
      let cat, type, reviewed;

      if (rule) {
        // TIER 1: Vendor rule — category only, type from Plaid
        cat = rule.cat;
        // Type is ALWAYS determined by Plaid amount sign — NEVER from vendor rule
        type = cat === 'Transfer' ? 'transfer' : (t.amount > 0 ? 'expense' : 'income');
        reviewed = true;
      } else {
        // TIER 2: Check if this transaction was manually reviewed
        const existingResult = await client.query('SELECT cat, type, reviewed FROM transactions WHERE id = $1', [t.transaction_id]);
        const existing = existingResult.rows[0];
        if (existing && existing.reviewed) {
          cat = existing.cat;
          type = existing.type;
          reviewed = true;
        } else {
          // TIER 3: Plaid categorization
          cat = mapCategory(t.personal_finance_category?.primary || t.category?.[0]);
          // Trust Plaid's categorization — no keyword overrides
          // Plaid already tags inter-account transfers correctly
          // Things like Venmo, PayPal, mortgage payments are real expenses, not transfers
          // NO REFUND TYPE — only income, expense, transfer
          type = cat === 'Transfer' ? 'transfer' : (t.amount > 0 ? 'expense' : 'income');
          reviewed = false;
        }
      }

      const originalCat = t.personal_finance_category?.primary || t.category?.[0] || null;

      await client.query(
        `INSERT INTO transactions (id, household, user_id, "desc", amount, type, cat, date, pending, account_id, source, original_cat, reviewed)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'plaid', $11, $12)
         ON CONFLICT (id) DO UPDATE SET
           household = EXCLUDED.household, user_id = EXCLUDED.user_id, "desc" = EXCLUDED."desc",
           amount = EXCLUDED.amount, type = EXCLUDED.type, cat = EXCLUDED.cat, date = EXCLUDED.date,
           pending = EXCLUDED.pending, account_id = EXCLUDED.account_id, source = EXCLUDED.source,
           original_cat = EXCLUDED.original_cat, reviewed = EXCLUDED.reviewed`,
        [t.transaction_id, household, userId, t.name,
         Math.abs(t.amount), type, cat,
         t.date, t.pending ? true : false, t.account_id,
         originalCat, reviewed]
      );
    }
    for (const a of allAccounts) {
      await client.query(
        `INSERT INTO accounts (account_id, user_id, household, name, mask, type, subtype, balance, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
         ON CONFLICT (account_id) DO UPDATE SET
           user_id = EXCLUDED.user_id, household = EXCLUDED.household, name = EXCLUDED.name,
           mask = EXCLUDED.mask, type = EXCLUDED.type, subtype = EXCLUDED.subtype,
           balance = EXCLUDED.balance, updated_at = NOW()`,
        [a.account_id, userId, household,
         a.name, a.mask, a.type, a.subtype, a.balances.current]
      );
    }

    // Remove duplicate pending transactions (posted version exists)
    const dupesResult = await client.query(`
      SELECT p.id as pending_id FROM transactions p
      INNER JOIN transactions posted ON p."desc" = posted."desc" AND p.amount = posted.amount
        AND p.date = posted.date AND p.account_id = posted.account_id AND p.household = posted.household
      WHERE p.household = $1 AND p.pending = TRUE AND posted.pending = FALSE AND p.id != posted.id`, [household]);
    const dupes = dupesResult.rows;
    for (const d of dupes) await client.query('DELETE FROM transactions WHERE id = $1', [d.pending_id]);
    if (dupes.length) console.log(`  Removed ${dupes.length} duplicate pending transactions`);

    // Also remove cross-account duplicates (same desc, amount, date, household but different account_ids)
    const crossDupes = await client.query(`
      DELETE FROM transactions WHERE id IN (
        SELECT id FROM (
          SELECT id, ROW_NUMBER() OVER (PARTITION BY household, "desc", amount, date ORDER BY created_at ASC) as rn
          FROM transactions WHERE household = $1
        ) ranked WHERE rn > 1
      ) RETURNING id`, [household]);
    if (crossDupes.rowCount) console.log(`  Removed ${crossDupes.rowCount} cross-account duplicate transactions`);

    await client.query('COMMIT');
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }

  return { count: allTransactions.length, accounts: allAccounts.length };
}

// ─── ROUTES ──────────────────────────────────────────────────────
app.get('/api/users', async (req, res) => {
  try { res.json({ users: await all('SELECT id, name, household FROM users') }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/create_link_token', async (req, res) => {
  try {
    const linkConfig = {
      user: { client_user_id: req.body.userId || 'christian' },
      client_name: 'Obsidian',
      products: [Products.Transactions],
      country_codes: [CountryCode.Us],
      language: 'en',
      webhook: process.env.WEBHOOK_URL || undefined,
    };
    if (process.env.PLAID_REDIRECT_URI) linkConfig.redirect_uri = process.env.PLAID_REDIRECT_URI;
    const resp = await plaid.linkTokenCreate(linkConfig);
    res.json({ link_token: resp.data.link_token });
  } catch (err) {
    console.error('create_link_token:', err.response?.data || err.message);
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/exchange_public_token', async (req, res) => {
  const { public_token, userId = 'christian', institution } = req.body;
  try {
    // BULLETPROOF: On reconnect, wipe ALL old data for this household
    // Plaid generates new account IDs on every connection, so old IDs become orphans
    const hh = await getHousehold(userId);
    const existingItems = await all('SELECT item_id FROM plaid_items WHERE user_id IN (SELECT id FROM users WHERE household = $1)', [hh]);
    if (existingItems.length > 0) {
      console.log(`  Reconnecting — clearing old data for household ${hh}`);
      await pool.query('DELETE FROM plaid_items WHERE user_id IN (SELECT id FROM users WHERE household = $1)', [hh]);
      await pool.query('DELETE FROM transactions WHERE household = $1 AND source = $2', [hh, 'plaid']);
      await pool.query('DELETE FROM accounts WHERE household = $1', [hh]);
      console.log(`  Cleared old plaid items, plaid transactions, and accounts`);
    }

    const resp = await plaid.itemPublicTokenExchange({ public_token });
    const { access_token, item_id } = resp.data;
    await pool.query(
      `INSERT INTO plaid_items (item_id, user_id, access_token, institution)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (item_id) DO UPDATE SET
         user_id = EXCLUDED.user_id, access_token = EXCLUDED.access_token, institution = EXCLUDED.institution`,
      [item_id, userId, access_token, institution || 'Unknown']
    );
    console.log(`Bank connected: user=${userId}, item=${item_id}`);
    // Try to fetch transactions, but don't fail if not ready yet
    let result = { count: 0, accounts: 0 };
    try {
      result = await fetchAndStorePlaidTransactions(access_token, userId, item_id);
    } catch (fetchErr) {
      const code = fetchErr.response?.data?.error_code;
      if (code === 'PRODUCT_NOT_READY') {
        console.log('  Transactions not ready yet — will arrive via webhook or manual sync');
      } else {
        console.error('  Initial fetch failed:', fetchErr.response?.data || fetchErr.message);
      }
    }
    res.json({ success: true, item_id, ...result });
  } catch (err) {
    console.error('exchange_public_token:', err.response?.data || err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/transactions', async (req, res) => {
  const { userId = 'christian', days = 60, household: hhParam = 'true', startDate, endDate } = req.query;
  try {
    let txs;
    if (startDate && endDate) {
      if (hhParam === 'true') {
        const hh = await getHousehold(userId);
        txs = await all(`SELECT t.*, u.name as user_name FROM transactions t
          LEFT JOIN users u ON t.user_id = u.id
          WHERE t.household = ? AND t.date >= ? AND t.date <= ?
          ORDER BY t.date DESC, t.created_at DESC`, [hh, startDate, endDate]);
      } else {
        txs = await all(`SELECT * FROM transactions WHERE user_id = ? AND date >= ? AND date <= ?
          ORDER BY date DESC, created_at DESC`, [userId, startDate, endDate]);
      }
    } else {
      const cutoff = new Date(); cutoff.setDate(cutoff.getDate() - parseInt(days));
      const cutoffStr = cutoff.toISOString().split('T')[0];
      if (hhParam === 'true') {
        const hh = await getHousehold(userId);
        txs = await all(`SELECT t.*, u.name as user_name FROM transactions t
          LEFT JOIN users u ON t.user_id = u.id
          WHERE t.household = ? AND t.date >= ?
          ORDER BY t.date DESC, t.created_at DESC`, [hh, cutoffStr]);
      } else {
        txs = await all(`SELECT * FROM transactions WHERE user_id = ? AND date >= ?
          ORDER BY date DESC, created_at DESC`, [userId, cutoffStr]);
      }
    }
    res.json({ transactions: txs, total: txs.length });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/accounts', async (req, res) => {
  try {
    const hh = await getHousehold(req.query.userId || 'christian');
    const accounts = await all(`SELECT a.*, u.name as user_name FROM accounts a
      LEFT JOIN users u ON a.user_id = u.id
      WHERE a.household = ? ORDER BY a.type, a.name`, [hh]);
    res.json({ accounts });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/sync', async (req, res) => {
  const userId = req.body.userId || 'christian';
  try {
    const household = await getHousehold(userId);
    const members = await all('SELECT id FROM users WHERE household = ?', [household]);
    let totalTx = 0;
    for (const member of members) {
      const items = await all('SELECT * FROM plaid_items WHERE user_id = ?', [member.id]);
      for (const item of items) {
        try {
          const r = await fetchAndStorePlaidTransactions(item.access_token, member.id, item.item_id);
          totalTx += r.count;
        } catch (err) { console.error(`Sync failed: ${err.message}`); }
      }
    }
    res.json({ success: true, synced: totalTx });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/transactions', async (req, res) => {
  const { userId = 'christian', desc, amount, type, cat, date } = req.body;
  if (!desc || !amount || !type || !cat || !date)
    return res.status(400).json({ error: 'Missing required fields' });
  try {
    const household = await getHousehold(userId);
    const id = 'manual_' + Date.now() + '_' + Math.random().toString(36).slice(2);
    await run(`INSERT INTO transactions (id, household, user_id, "desc", amount, type, cat, date, source)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'manual')`,
      [id, household, userId, desc, parseFloat(amount), type, cat, date]);
    res.json({ success: true, id });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── TRANSACTION DETAIL ─────────────────────────────────────────
// Static routes MUST come before parameterized /:id routes
app.get('/api/transactions/unsure', async (req, res) => {
  try {
    const hh = await getHousehold(req.query.userId || 'christian');
    const txs = await all(`SELECT t.*, u.name as user_name FROM transactions t
      LEFT JOIN users u ON t.user_id=u.id
      WHERE t.household=? AND t.status='unsure' ORDER BY t.date DESC`, [hh]);
    res.json({ transactions: txs });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/transactions/:id/status', async (req, res) => {
  const { status } = req.body;
  if (!status) return res.status(400).json({ error: 'Missing status' });
  try {
    await run('UPDATE transactions SET status=? WHERE id=?', [status, req.params.id]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/transactions/:id', async (req, res) => {
  try {
    const tx = await get(`SELECT t.*, u.name as user_name FROM transactions t
      LEFT JOIN users u ON t.user_id = u.id WHERE t.id = ?`, [req.params.id]);
    if (!tx) return res.status(404).json({ error: 'Transaction not found' });
    const tags = (await all('SELECT tag FROM transaction_tags WHERE transaction_id = ?', [req.params.id])).map(r => r.tag);
    const recurring = tx.recurring_group_id ? await get('SELECT * FROM recurring_rules WHERE id = ?', [tx.recurring_group_id]) : null;
    res.json({ ...tx, tags, recurring });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/transactions/:id', async (req, res) => {
  const { category, type, notes, is_recurring, applyToVendor, original_sign, status } = req.body;
  try {
    const tx = await get('SELECT * FROM transactions WHERE id = ?', [req.params.id]);
    if (!tx) return res.status(404).json({ error: 'Transaction not found' });
    const newCat = category || tx.cat;
    const newType = type || tx.type;
    const newNotes = notes !== undefined ? notes : tx.notes;
    const newRecurring = is_recurring !== undefined ? (is_recurring ? true : false) : tx.is_recurring;
    const newSign = original_sign !== undefined ? original_sign : tx.original_sign;
    const newStatus = status || tx.status || 'confirmed';
    await run('UPDATE transactions SET cat=?, type=?, notes=?, is_recurring=?, original_sign=?, status=?, reviewed=TRUE WHERE id=?',
      [newCat, newType, newNotes, newRecurring, newSign, newStatus, req.params.id]);
    let updatedCount = 1;
    if (applyToVendor) {
      // Vendor rule stores ONLY category
      await pool.query(
        `INSERT INTO vendor_rules (household, vendor, category, type, updated_at) VALUES ($1,$2,$3,'expense',NOW())
         ON CONFLICT (household, vendor) DO UPDATE SET category = EXCLUDED.category, type = EXCLUDED.type, updated_at = NOW()`,
        [tx.household, tx.desc, newCat]
      );
      // Apply category only — preserve each tx's type and sign
      const result = await run('UPDATE transactions SET cat=?, reviewed=TRUE WHERE household=? AND "desc"=?',
        [newCat, tx.household, tx.desc]);
      updatedCount = result.changes || 1;
    }
    res.json({ success: true, updated: updatedCount });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Legacy endpoint (frontend compat)
app.put('/api/transactions/:id/category', async (req, res) => {
  const { category, type, applyToVendor, original_sign } = req.body;
  if (!category && !type && original_sign === undefined) return res.status(400).json({ error: 'Missing category, type, or sign' });
  try {
    const tx = await get('SELECT * FROM transactions WHERE id = ?', [req.params.id]);
    if (!tx) return res.status(404).json({ error: 'Transaction not found' });
    const newCat = category || tx.cat;
    const newType = type || tx.type;
    const newSign = original_sign !== undefined ? original_sign : tx.original_sign;
    // Always update this specific transaction (type + sign + category)
    await run('UPDATE transactions SET cat=?, type=?, original_sign=?, reviewed=TRUE WHERE id=?', [newCat, newType, newSign, req.params.id]);
    let updatedCount = 1;
    if (applyToVendor) {
      // Vendor rule stores ONLY category — not type, not sign
      await pool.query(
        `INSERT INTO vendor_rules (household, vendor, category, type, updated_at) VALUES ($1,$2,$3,'expense',NOW())
         ON CONFLICT (household, vendor) DO UPDATE SET category = EXCLUDED.category, type = EXCLUDED.type, updated_at = NOW()`,
        [tx.household, tx.desc, newCat]
      );
      // Apply category to all matching, but PRESERVE their type and sign
      const result = await run('UPDATE transactions SET cat=?, reviewed=TRUE WHERE household=? AND "desc"=?',
        [newCat, tx.household, tx.desc]);
      updatedCount = result.changes || 1;
    }
    res.json({ success: true, updated: updatedCount });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/transactions/:id/tags', async (req, res) => {
  const { tag } = req.body;
  if (!tag) return res.status(400).json({ error: 'Missing tag' });
  try {
    const tx = await get('SELECT household FROM transactions WHERE id = ?', [req.params.id]);
    if (!tx) return res.status(404).json({ error: 'Transaction not found' });
    await pool.query(
      'INSERT INTO transaction_tags (transaction_id, tag, household) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING',
      [req.params.id, tag.trim(), tx.household]
    );
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/transactions/:id/tags/:tag', async (req, res) => {
  try {
    await run('DELETE FROM transaction_tags WHERE transaction_id=? AND tag=?', [req.params.id, req.params.tag]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/transactions/:id', async (req, res) => {
  try {
    await run('DELETE FROM transaction_tags WHERE transaction_id = ?', [req.params.id]);
    await run('DELETE FROM transactions WHERE id = ?', [req.params.id]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── CATEGORY MANAGEMENT ────────────────────────────────────────
app.get('/api/categories', async (req, res) => {
  try {
    const hh = await getHousehold(req.query.userId || 'christian');
    const cats = await all(`SELECT c.*, COALESCE(s.total,0) as spent, COALESCE(s.cnt,0) as tx_count
      FROM categories c LEFT JOIN (
        SELECT cat, SUM(amount) as total, COUNT(*) as cnt FROM transactions
        WHERE household=? AND type='expense' GROUP BY cat
      ) s ON c.id = s.cat
      WHERE c.household=? AND c.is_active=TRUE ORDER BY c.sort_order`, [hh, hh]);
    res.json({ categories: cats });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/categories', async (req, res) => {
  const { userId = 'christian', name, icon, color, type = 'expense' } = req.body;
  if (!name) return res.status(400).json({ error: 'Missing name' });
  try {
    const hh = await getHousehold(userId);
    const maxOrder = await get('SELECT MAX(sort_order) as m FROM categories WHERE household=?', [hh]);
    await pool.query(
      'INSERT INTO categories (id,household,icon,color,type,sort_order) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING',
      [name, hh, icon || '📌', color || '#7a78a0', type, (maxOrder?.m || 0) + 1]
    );
    // Also sync to category_icons for backward compat
    if (icon) {
      await pool.query(
        `INSERT INTO category_icons (household,category,icon) VALUES ($1,$2,$3)
         ON CONFLICT (household, category) DO UPDATE SET icon = EXCLUDED.icon`,
        [hh, name, icon]
      );
    }
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/categories/:id', async (req, res) => {
  const { userId = 'christian', name, icon, color, sort_order, type, budget_amount } = req.body;
  try {
    const hh = await getHousehold(userId);
    const catId = req.params.id;

    if (name && name !== catId) {
      // Rename: update all references
      await pool.query('UPDATE transactions SET cat=$1 WHERE household=$2 AND cat=$3', [name, hh, catId]);
      await pool.query('UPDATE vendor_rules SET category=$1 WHERE household=$2 AND category=$3', [name, hh, catId]);
      await pool.query('DELETE FROM categories WHERE household=$1 AND id=$2', [hh, catId]);
      const oldCat = await pool.query('SELECT * FROM categories WHERE household=$1 AND id=$2', [hh, catId]);
      const old = oldCat.rows[0] || {};
      await pool.query(
        `INSERT INTO categories (id,household,icon,color,type,sort_order,is_active,budget_amount) VALUES ($1,$2,$3,$4,$5,$6,TRUE,$7)
         ON CONFLICT (id, household) DO UPDATE SET icon=EXCLUDED.icon, color=EXCLUDED.color, type=EXCLUDED.type, sort_order=EXCLUDED.sort_order, is_active=TRUE, budget_amount=EXCLUDED.budget_amount`,
        [name, hh, icon||old.icon||'📌', color||old.color||'#7a78a0', type||old.type||'expense', sort_order!==undefined?sort_order:(old.sort_order||0), budget_amount!==undefined?budget_amount:(old.budget_amount||0)]
      );
    } else {
      // Simple update — build SET clause
      const sets = [];
      const vals = [];
      let i = 0;
      if (icon !== undefined) { i++; sets.push(`icon=$${i}`); vals.push(icon); }
      if (color !== undefined) { i++; sets.push(`color=$${i}`); vals.push(color); }
      if (sort_order !== undefined) { i++; sets.push(`sort_order=$${i}`); vals.push(sort_order); }
      if (type !== undefined) { i++; sets.push(`type=$${i}`); vals.push(type); }
      if (budget_amount !== undefined) { i++; sets.push(`budget_amount=$${i}`); vals.push(budget_amount); }
      if (sets.length) {
        i++; vals.push(hh);
        i++; vals.push(catId);
        await pool.query(`UPDATE categories SET ${sets.join(',')} WHERE household=$${i-1} AND id=$${i}`, vals);
      }
    }
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/categories/:id', async (req, res) => {
  const { userId = 'christian' } = req.query;
  try {
    const hh = await getHousehold(userId);
    await run('UPDATE transactions SET cat=? WHERE household=? AND cat=?', ['Other', hh, req.params.id]);
    await run('UPDATE categories SET is_active=FALSE WHERE household=? AND id=?', [hh, req.params.id]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/categories/:id/transactions', async (req, res) => {
  const { userId = 'christian', group_by, startDate, endDate } = req.query;
  try {
    const hh = await getHousehold(userId);
    let txs;
    if (startDate && endDate) {
      txs = await all(`SELECT t.*, u.name as user_name FROM transactions t
        LEFT JOIN users u ON t.user_id=u.id
        WHERE t.household=? AND t.cat=? AND t.date>=? AND t.date<=?
        ORDER BY t.date DESC`, [hh, req.params.id, startDate, endDate]);
    } else {
      txs = await all(`SELECT t.*, u.name as user_name FROM transactions t
        LEFT JOIN users u ON t.user_id=u.id
        WHERE t.household=? AND t.cat=?
        ORDER BY t.date DESC LIMIT 200`, [hh, req.params.id]);
    }
    if (group_by === 'vendor') {
      const groups = {};
      txs.forEach(t => {
        if (!groups[t.desc]) groups[t.desc] = { vendor: t.desc, total: 0, count: 0, transactions: [] };
        groups[t.desc].total += parseFloat(t.amount);
        groups[t.desc].count++;
        groups[t.desc].transactions.push(t);
      });
      return res.json({ groups: Object.values(groups).sort((a,b) => b.total - a.total) });
    }
    res.json({ transactions: txs });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── TRANSACTION STATUS (unsure/confirmed) ──────────────────────
// ─── VENDOR RULES ───────────────────────────────────────────────
app.get('/api/vendor-rules', async (req, res) => {
  try {
    const hh = await getHousehold(req.query.userId || 'christian');
    const rules = await all('SELECT vendor, category, type FROM vendor_rules WHERE household = ?', [hh]);
    res.json({ rules });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Legacy category-icons endpoints (kept for backward compat)
app.get('/api/category-icons', async (req, res) => {
  try {
    const hh = await getHousehold(req.query.userId || 'christian');
    // Serve from categories table now
    const icons = await all('SELECT id as category, icon FROM categories WHERE household=? AND is_active=TRUE', [hh]);
    res.json({ icons });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/category-icons', async (req, res) => {
  const { userId = 'christian', category, icon } = req.body;
  if (!category || !icon) return res.status(400).json({ error: 'Missing category or icon' });
  try {
    const hh = await getHousehold(userId);
    await pool.query(
      `INSERT INTO category_icons (household, category, icon) VALUES ($1, $2, $3)
       ON CONFLICT (household, category) DO UPDATE SET icon = EXCLUDED.icon`,
      [hh, category, icon]
    );
    // Also update categories table
    await run('UPDATE categories SET icon=? WHERE household=? AND id=?', [icon, hh, category]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── RECURRING & SUBSCRIPTIONS ──────────────────────────────────
app.get('/api/recurring', async (req, res) => {
  try {
    const hh = await getHousehold(req.query.userId || 'christian');
    const rules = await all('SELECT * FROM recurring_rules WHERE household=? AND is_active=TRUE ORDER BY last_seen DESC', [hh]);
    res.json({ recurring: rules });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/recurring', async (req, res) => {
  const { userId = 'christian', vendor, category, expected_amount, frequency = 'monthly', is_subscription = 0 } = req.body;
  if (!vendor) return res.status(400).json({ error: 'Missing vendor' });
  try {
    const hh = await getHousehold(userId);
    // Check if rule already exists for this vendor
    const existing = await get('SELECT id FROM recurring_rules WHERE household=? AND vendor=? AND is_active=TRUE', [hh, vendor]);
    if (existing) return res.json({ success: true, id: existing.id, existing: true });
    const id = 'rec_' + Date.now() + '_' + Math.random().toString(36).slice(2);
    await run(`INSERT INTO recurring_rules (id,household,vendor,category,expected_amount,frequency,is_subscription,last_seen)
      VALUES (?,?,?,?,?,?,?,CURRENT_DATE)`, [id, hh, vendor, category, expected_amount, frequency, is_subscription ? true : false]);
    await run('UPDATE transactions SET is_recurring=TRUE, recurring_group_id=? WHERE household=? AND "desc"=?', [id, hh, vendor]);
    res.json({ success: true, id });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/recurring/:id', async (req, res) => {
  const { frequency, expected_amount, is_active, is_subscription } = req.body;
  try {
    const updates = [], params = [];
    let paramIdx = 0;
    if (frequency !== undefined) { paramIdx++; updates.push(`frequency=$${paramIdx}`); params.push(frequency); }
    if (expected_amount !== undefined) { paramIdx++; updates.push(`expected_amount=$${paramIdx}`); params.push(expected_amount); }
    if (is_active !== undefined) { paramIdx++; updates.push(`is_active=$${paramIdx}`); params.push(is_active ? true : false); }
    if (is_subscription !== undefined) { paramIdx++; updates.push(`is_subscription=$${paramIdx}`); params.push(is_subscription ? true : false); }
    if (!updates.length) return res.json({ success: true });
    paramIdx++; params.push(req.params.id);
    await pool.query(`UPDATE recurring_rules SET ${updates.join(',')} WHERE id=$${paramIdx}`, params);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/recurring/detect', async (req, res) => {
  const { userId = 'christian' } = req.body;
  try {
    const hh = await getHousehold(userId);
    // Group transactions by vendor, analyze date patterns
    const vendors = await all(`SELECT "desc" as vendor, STRING_AGG(date, ',' ORDER BY date) as dates, STRING_AGG(amount::text, ',') as amounts,
      COUNT(*) as cnt, AVG(amount) as avg_amt, cat
      FROM transactions WHERE household=? AND type='expense'
      GROUP BY "desc", cat HAVING COUNT(*) >= 3 ORDER BY COUNT(*) DESC`, [hh]);

    const suggestions = [];
    for (const v of vendors) {
      // Check if already a rule
      const existing = await get('SELECT id FROM recurring_rules WHERE household=? AND vendor=?', [hh, v.vendor]);
      if (existing) continue;

      const dates = v.dates.split(',').map(d => new Date(d)).sort((a,b) => a-b);
      const amounts = v.amounts.split(',').map(Number);
      if (dates.length < 3) continue;

      // Calculate gaps between consecutive transactions
      const gaps = [];
      for (let i = 1; i < dates.length; i++) {
        gaps.push((dates[i] - dates[i-1]) / (1000*60*60*24));
      }
      const avgGap = gaps.reduce((a,b) => a+b, 0) / gaps.length;

      let frequency = null;
      if (avgGap >= 5 && avgGap <= 9) frequency = 'weekly';
      else if (avgGap >= 25 && avgGap <= 35) frequency = 'monthly';
      else if (avgGap >= 80 && avgGap <= 100) frequency = 'quarterly';
      else if (avgGap >= 350 && avgGap <= 380) frequency = 'annual';

      if (!frequency) continue;

      // Check amount consistency for subscription detection
      const amtVariance = amounts.length > 1 ? Math.max(...amounts) / Math.min(...amounts) : 1;
      const is_subscription = amtVariance <= 1.1 ? 1 : 0;

      suggestions.push({
        vendor: v.vendor, category: v.cat, expected_amount: parseFloat(v.avg_amt),
        frequency, is_subscription, tx_count: parseInt(v.cnt),
        last_seen: dates[dates.length-1].toISOString().split('T')[0]
      });
    }
    res.json({ suggestions });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/subscriptions', async (req, res) => {
  try {
    const hh = await getHousehold(req.query.userId || 'christian');
    const subs = await all('SELECT * FROM recurring_rules WHERE household=? AND is_subscription=TRUE AND is_active=TRUE', [hh]);
    const monthlyTotal = subs.reduce((a, s) => {
      const mult = s.frequency === 'weekly' ? 4.33 : s.frequency === 'quarterly' ? 1/3 : s.frequency === 'annual' ? 1/12 : 1;
      return a + (parseFloat(s.expected_amount) || 0) * mult;
    }, 0);
    res.json({ subscriptions: subs, monthly_total: monthlyTotal });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/recurring/dedup', async (req, res) => {
  const { userId = 'christian' } = req.body;
  try {
    const hh = await getHousehold(userId);
    // Keep the earliest rule per vendor, delete the rest
    const dupes = await all(`SELECT id FROM recurring_rules WHERE household=? AND id NOT IN (
      SELECT MIN(id) FROM recurring_rules WHERE household=? GROUP BY vendor
    )`, [hh, hh]);
    for (const d of dupes) await run('DELETE FROM recurring_rules WHERE id=?', [d.id]);
    res.json({ success: true, removed: dupes.length });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/recurring/dismiss', async (req, res) => {
  const { userId = 'christian', vendor } = req.body;
  if (!vendor) return res.status(400).json({ error: 'Missing vendor' });
  try {
    const hh = await getHousehold(userId);
    const id = 'rec_dismissed_' + Date.now() + '_' + Math.random().toString(36).slice(2);
    await pool.query(
      `INSERT INTO recurring_rules (id,household,vendor,is_active,frequency) VALUES ($1,$2,$3,FALSE,'dismissed')
       ON CONFLICT (id) DO UPDATE SET is_active=FALSE, frequency='dismissed'`,
      [id, hh, vendor]
    );
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── ANALYTICS & TRENDS ─────────────────────────────────────────
app.get('/api/trends/categories', async (req, res) => {
  const { userId = 'christian', months = 6 } = req.query;
  try {
    const hh = await getHousehold(userId);
    const cutoffDate = new Date();
    cutoffDate.setMonth(cutoffDate.getMonth() - parseInt(months));
    const cutoffStr = cutoffDate.toISOString().split('T')[0];
    const data = await all(`SELECT cat, TO_CHAR(date::date, 'YYYY-MM') as month, SUM(amount) as total
      FROM transactions WHERE household=? AND type='expense'
        AND date >= ?
      GROUP BY cat, TO_CHAR(date::date, 'YYYY-MM') ORDER BY TO_CHAR(date::date, 'YYYY-MM'), SUM(amount) DESC`, [hh, cutoffStr]);
    // Compute MoM change per category
    const byCategory = {};
    data.forEach(r => {
      if (!byCategory[r.cat]) byCategory[r.cat] = [];
      byCategory[r.cat].push({ month: r.month, amount: parseFloat(r.total) });
    });
    const result = {};
    for (const [cat, months_data] of Object.entries(byCategory)) {
      result[cat] = months_data.map((m, i) => ({
        ...m,
        prev_amount: i > 0 ? months_data[i-1].amount : null,
        change_pct: i > 0 ? ((m.amount - months_data[i-1].amount) / months_data[i-1].amount * 100) : null,
      }));
    }
    res.json({ data: result });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/trends/vendors', async (req, res) => {
  const { userId = 'christian', vendor, months = 6 } = req.query;
  try {
    const hh = await getHousehold(userId);
    const cutoffDate = new Date();
    cutoffDate.setMonth(cutoffDate.getMonth() - parseInt(months));
    const cutoffStr = cutoffDate.toISOString().split('T')[0];
    let data;
    if (vendor) {
      data = await all(`SELECT "desc" as vendor, TO_CHAR(date::date, 'YYYY-MM') as month, SUM(amount) as total, COUNT(*) as cnt
        FROM transactions WHERE household=? AND type='expense'
          AND date >= ? AND "desc"=?
        GROUP BY "desc", TO_CHAR(date::date, 'YYYY-MM') ORDER BY TO_CHAR(date::date, 'YYYY-MM')`, [hh, cutoffStr, vendor]);
    } else {
      data = await all(`SELECT "desc" as vendor, TO_CHAR(date::date, 'YYYY-MM') as month, SUM(amount) as total, COUNT(*) as cnt
        FROM transactions WHERE household=? AND type='expense'
          AND date >= ?
        GROUP BY "desc", TO_CHAR(date::date, 'YYYY-MM') ORDER BY TO_CHAR(date::date, 'YYYY-MM')`, [hh, cutoffStr]);
    }
    res.json({ data });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/trends/cashflow', async (req, res) => {
  const { userId = 'christian', months = '6' } = req.query;
  try {
    const hh = await getHousehold(userId);
    const numMonths = parseInt(months) || 6;
    const now = new Date();
    const data = [];
    for (let i = numMonths - 1; i >= 0; i--) {
      const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
      const start = d.toISOString().split('T')[0];
      const end = new Date(d.getFullYear(), d.getMonth() + 1, 0).toISOString().split('T')[0];
      const month = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
      const txs = await all('SELECT type, SUM(amount) as total FROM transactions WHERE household = ? AND date >= ? AND date <= ? GROUP BY type', [hh, start, end]);
      const income = parseFloat(txs.find(t => t.type === 'income')?.total) || 0;
      const expenses = parseFloat(txs.find(t => t.type === 'expense')?.total) || 0;
      data.push({ month, income, expenses, net: income - expenses });
    }
    res.json({ data });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/trends/daily-average', async (req, res) => {
  const { userId = 'christian', month } = req.query;
  try {
    const hh = await getHousehold(userId);
    const now = new Date();
    const targetMonth = month || `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
    const [y, m] = targetMonth.split('-').map(Number);
    const daysInMonth = new Date(y, m, 0).getDate();
    const isCurrentMonth = y === now.getFullYear() && m === now.getMonth()+1;
    const daysElapsed = isCurrentMonth ? now.getDate() : daysInMonth;

    const result = await get(`SELECT SUM(amount) as total FROM transactions
      WHERE household=? AND type='expense' AND TO_CHAR(date::date, 'YYYY-MM')=?`, [hh, targetMonth]);
    const total = parseFloat(result?.total) || 0;
    const dailyAvg = daysElapsed > 0 ? total / daysElapsed : 0;

    // Previous month for comparison
    const prevDate = new Date(y, m - 2, 1);
    const prevMonth = `${prevDate.getFullYear()}-${String(prevDate.getMonth()+1).padStart(2,'0')}`;
    const prevDays = new Date(prevDate.getFullYear(), prevDate.getMonth()+1, 0).getDate();
    const prevResult = await get(`SELECT SUM(amount) as total FROM transactions
      WHERE household=? AND type='expense' AND TO_CHAR(date::date, 'YYYY-MM')=?`, [hh, prevMonth]);
    const prevDailyAvg = prevDays > 0 ? (parseFloat(prevResult?.total) || 0) / prevDays : 0;

    res.json({ daily_average: dailyAvg, total, days: daysElapsed,
      prev_daily_average: prevDailyAvg, change_pct: prevDailyAvg > 0 ? ((dailyAvg - prevDailyAvg)/prevDailyAvg*100) : null });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/income/breakdown', async (req, res) => {
  const { userId = 'christian', startDate, endDate } = req.query;
  try {
    const hh = await getHousehold(userId);
    if (startDate && endDate) {
      const byCategory = await all(`SELECT cat as category, SUM(amount) as total, COUNT(*) as cnt
        FROM transactions WHERE household=? AND type='income' AND date>=? AND date<=?
        GROUP BY cat ORDER BY SUM(amount) DESC`, [hh, startDate, endDate]);
      const byMerchant = await all(`SELECT "desc" as merchant, SUM(amount) as total, COUNT(*) as cnt, cat
        FROM transactions WHERE household=? AND type='income' AND date>=? AND date<=?
        GROUP BY "desc", cat ORDER BY SUM(amount) DESC`, [hh, startDate, endDate]);
      res.json({ by_category: byCategory, by_merchant: byMerchant });
    } else {
      const cutoffDate = new Date();
      cutoffDate.setMonth(cutoffDate.getMonth() - 12);
      const cutoffStr = cutoffDate.toISOString().split('T')[0];
      const byCategory = await all(`SELECT cat as category, SUM(amount) as total, COUNT(*) as cnt
        FROM transactions WHERE household=? AND type='income' AND date >= ?
        GROUP BY cat ORDER BY SUM(amount) DESC`, [hh, cutoffStr]);
      const byMerchant = await all(`SELECT "desc" as merchant, SUM(amount) as total, COUNT(*) as cnt, cat
        FROM transactions WHERE household=? AND type='income' AND date >= ?
        GROUP BY "desc", cat ORDER BY SUM(amount) DESC`, [hh, cutoffStr]);
      res.json({ by_category: byCategory, by_merchant: byMerchant });
    }
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── REVIEW QUEUE & WIZARD ──────────────────────────────────────
app.get('/api/review-queue', async (req, res) => {
  try {
    const hh = await getHousehold(req.query.userId || 'christian');
    const vendorRows = await all(`SELECT "desc" as vendor, COUNT(*) as cnt, SUM(amount) as total,
      STRING_AGG(DISTINCT cat, ',') as current_cats, MIN(date) as first_seen, MAX(date) as last_seen
      FROM transactions WHERE household=? AND cat='Other' AND reviewed=FALSE
      GROUP BY "desc" ORDER BY SUM(amount) DESC`, [hh]);
    // Include individual transactions per vendor
    const vendors = [];
    for (const v of vendorRows) {
      const txs = await all(`SELECT id, date, "desc", amount, type FROM transactions
        WHERE household=? AND "desc"=? AND cat='Other' AND reviewed=FALSE
        ORDER BY date DESC LIMIT 20`, [hh, v.vendor]);
      vendors.push({ ...v, transactions: txs });
    }
    res.json({ queue: vendors, total_unreviewed: vendors.reduce((a,v) => a + parseInt(v.cnt), 0) });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/review-queue/resolve', async (req, res) => {
  const { userId = 'christian', vendor, category, type = 'expense' } = req.body;
  if (!vendor || !category) return res.status(400).json({ error: 'Missing vendor or category' });
  try {
    const hh = await getHousehold(userId);
    await pool.query(
      `INSERT INTO vendor_rules (household, vendor, category, type) VALUES ($1,$2,$3,$4)
       ON CONFLICT (household, vendor) DO UPDATE SET category = EXCLUDED.category, type = EXCLUDED.type`,
      [hh, vendor, category, type]
    );
    // Only update category — NEVER override Plaid-determined type
    const result = await run('UPDATE transactions SET cat=?, reviewed=TRUE WHERE household=? AND "desc"=?',
      [category, hh, vendor]);
    res.json({ success: true, updated: result.changes });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/wizard/uncategorized-vendors', async (req, res) => {
  try {
    const hh = await getHousehold(req.query.userId || 'christian');
    const vendors = await all(`SELECT t."desc" as vendor, COUNT(*) as cnt, SUM(t.amount) as total, t.cat as current_cat
      FROM transactions t
      LEFT JOIN vendor_rules vr ON vr.household=t.household AND vr.vendor=t."desc"
      WHERE t.household=? AND vr.vendor IS NULL
      GROUP BY t."desc", t.cat ORDER BY SUM(t.amount) DESC LIMIT 25`, [hh]);
    res.json({ vendors });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/wizard/bulk-assign', async (req, res) => {
  const { userId = 'christian', assignments } = req.body;
  if (!assignments || !assignments.length) return res.status(400).json({ error: 'Missing assignments' });
  try {
    const hh = await getHousehold(userId);
    let totalUpdated = 0;
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      for (const { vendor, category, type = 'expense' } of assignments) {
        await client.query(
          `INSERT INTO vendor_rules (household,vendor,category,type) VALUES ($1,$2,$3,$4)
           ON CONFLICT (household, vendor) DO UPDATE SET category = EXCLUDED.category, type = EXCLUDED.type`,
          [hh, vendor, category, type]
        );
        const r = await client.query(
          'UPDATE transactions SET cat=$1, reviewed=TRUE WHERE household=$2 AND "desc"=$3',
          [category, hh, vendor]
        );
        totalUpdated += r.rowCount || 0;
      }
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
    res.json({ success: true, updated: totalUpdated });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/status', async (req, res) => {
  try {
    const household = await getHousehold(req.query.userId || 'christian');
    const items = await all(`SELECT pi.item_id, pi.user_id, pi.institution
      FROM plaid_items pi JOIN users u ON pi.user_id = u.id
      WHERE u.household = ?`, [household]);
    res.json({ connected: items.length > 0, items, household });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/webhook', async (req, res) => {
  const { webhook_type, webhook_code, item_id } = req.body;
  res.json({ received: true });
  if (webhook_type === 'TRANSACTIONS' &&
    ['DEFAULT_UPDATE','INITIAL_UPDATE','HISTORICAL_UPDATE'].includes(webhook_code)) {
    try {
      const item = await get('SELECT * FROM plaid_items WHERE item_id = ?', [item_id]);
      if (item) await fetchAndStorePlaidTransactions(item.access_token, item.user_id, item_id);
    } catch (err) { console.error('Webhook error:', err.message); }
  }
});

// ─── BUDGETS ──────────────────────────────────────────────────────
// Budgets now live in the categories table as budget_amount
app.get('/api/budgets', async (req, res) => {
  try {
    const hh = await getHousehold(req.query.userId || 'christian');
    const rows = await all('SELECT id as category, budget_amount as amount FROM categories WHERE household = ? AND budget_amount > 0 AND is_active = TRUE ORDER BY sort_order', [hh]);
    res.json({ budgets: rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/budgets', async (req, res) => {
  const { userId = 'christian', category, amount } = req.body;
  if (!category || amount == null) return res.status(400).json({ error: 'Missing category or amount' });
  try {
    const hh = await getHousehold(userId);
    // Upsert category with budget amount
    await pool.query(
      `INSERT INTO categories (id, household, budget_amount) VALUES ($1, $2, $3)
       ON CONFLICT (id, household) DO UPDATE SET budget_amount = EXCLUDED.budget_amount`,
      [category, hh, parseFloat(amount)]
    );
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/budgets', async (req, res) => {
  const { userId = 'christian', category } = req.body;
  if (!category) return res.status(400).json({ error: 'Missing category' });
  try {
    const hh = await getHousehold(userId);
    await run('UPDATE categories SET budget_amount = 0 WHERE id = ? AND household = ?', [category, hh]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── VENDOR SPENDING ─────────────────────────────────────────────
app.get('/api/spending/by-vendor', async (req, res) => {
  const { userId = 'christian', startDate, endDate } = req.query;
  try {
    const hh = await getHousehold(userId);
    const rows = await all(`SELECT "desc" as vendor, SUM(amount) as total, COUNT(*) as count
      FROM transactions
      WHERE household = ? AND type = 'expense' AND date >= ? AND date <= ?
      GROUP BY "desc" ORDER BY SUM(amount) DESC LIMIT 20`, [hh, startDate, endDate]);
    res.json({ vendors: rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── AI CHAT ─────────────────────────────────────────────────────
const anthropic = process.env.ANTHROPIC_API_KEY ? new Anthropic() : null;

// ─── AI AGENT TOOLS ─────────────────────────────────────────────
const agentTools = [
  { name: 'change_vendor_category', description: 'Change the category for ALL transactions from a specific vendor. This creates a vendor rule so future transactions are also categorized.', input_schema: { type: 'object', properties: { vendor: { type: 'string', description: 'Exact vendor name as it appears in transactions' }, category: { type: 'string', description: 'Target category name (e.g., Food, Shopping, Housing)' } }, required: ['vendor', 'category'] } },
  { name: 'set_budget', description: 'Set or update the monthly budget amount for a category', input_schema: { type: 'object', properties: { category: { type: 'string', description: 'Category name' }, amount: { type: 'number', description: 'Monthly budget amount in dollars' } }, required: ['category', 'amount'] } },
  { name: 'create_category', description: 'Create a new spending category', input_schema: { type: 'object', properties: { name: { type: 'string', description: 'New category name' }, icon: { type: 'string', description: 'Emoji icon for the category' }, budget: { type: 'number', description: 'Optional monthly budget amount' } }, required: ['name'] } },
  { name: 'get_vendor_transactions', description: 'Look up all transactions from a specific vendor', input_schema: { type: 'object', properties: { vendor: { type: 'string', description: 'Vendor name to search for (partial match supported)' } }, required: ['vendor'] } },
];

async function executeAgentTool(toolName, toolInput, hh, userId) {
  switch (toolName) {
    case 'change_vendor_category': {
      const { vendor, category } = toolInput;
      await pool.query(`INSERT INTO vendor_rules (household, vendor, category, type, updated_at) VALUES ($1,$2,$3,'expense',NOW()) ON CONFLICT (household, vendor) DO UPDATE SET category=EXCLUDED.category, updated_at=NOW()`, [hh, vendor, category]);
      const r = await pool.query('UPDATE transactions SET cat=$1, reviewed=TRUE WHERE household=$2 AND "desc"=$3', [category, hh, vendor]);
      await pool.query('INSERT INTO categories (id, household) VALUES ($1,$2) ON CONFLICT DO NOTHING', [category, hh]);
      return `Changed ${r.rowCount} transactions from "${vendor}" to category "${category}". Future transactions will also be categorized automatically.`;
    }
    case 'set_budget': {
      const { category, amount } = toolInput;
      await pool.query(`INSERT INTO categories (id, household, budget_amount) VALUES ($1,$2,$3) ON CONFLICT (id, household) DO UPDATE SET budget_amount=EXCLUDED.budget_amount`, [category, hh, amount]);
      return `Set monthly budget for "${category}" to $${amount.toFixed(2)}.`;
    }
    case 'create_category': {
      const { name, icon, budget } = toolInput;
      await pool.query(`INSERT INTO categories (id, household, icon, budget_amount) VALUES ($1,$2,$3,$4) ON CONFLICT (id, household) DO UPDATE SET icon=COALESCE(EXCLUDED.icon, categories.icon), budget_amount=COALESCE(EXCLUDED.budget_amount, categories.budget_amount)`, [name, hh, icon || '📌', budget || 0]);
      return `Created category "${name}"${icon ? ' with icon ' + icon : ''}${budget ? ' and $' + budget + '/month budget' : ''}.`;
    }
    case 'get_vendor_transactions': {
      const { vendor } = toolInput;
      const txs = await all(`SELECT "desc", amount, type, cat, date FROM transactions WHERE household=$1 AND "desc" ILIKE $2 ORDER BY date DESC LIMIT 20`, [hh, `%${vendor}%`]);
      if (!txs.length) return `No transactions found matching "${vendor}".`;
      return txs.map(t => `${t.date} | ${t.type} | ${t.cat} | ${t.desc} | $${parseFloat(t.amount).toFixed(2)}`).join('\n');
    }
    default: return 'Unknown tool';
  }
}

app.post('/api/chat', async (req, res) => {
  if (!anthropic) return res.status(500).json({ error: 'ANTHROPIC_API_KEY not configured' });
  const { userId = 'christian', message, history = [], confirmAction } = req.body;
  if (!message && !confirmAction) return res.status(400).json({ error: 'Missing message' });

  try {
    const hh = await getHousehold(userId);

    // If confirming a pending action, execute it
    if (confirmAction) {
      const result = await executeAgentTool(confirmAction.tool, confirmAction.input, hh, userId);
      return res.json({ reply: `✅ Done! ${result}`, actionExecuted: true });
    }

    const now = new Date();
    const monthStart = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-01`;
    const lastDay = new Date(now.getFullYear(), now.getMonth()+1, 0).getDate();
    const monthEnd = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-${String(lastDay).padStart(2,'0')}`;

    const monthTxs = await all(`SELECT "desc", amount, type, cat, date FROM transactions WHERE household = ? AND date >= ? AND date <= ? ORDER BY date DESC`, [hh, monthStart, monthEnd]);
    const income = monthTxs.filter(t=>t.type==='income').reduce((a,t)=>a+parseFloat(t.amount),0);
    const expenses = monthTxs.filter(t=>t.type==='expense').reduce((a,t)=>a+parseFloat(t.amount),0);
    const catSpend = {};
    monthTxs.filter(t=>t.type==='expense').forEach(t=>{ catSpend[t.cat]=(catSpend[t.cat]||0)+parseFloat(t.amount); });
    const vendorSpend = {};
    monthTxs.filter(t=>t.type==='expense').forEach(t=>{ vendorSpend[t.desc]=(vendorSpend[t.desc]||0)+parseFloat(t.amount); });
    const topVendors = Object.entries(vendorSpend).sort((a,b)=>b[1]-a[1]).slice(0,15);
    const budgetRows = await all('SELECT id as category, budget_amount as amount FROM categories WHERE household = ? AND budget_amount > 0', [hh]);
    const categories = await all('SELECT id, icon, budget_amount FROM categories WHERE household = ? AND is_active = TRUE', [hh]);
    const accts = await all(`SELECT a.name, a.type, a.subtype, a.balance FROM accounts a JOIN users u ON a.user_id = u.id WHERE u.household = ?`, [hh]);

    const context = `## Financial Data for ${now.toLocaleString('default',{month:'long',year:'numeric'})}
### Summary
- Income: $${income.toFixed(2)} | Expenses: $${expenses.toFixed(2)} | Net: $${(income-expenses).toFixed(2)}
- Savings rate: ${income>0?((income-expenses)/income*100).toFixed(0):'0'}% | Transactions: ${monthTxs.length}
### Spending by Category
${Object.entries(catSpend).sort((a,b)=>b[1]-a[1]).map(([c,a])=>`- ${c}: $${a.toFixed(2)}`).join('\n')}
### Budgets
${budgetRows.map(b=>{const spent=catSpend[b.category]||0;return `- ${b.category}: $${spent.toFixed(2)}/$${parseFloat(b.amount).toFixed(2)} (${(spent/parseFloat(b.amount)*100).toFixed(0)}%)`;}).join('\n')}
### Available Categories
${categories.map(c=>`${c.icon||'📌'} ${c.id}`).join(', ')}
### Top 15 Vendors
${topVendors.map(([v,a])=>`- ${v}: $${a.toFixed(2)}`).join('\n')}
### Accounts
${accts.length?accts.map(a=>`- ${a.name} (${a.type}): $${(parseFloat(a.balance)||0).toFixed(2)}`).join('\n'):'No linked accounts'}
### Recent Transactions (last 20)
${monthTxs.slice(0,20).map(t=>`- ${t.date} | ${t.type} | ${t.cat} | ${t.desc} | $${parseFloat(t.amount).toFixed(2)}`).join('\n')}`;

    const systemPrompt = `You are Obsidian, an AI finance assistant for the Spenziero household. You have real financial data and can take actions on their behalf.

${context}

CAPABILITIES:
- Answer questions about spending, income, budgets, and trends
- Change vendor categories (e.g., "move Amazon to Shopping")
- Set or update budgets (e.g., "set food budget to $1000")
- Create new categories
- Look up vendor transaction history

RULES:
- When the user asks you to make a change, USE THE APPROPRIATE TOOL. Don't just describe what to do.
- Be concise — under 150 words unless detail is needed
- Reference specific numbers from their data
- Use $ formatting for amounts
- Be conversational and helpful`;

    const messages = [...history.slice(-10), { role: 'user', content: message }];

    const response = await anthropic.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 1024,
      system: systemPrompt,
      messages,
      tools: agentTools,
    });

    // Check if Claude wants to use a tool
    const toolUse = response.content.find(c => c.type === 'tool_use');
    if (toolUse) {
      // Return the proposed action for user confirmation
      const toolDescriptions = {
        change_vendor_category: `Change all "${toolUse.input.vendor}" transactions to category "${toolUse.input.category}"`,
        set_budget: `Set ${toolUse.input.category} budget to $${toolUse.input.amount}`,
        create_category: `Create new category "${toolUse.input.name}"`,
        get_vendor_transactions: `Look up transactions for "${toolUse.input.vendor}"`,
      };

      // For read-only tools, execute immediately
      if (toolUse.name === 'get_vendor_transactions') {
        const result = await executeAgentTool(toolUse.name, toolUse.input, hh, userId);
        // Send result back to Claude for a human-friendly response
        const followUp = await anthropic.messages.create({
          model: 'claude-sonnet-4-6', max_tokens: 1024, system: systemPrompt,
          messages: [...messages, { role: 'assistant', content: response.content }, { role: 'user', content: [{ type: 'tool_result', tool_use_id: toolUse.id, content: result }] }],
        });
        return res.json({ reply: followUp.content.find(c=>c.type==='text')?.text || result });
      }

      // For write tools, ask for confirmation
      const textContent = response.content.find(c => c.type === 'text');
      return res.json({
        reply: (textContent?.text || '') + `\n\n⚡ **Proposed action:** ${toolDescriptions[toolUse.name] || toolUse.name}`,
        pendingAction: { tool: toolUse.name, input: toolUse.input, description: toolDescriptions[toolUse.name] }
      });
    }

    // No tool use — just a text response
    const textContent = response.content.find(c => c.type === 'text');
    res.json({ reply: textContent?.text || 'I couldn\'t generate a response.' });
  } catch (e) {
    console.error('Chat error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─── MAINTENANCE ────────────────────────────────────────────────
app.post('/api/maintenance/dedup', async (req, res) => {
  const { userId = 'christian' } = req.body;
  try {
    const hh = await getHousehold(userId);
    let removed = 0;

    // 1. Dedup accounts by name+mask (keep newest)
    const acctDupes = await pool.query(`
      DELETE FROM accounts WHERE account_id IN (
        SELECT account_id FROM (
          SELECT account_id, ROW_NUMBER() OVER (PARTITION BY name, mask, household ORDER BY updated_at DESC) as rn
          FROM accounts WHERE household = $1
        ) ranked WHERE rn > 1
      ) RETURNING account_id`, [hh]);
    removed += acctDupes.rowCount;

    // 2. Dedup pending/posted transactions
    const pendDupes = await pool.query(`
      DELETE FROM transactions WHERE id IN (
        SELECT p.id FROM transactions p
        INNER JOIN transactions posted ON p."desc" = posted."desc" AND p.amount = posted.amount
          AND p.date = posted.date AND p.household = posted.household
        WHERE p.household = $1 AND p.pending = TRUE AND posted.pending = FALSE AND p.id != posted.id
      ) RETURNING id`, [hh]);
    removed += pendDupes.rowCount;

    // 3. Dedup cross-account duplicates (same desc+amount+date, keep oldest)
    const crossDupes = await pool.query(`
      DELETE FROM transactions WHERE id IN (
        SELECT id FROM (
          SELECT id, ROW_NUMBER() OVER (PARTITION BY household, "desc", amount, date ORDER BY created_at ASC) as rn
          FROM transactions WHERE household = $1
        ) ranked WHERE rn > 1
      ) RETURNING id`, [hh]);
    removed += crossDupes.rowCount;

    res.json({ success: true, removed, accounts_deduped: acctDupes.rowCount, pending_deduped: pendDupes.rowCount, cross_deduped: crossDupes.rowCount });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── VENDOR SUMMARY + BULK RULES ────────────────────────────────
app.get('/api/vendor-summary', async (req, res) => {
  const { userId = 'christian' } = req.query;
  try {
    const hh = await getHousehold(userId);
    const vendors = await all(`
      SELECT t."desc" as vendor, COUNT(*) as tx_count, SUM(t.amount) as total_spend,
        MIN(t.date) as first_seen, MAX(t.date) as last_seen, t.cat as plaid_category,
        t.type as inferred_type, vr.category as rule_category, vr.type as rule_type,
        CASE WHEN vr.vendor IS NOT NULL THEN 1 ELSE 0 END as has_rule
      FROM transactions t
      LEFT JOIN vendor_rules vr ON vr.vendor = t."desc" AND vr.household = t.household
      WHERE t.household = ? GROUP BY t."desc", t.cat, t.type, vr.category, vr.type, vr.vendor ORDER BY SUM(t.amount) DESC`, [hh]);
    const withRules = vendors.filter(v => v.has_rule);
    res.json({
      vendors,
      stats: {
        total_vendors: vendors.length, categorized: withRules.length,
        needs_review: vendors.length - withRules.length,
        total_spend: vendors.reduce((a, v) => a + parseFloat(v.total_spend), 0),
        coverage_pct: vendors.length > 0 ? Math.round(withRules.length / vendors.length * 100) : 0
      }
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/vendor-rules/bulk', async (req, res) => {
  const { userId = 'christian', assignments } = req.body;
  if (!assignments || !assignments.length) return res.status(400).json({ error: 'No assignments' });
  try {
    const hh = await getHousehold(userId);
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      let totalUpdated = 0;
      for (const a of assignments) {
        await client.query(
          `INSERT INTO vendor_rules (household, vendor, category, type, updated_at) VALUES ($1, $2, $3, $4, NOW())
           ON CONFLICT (household, vendor) DO UPDATE SET category = EXCLUDED.category, type = EXCLUDED.type, updated_at = NOW()`,
          [hh, a.vendor, a.category, a.type || 'expense']
        );
        const result = await client.query(
          'UPDATE transactions SET cat = $1, reviewed = TRUE WHERE household = $2 AND "desc" = $3',
          [a.category, hh, a.vendor]
        );
        totalUpdated += result.rowCount;
        await client.query(
          'INSERT INTO categories (id, household) VALUES ($1, $2) ON CONFLICT DO NOTHING',
          [a.category, hh]
        );
      }
      await client.query('COMMIT');
      res.json({ success: true, rules_created: assignments.length, transactions_updated: totalUpdated });
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── HEALTH SCORE ───────────────────────────────────────────────
app.get('/api/health-score', async (req, res) => {
  const { userId = 'christian' } = req.query;
  try {
    const hh = await getHousehold(userId);
    const now = new Date();
    const monthStart = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-01`;
    const monthEnd = new Date(now.getFullYear(), now.getMonth()+1, 0).toISOString().split('T')[0];
    const mtxs = await all('SELECT * FROM transactions WHERE household = ? AND date >= ? AND date <= ?', [hh, monthStart, monthEnd]);
    const income = mtxs.filter(t => t.type === 'income').reduce((a,t) => a + parseFloat(t.amount), 0);
    const expenses = mtxs.filter(t => t.type === 'expense').reduce((a,t) => a + parseFloat(t.amount), 0);
    const savingsRate = income > 0 ? (income - expenses) / income : 0;
    const savingsScore = Math.min(30, Math.max(0, Math.round(savingsRate * 150)));
    const budgetRows = await all('SELECT category, amount FROM budgets WHERE household = ?', [hh]);
    let budgetScore = 25;
    for (const b of budgetRows) {
      const spent = mtxs.filter(t => t.type === 'expense' && t.cat === b.category).reduce((a,t) => a + parseFloat(t.amount), 0);
      if (spent > parseFloat(b.amount)) budgetScore -= Math.min(5, Math.round((spent - parseFloat(b.amount)) / parseFloat(b.amount) * 10));
    }
    budgetScore = Math.max(0, budgetScore);
    const totalVendors = parseInt((await get('SELECT COUNT(DISTINCT "desc") as cnt FROM transactions WHERE household = ?', [hh]))?.cnt) || 1;
    const ruledVendors = parseInt((await get('SELECT COUNT(*) as cnt FROM vendor_rules WHERE household = ?', [hh]))?.cnt) || 0;
    const coverageScore = Math.round((ruledVendors / totalVendors) * 15);
    const recurringTotal = parseFloat((await get('SELECT SUM(expected_amount) as total FROM recurring_rules WHERE household = ? AND is_active = TRUE', [hh]))?.total) || 0;
    const recurringPct = income > 0 ? recurringTotal / income : 0;
    const recurringScore = recurringPct < 0.5 ? 15 : Math.max(0, 15 - Math.round((recurringPct - 0.5) * 30));
    const daySpends = {};
    mtxs.filter(t => t.type === 'expense').forEach(t => { daySpends[t.date] = (daySpends[t.date] || 0) + parseFloat(t.amount); });
    const dayValues = Object.values(daySpends);
    const avgDay = dayValues.length > 0 ? dayValues.reduce((a,b) => a+b, 0) / dayValues.length : 0;
    const variance = dayValues.length > 0 ? dayValues.reduce((a,v) => a + Math.pow(v - avgDay, 2), 0) / dayValues.length : 0;
    const cv = avgDay > 0 ? Math.sqrt(variance) / avgDay : 0;
    const consistencyScore = Math.max(0, Math.round(15 * (1 - Math.min(cv, 1.5) / 1.5)));
    const total = Math.min(100, Math.max(0, savingsScore + budgetScore + coverageScore + recurringScore + consistencyScore));
    const color = total >= 80 ? '#34D399' : total >= 60 ? '#F5C518' : total >= 40 ? '#FB923C' : '#F87171';
    res.json({ score: total, color, breakdown: { savings: { score: savingsScore, max: 30, rate: savingsRate }, budget: { score: budgetScore, max: 25 }, coverage: { score: coverageScore, max: 15, pct: ruledVendors / totalVendors }, recurring: { score: recurringScore, max: 15, pct: recurringPct }, consistency: { score: consistencyScore, max: 15, cv } } });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── STREAK ─────────────────────────────────────────────────────
app.post('/api/streak/check-in', async (req, res) => {
  const { userId = 'christian' } = req.query;
  try {
    const user = await get('SELECT * FROM users WHERE id = ?', [userId]);
    if (!user) return res.status(404).json({ error: 'User not found' });
    const today = new Date().toISOString().split('T')[0];
    const yesterday = new Date(Date.now() - 86400000).toISOString().split('T')[0];
    let streak = user.streak_count || 0;
    let best = user.streak_best || 0;
    if (user.last_active_date === today) { /* already checked in */ }
    else if (user.last_active_date === yesterday) { streak++; }
    else if (!user.last_active_date) { streak = 1; }
    else { streak = 1; }
    if (streak > best) best = streak;
    await run('UPDATE users SET streak_count = ?, streak_best = ?, last_active_date = ? WHERE id = ?', [streak, best, today, userId]);
    res.json({ streak, best, is_new_day: user.last_active_date !== today, milestone: [7,30,100,365].includes(streak) });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── CHALLENGES ─────────────────────────────────────────────────
app.get('/api/challenges', async (req, res) => {
  const { userId = 'christian' } = req.query;
  try {
    const hh = await getHousehold(userId);
    const now = new Date();
    const currentMonth = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
    let challenges = await all('SELECT * FROM challenges WHERE household = ? AND month = ?', [hh, currentMonth]);
    if (!challenges.length) {
      const prev = new Date(now.getFullYear(), now.getMonth() - 1, 1);
      const prevStart = prev.toISOString().split('T')[0];
      const prevEnd = new Date(prev.getFullYear(), prev.getMonth()+1, 0).toISOString().split('T')[0];
      const prevTxs = await all('SELECT * FROM transactions WHERE household = ? AND date >= ? AND date <= ?', [hh, prevStart, prevEnd]);
      const prevExpenses = prevTxs.filter(t => t.type === 'expense');
      const prevTotal = prevExpenses.reduce((a,t) => a + parseFloat(t.amount), 0);
      const prevIncome = prevTxs.filter(t => t.type === 'income').reduce((a,t) => a + parseFloat(t.amount), 0);
      const daysInPrev = new Date(prev.getFullYear(), prev.getMonth()+1, 0).getDate();
      const prevDailyAvg = daysInPrev > 0 ? prevTotal / daysInPrev : 0;
      const uuid = () => 'ch_' + Date.now() + '_' + Math.random().toString(36).slice(2,8);
      const newChallenges = [];
      if (prevDailyAvg > 0) newChallenges.push({ id: uuid(), household: hh, month: currentMonth, title: `Daily avg under $${Math.round(prevDailyAvg)}`, description: `Beat last month's $${Math.round(prevDailyAvg)}/day average`, target_value: prevDailyAvg, challenge_type: 'daily_average' });
      if (prevIncome > 0) { const prevRate = (prevIncome - prevTotal) / prevIncome; const target = Math.min(0.30, prevRate + 0.05); newChallenges.push({ id: uuid(), household: hh, month: currentMonth, title: `Save ${Math.round(target * 100)}% of income`, description: `Improve from ${Math.round(prevRate * 100)}% last month`, target_value: target, challenge_type: 'savings_rate' }); }
      newChallenges.push({ id: uuid(), household: hh, month: currentMonth, title: 'Stay organized', description: '100% vendor coverage', target_value: 100, challenge_type: 'coverage' });
      for (const c of newChallenges) {
        await pool.query(
          'INSERT INTO challenges (id, household, month, title, description, target_value, challenge_type, category) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)',
          [c.id, c.household, c.month, c.title, c.description, c.target_value, c.challenge_type, c.category || null]
        );
      }
      challenges = newChallenges;
    }
    const monthStart = `${currentMonth}-01`;
    const monthEnd = new Date(now.getFullYear(), now.getMonth()+1, 0).toISOString().split('T')[0];
    const currTxs = await all('SELECT * FROM transactions WHERE household = ? AND date >= ? AND date <= ?', [hh, monthStart, monthEnd]);
    const currExpenses = currTxs.filter(t => t.type === 'expense');
    const currIncome = currTxs.filter(t => t.type === 'income').reduce((a,t) => a + parseFloat(t.amount), 0);
    const currTotal = currExpenses.reduce((a,t) => a + parseFloat(t.amount), 0);
    const daysElapsed = now.getDate();
    const updatedChallenges = [];
    for (const c of challenges) {
      let current = 0, progress = 0;
      switch(c.challenge_type) {
        case 'daily_average': current = daysElapsed > 0 ? currTotal / daysElapsed : 0; progress = c.target_value > 0 ? Math.min(100, Math.max(0, (1 - current / parseFloat(c.target_value)) * 100)) : 0; break;
        case 'savings_rate': current = currIncome > 0 ? (currIncome - currTotal) / currIncome : 0; progress = c.target_value > 0 ? Math.min(100, (current / parseFloat(c.target_value)) * 100) : 0; break;
        case 'coverage': {
          const tv = parseInt((await get('SELECT COUNT(DISTINCT "desc") as cnt FROM transactions WHERE household = ?', [hh]))?.cnt) || 1;
          const rv = parseInt((await get('SELECT COUNT(*) as cnt FROM vendor_rules WHERE household = ?', [hh]))?.cnt) || 0;
          current = Math.round(rv / tv * 100);
          progress = current;
          break;
        }
      }
      updatedChallenges.push({ ...c, current_value: current, progress: Math.max(0, progress), is_completed: progress >= 100 && daysElapsed >= 28 });
    }
    res.json({ challenges: updatedChallenges });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── WEEKLY RECAP ───────────────────────────────────────────────
app.get('/api/weekly-recap', async (req, res) => {
  const { userId = 'christian' } = req.query;
  try {
    const hh = await getHousehold(userId);
    const now = new Date();
    const weekAgo = new Date(now); weekAgo.setDate(weekAgo.getDate() - 7);
    const prevWeekStart = new Date(weekAgo); prevWeekStart.setDate(prevWeekStart.getDate() - 7);
    const thisWeek = await all('SELECT * FROM transactions WHERE household = ? AND date >= ? AND date <= ?', [hh, weekAgo.toISOString().split('T')[0], now.toISOString().split('T')[0]]);
    const prevWeek = await all('SELECT * FROM transactions WHERE household = ? AND date >= ? AND date <= ?', [hh, prevWeekStart.toISOString().split('T')[0], weekAgo.toISOString().split('T')[0]]);
    const thisExp = thisWeek.filter(t => t.type === 'expense');
    const prevExp = prevWeek.filter(t => t.type === 'expense');
    const thisTotal = thisExp.reduce((a,t) => a + parseFloat(t.amount), 0);
    const prevTotal = prevExp.reduce((a,t) => a + parseFloat(t.amount), 0);
    const change = prevTotal > 0 ? ((thisTotal - prevTotal) / prevTotal * 100) : 0;
    const biggest = thisExp.sort((a,b) => parseFloat(b.amount) - parseFloat(a.amount))[0];
    const vendorCounts = {}; thisExp.forEach(t => { vendorCounts[t.desc] = (vendorCounts[t.desc]||0) + 1; });
    const topVendor = Object.entries(vendorCounts).sort((a,b) => b[1] - a[1])[0];
    res.json({ period: { start: weekAgo.toISOString().split('T')[0], end: now.toISOString().split('T')[0] }, total_spent: thisTotal, prev_total_spent: prevTotal, change_pct: change, transaction_count: thisWeek.length, biggest_purchase: biggest ? { vendor: biggest.desc, amount: parseFloat(biggest.amount), date: biggest.date } : null, most_frequent_vendor: topVendor ? { vendor: topVendor[0], count: topVendor[1] } : null });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Serve frontend
const frontendPath = path.join(__dirname, '../frontend');
app.use(express.static(frontendPath));
app.get('*', (req, res) => res.sendFile(path.join(frontendPath, 'index.html')));

// ─── START ────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3001;

async function start() {
  await initDB();
  app.listen(PORT, () => {
    console.log(`\nObsidian running on http://localhost:${PORT}`);
    console.log(`  Plaid env:  ${process.env.PLAID_ENV || 'sandbox'}`);
    console.log(`  Client ID:  ${process.env.PLAID_CLIENT_ID ? 'set' : 'MISSING'}`);
    console.log(`  Webhook:    ${process.env.WEBHOOK_URL || 'not set'}\n`);
  });
}

start();
