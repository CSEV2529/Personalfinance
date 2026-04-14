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
const jwt = require('jsonwebtoken');
const { createClient } = require('@supabase/supabase-js');

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

  // ── AUTH TABLES (v10) ──
  await pool.query(`CREATE TABLE IF NOT EXISTS households (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS household_members (
    household_id UUID NOT NULL REFERENCES households(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'member',
    joined_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (household_id, user_id)
  )`);
  try { await pool.query('CREATE INDEX IF NOT EXISTS idx_hh_members_user ON household_members(user_id)'); } catch(e) {}
  await pool.query(`CREATE TABLE IF NOT EXISTS user_profiles (
    id TEXT PRIMARY KEY,
    display_name TEXT,
    avatar_color TEXT DEFAULT '#E8A828',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
  )`);

  // ── SEED CATEGORIES ──
  const defaultCats = {
    Housing: { icon: '🏠', color: '#F59E0B' }, Food: { icon: '🍽️', color: '#22C55E' },
    Transport: { icon: '🚗', color: '#3B82F6' }, Health: { icon: '💊', color: '#EC4899' },
    Entertainment: { icon: '🎬', color: '#8B5CF6' }, Shopping: { icon: '🛍️', color: '#F97316' },
    Utilities: { icon: '⚡', color: '#06B6D4' }, Income: { icon: '💰', color: '#10B981' },
    Transfer: { icon: '🔁', color: '#6366F1' }, Other: { icon: '📌', color: '#64748B' },
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

  // ── FIX MISCLASSIFIED TRANSFERS (v8) ──
  await run(`UPDATE transactions SET cat='Transfer', type='transfer'
    WHERE reviewed=FALSE AND cat != 'Transfer' AND (
      UPPER("desc") LIKE 'XFER %' OR UPPER("desc") LIKE 'EXT XFER%'
      OR UPPER("desc") LIKE '%OFFICIAL CHECK%'
      OR (UPPER("desc") LIKE '%CREDIT CARD%' AND UPPER("desc") LIKE '%PAYMENT%')
      OR UPPER("desc") LIKE '%AUTOMATIC PAYMENT%'
      OR UPPER("desc") LIKE '%PAYMENT - THANK%'
    )`);

  // ── CLEANUP: Remove stale pending txns that have a posted match within 3 days ──
  // Name match omitted — card holds often have different names than posted charges
  const stalePending = await pool.query(`
    DELETE FROM transactions WHERE id IN (
      SELECT DISTINCT p.id FROM transactions p
      INNER JOIN transactions posted ON p.amount = posted.amount
        AND p.account_id = posted.account_id AND p.household = posted.household
        AND ABS(posted.date::date - p.date::date) <= 3
      WHERE p.pending = TRUE AND posted.pending = FALSE AND p.id != posted.id
    ) RETURNING id`);
  if (stalePending.rowCount) console.log(`  DB: Cleaned up ${stalePending.rowCount} stale pending duplicates`);

  await pool.query(`INSERT INTO users (id, name, household) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, ['christian', 'Christian', 'spenziero']);
  await pool.query(`INSERT INTO users (id, name, household) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, ['wife', 'Marisol', 'spenziero']);
  const defaultBudgets = {Housing:2000,Food:800,Transport:400,Health:300,Entertainment:200,Shopping:400,Utilities:250};
  for (const [cat, amt] of Object.entries(defaultBudgets)) {
    await pool.query(`INSERT INTO categories (id, household, budget_amount) VALUES ($1, $2, $3)
      ON CONFLICT (id, household) DO UPDATE SET budget_amount = COALESCE(NULLIF(categories.budget_amount, 0), EXCLUDED.budget_amount)`,
      [cat, 'spenziero', amt]);
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

// ─── SUPABASE AUTH ──────────────────────────────────────────────
console.log('  Auth config:', {
  supabase_url: !!process.env.SUPABASE_URL,
  anon_key: !!process.env.SUPABASE_ANON_KEY,
  service_role: !!process.env.SUPABASE_SERVICE_ROLE_KEY,
  jwt_secret: !!process.env.SUPABASE_JWT_SECRET,
  jwt_secret_length: process.env.SUPABASE_JWT_SECRET?.length || 0
});
const supabaseAdmin = (process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY)
  ? createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY)
  : null;

// Legacy fallback — used during migration period when no JWT is present
async function getHousehold(userId) {
  // First check new household_members table
  const member = await get('SELECT household_id FROM household_members WHERE user_id = $1 LIMIT 1', [userId]);
  if (member) return member.household_id;
  // Fall back to legacy users table
  const user = await get('SELECT household FROM users WHERE id = ?', [userId]);
  return user?.household || 'default';
}

// Auth middleware — supports JWT auth with legacy userId fallback
async function requireAuth(req, res, next) {
  const authHeader = req.headers.authorization;

  // Try JWT auth first — use Supabase to verify the token
  if (authHeader && authHeader.startsWith('Bearer ') && supabaseAdmin) {
    try {
      const token = authHeader.slice(7);
      const { data: { user }, error } = await supabaseAdmin.auth.getUser(token);
      if (error || !user) return res.status(401).json({ error: error?.message || 'Invalid token' });

      req.user = { id: user.id, email: user.email };

      const hh = await get('SELECT household_id FROM household_members WHERE user_id = $1 LIMIT 1', [user.id]);
      req.household = hh?.household_id || null;
      req.needsOnboarding = !hh;
      return next();
    } catch (e) {
      console.error('Auth verification failed:', e.message);
      return res.status(401).json({ error: 'Authentication failed: ' + e.message });
    }
  }

  // Legacy fallback — accept userId from query/body
  const userId = req.query.userId || req.body?.userId || 'christian';
  req.user = { id: userId, email: null };
  req.household = await getHousehold(userId);
  next();
}

function mapCategory(plaidCat) {
  if (!plaidCat) return 'Other';
  const c = plaidCat.toUpperCase();
  // Transfer detection first — highest priority
  if (c.includes('TRANSFER') || c.includes('WIRE') || c.includes('ACH'))          return 'Transfer';
  if (c.includes('RENT') || c.includes('MORTGAGE') || c.includes('HOME'))         return 'Housing';
  if (c.includes('FOOD') || c.includes('RESTAURANT') || c.includes('GROCERY'))    return 'Food';
  if (c.includes('TRANSPORT') || c.includes('TRAVEL') || c.includes('GAS') || c.includes('AUTO')) return 'Transport';
  if (c.includes('MEDICAL') || c.includes('HEALTH') || c.includes('PHARMACY'))    return 'Health';
  if (c.includes('ENTERTAINMENT') || c.includes('RECREATION'))                    return 'Entertainment';
  if (c.includes('SHOPS') || c.includes('SHOPPING') || c.includes('MERCHANDISE')) return 'Shopping';
  if (c.includes('UTILITIES') || c.includes('TELECOM') || c.includes('INTERNET')) return 'Utilities';
  if (c.includes('PAYROLL') || c.includes('INCOME') || c.includes('DEPOSIT'))     return 'Income';
  return 'Other';
}

// Detect transfers from transaction name (for cases Plaid doesn't catch)
function isTransferByName(name) {
  if (!name) return false;
  const desc = name.toUpperCase();
  return (
    desc.startsWith('XFER ') || desc.startsWith('EXT XFER') ||
    desc.includes('TRANSFER') || desc.includes('AUTOPAY') ||
    (desc.includes('CREDIT CARD') && desc.includes('PAYMENT')) ||
    desc.includes('CD DEPOSIT') ||
    (desc.includes('SAVINGS') && desc.includes('WITHDRAWAL')) ||
    desc.includes('AUTOMATIC PAYMENT') ||
    desc.includes('PAYMENT - THANK') ||
    desc.startsWith('OFFICIAL CHECK')
  );
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

  // ── LOAD VENDOR RULES (case-insensitive lookup) ──
  const vendorRules = {};
  const vendorRulesUpper = {};
  (await all('SELECT vendor, category, type FROM vendor_rules WHERE household = ?', [household]))
    .forEach(r => {
      vendorRules[r.vendor] = { cat: r.category, type: r.type };
      vendorRulesUpper[r.vendor.toUpperCase().trim()] = { cat: r.category, type: r.type };
    });

  // ── UPSERT WITH THREE-TIER CATEGORIZATION (inside a transaction) ──
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    for (const t of allTransactions) {
      // Case-insensitive vendor rule lookup
      const rule = vendorRules[t.name] || vendorRulesUpper[(t.name || '').toUpperCase().trim()];
      let cat, type, reviewed;

      // STEP 1: Check if this transaction was manually reviewed — ALWAYS wins
      const existingResult = await client.query(
        'SELECT cat, type, reviewed, notes, status, is_recurring FROM transactions WHERE id = $1',
        [t.transaction_id]
      );
      const existing = existingResult.rows[0];

      if (existing && existing.reviewed) {
        // MANUAL REVIEW WINS — preserve both category AND type
        cat = existing.cat;
        type = existing.type;
        reviewed = true;
      } else if (rule) {
        // VENDOR RULE — apply category from rule
        cat = rule.cat;
        if (cat === 'Transfer') {
          type = 'transfer';
        } else if (rule.type && rule.type !== 'expense') {
          // Rule has explicit non-default type — respect it
          type = rule.type;
        } else {
          type = t.amount > 0 ? 'expense' : 'income';
        }
        reviewed = true;
      } else {
        // TIER 3: Plaid categorization + name-based transfer detection
        cat = mapCategory(t.personal_finance_category?.primary || t.category?.[0]);
        if (cat !== 'Transfer' && isTransferByName(t.name)) {
          cat = 'Transfer';
        }
        type = cat === 'Transfer' ? 'transfer' : (t.amount > 0 ? 'expense' : 'income');
        reviewed = false;
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

    // Remove pending transactions that Plaid explicitly replaced (pending_transaction_id)
    let pendingRemoved = 0;
    for (const t of allTransactions) {
      if (!t.pending && t.pending_transaction_id) {
        const del = await client.query('DELETE FROM transactions WHERE id = $1 AND household = $2 AND pending = TRUE', [t.pending_transaction_id, household]);
        pendingRemoved += del.rowCount;
      }
    }
    if (pendingRemoved) console.log(`  Removed ${pendingRemoved} pending txns via Plaid pending_transaction_id`);

    // Remove pending transactions where a posted version exists (same amount + account within 3 days)
    // Note: name match intentionally omitted — card holds often have different names than posted charges
    const dupesResult = await client.query(`
      SELECT DISTINCT p.id as pending_id FROM transactions p
      INNER JOIN transactions posted ON p.amount = posted.amount
        AND p.account_id = posted.account_id AND p.household = posted.household
        AND ABS(posted.date::date - p.date::date) <= 3
      WHERE p.household = $1 AND p.pending = TRUE AND posted.pending = FALSE AND p.id != posted.id`, [household]);
    const dupes = dupesResult.rows;
    for (const d of dupes) await client.query('DELETE FROM transactions WHERE id = $1', [d.pending_id]);
    if (dupes.length) console.log(`  Removed ${dupes.length} pending duplicates (amount+account+date match)`);

    // Also remove cross-account duplicates (same desc, amount, date, household)
    // Prefer keeping records with user edits (notes, reviewed, unsure, tags, recurring)
    const crossDupes = await client.query(`
      DELETE FROM transactions WHERE id IN (
        SELECT id FROM (
          SELECT id, ROW_NUMBER() OVER (
            PARTITION BY household, "desc", amount, date
            ORDER BY
              (CASE WHEN notes IS NOT NULL AND notes != '' THEN 0 ELSE 1 END),
              (CASE WHEN reviewed = TRUE THEN 0 ELSE 1 END),
              (CASE WHEN status IS NOT NULL AND status != 'confirmed' THEN 0 ELSE 1 END),
              (CASE WHEN is_recurring = TRUE THEN 0 ELSE 1 END),
              created_at ASC
          ) as rn
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

app.post('/api/create_link_token', requireAuth, async (req, res) => {
  try {
    const hh = req.household;
    const linkConfig = {
      user: { client_user_id: req.user.id },
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

app.post('/api/exchange_public_token', requireAuth, async (req, res) => {
  const { public_token, institution } = req.body;
  try {
    // BULLETPROOF: On reconnect, wipe ALL old data for this household
    // Plaid generates new account IDs on every connection, so old IDs become orphans
    const hh = req.household;
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
      [item_id, req.user.id, access_token, institution || 'Unknown']
    );
    console.log(`Bank connected: user=${req.user.id}, item=${item_id}`);
    // Try to fetch transactions, but don't fail if not ready yet
    let result = { count: 0, accounts: 0 };
    try {
      result = await fetchAndStorePlaidTransactions(access_token, req.user.id, item_id);
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

app.get('/api/transactions', requireAuth, async (req, res) => {
  const { days = 60, household: hhParam = 'true', startDate, endDate, vendor, limit } = req.query;
  try {
    const hh = req.household;
    // Quick vendor lookup
    if (vendor) {
      const lim = parseInt(limit) || 20;
      const txs = await all(`SELECT t.*, u.name as user_name FROM transactions t
        LEFT JOIN users u ON t.user_id = u.id
        WHERE t.household = $1 AND t."desc" ILIKE $2
        ORDER BY t.date DESC LIMIT ${lim}`, [hh, `%${vendor}%`]);
      return res.json({ transactions: txs, total: txs.length });
    }
    let txs;
    if (startDate && endDate) {
      if (hhParam === 'true') {
        txs = await all(`SELECT t.*, u.name as user_name FROM transactions t
          LEFT JOIN users u ON t.user_id = u.id
          WHERE t.household = ? AND t.date >= ? AND t.date <= ?
          ORDER BY t.date DESC, t.created_at DESC`, [hh, startDate, endDate]);
      } else {
        txs = await all(`SELECT * FROM transactions WHERE user_id = ? AND date >= ? AND date <= ?
          ORDER BY date DESC, created_at DESC`, [req.user.id, startDate, endDate]);
      }
    } else {
      const cutoff = new Date(); cutoff.setDate(cutoff.getDate() - parseInt(days));
      const cutoffStr = cutoff.toISOString().split('T')[0];
      if (hhParam === 'true') {
        txs = await all(`SELECT t.*, u.name as user_name FROM transactions t
          LEFT JOIN users u ON t.user_id = u.id
          WHERE t.household = ? AND t.date >= ?
          ORDER BY t.date DESC, t.created_at DESC`, [hh, cutoffStr]);
      } else {
        txs = await all(`SELECT * FROM transactions WHERE user_id = ? AND date >= ?
          ORDER BY date DESC, created_at DESC`, [req.user.id, cutoffStr]);
      }
    }
    res.json({ transactions: txs, total: txs.length });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/accounts', requireAuth, async (req, res) => {
  try {
    const hh = req.household;
    const accounts = await all(`SELECT a.*, u.name as user_name FROM accounts a
      LEFT JOIN users u ON a.user_id = u.id
      WHERE a.household = ? ORDER BY a.type, a.name`, [hh]);
    res.json({ accounts });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/sync', requireAuth, async (req, res) => {
  try {
    const household = req.household;
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

app.post('/api/transactions', requireAuth, async (req, res) => {
  const { desc, amount, type, cat, date } = req.body;
  if (!desc || !amount || !type || !cat || !date)
    return res.status(400).json({ error: 'Missing required fields' });
  try {
    const household = req.household;
    const id = 'manual_' + Date.now() + '_' + Math.random().toString(36).slice(2);
    await run(`INSERT INTO transactions (id, household, user_id, "desc", amount, type, cat, date, source)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'manual')`,
      [id, household, req.user.id, desc, parseFloat(amount), type, cat, date]);
    res.json({ success: true, id });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── TRANSACTION DETAIL ─────────────────────────────────────────
// Static routes MUST come before parameterized /:id routes
app.get('/api/transactions/unsure', requireAuth, async (req, res) => {
  try {
    const hh = req.household;
    const txs = await all(`SELECT t.*, u.name as user_name FROM transactions t
      LEFT JOIN users u ON t.user_id=u.id
      WHERE t.household=? AND t.status='unsure' ORDER BY t.date DESC`, [hh]);
    res.json({ transactions: txs });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/transactions/:id/status', requireAuth, async (req, res) => {
  const { status } = req.body;
  if (!status) return res.status(400).json({ error: 'Missing status' });
  try {
    await run('UPDATE transactions SET status=? WHERE id=?', [status, req.params.id]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/transactions/:id', requireAuth, async (req, res) => {
  try {
    const tx = await get(`SELECT t.*, u.name as user_name FROM transactions t
      LEFT JOIN users u ON t.user_id = u.id WHERE t.id = ?`, [req.params.id]);
    if (!tx) return res.status(404).json({ error: 'Transaction not found' });
    const tags = (await all('SELECT tag FROM transaction_tags WHERE transaction_id = ?', [req.params.id])).map(r => r.tag);
    const recurring = tx.recurring_group_id ? await get('SELECT * FROM recurring_rules WHERE id = ?', [tx.recurring_group_id]) : null;
    res.json({ ...tx, tags, recurring });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/transactions/:id', requireAuth, async (req, res) => {
  const { category, type, notes, is_recurring, original_sign, status } = req.body;
  try {
    const tx = await get('SELECT * FROM transactions WHERE id = ?', [req.params.id]);
    if (!tx) return res.status(404).json({ error: 'Transaction not found' });
    const newCat = category || tx.cat;
    const newType = type || tx.type;
    const newNotes = notes !== undefined ? notes : tx.notes;
    const newRecurring = is_recurring !== undefined ? (is_recurring ? true : false) : tx.is_recurring;
    const newSign = original_sign !== undefined ? original_sign : tx.original_sign;
    const newStatus = status || tx.status || 'confirmed';
    // Single transaction update ONLY. Vendor-wide changes go through dedicated endpoints.
    await run('UPDATE transactions SET cat=?, type=?, notes=?, is_recurring=?, original_sign=?, status=?, reviewed=TRUE WHERE id=?',
      [newCat, newType, newNotes, newRecurring, newSign, newStatus, req.params.id]);
    res.json({ success: true, updated: 1 });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Legacy endpoint (frontend compat)
app.put('/api/transactions/:id/category', requireAuth, async (req, res) => {
  const { category, type, original_sign } = req.body;
  if (!category && !type && original_sign === undefined) return res.status(400).json({ error: 'Missing category, type, or sign' });
  try {
    const tx = await get('SELECT * FROM transactions WHERE id = ?', [req.params.id]);
    if (!tx) return res.status(404).json({ error: 'Transaction not found' });
    const newCat = category || tx.cat;
    const newType = type || tx.type;
    const newSign = original_sign !== undefined ? original_sign : tx.original_sign;
    // Single transaction update ONLY. Vendor-wide changes go through dedicated endpoints.
    await run('UPDATE transactions SET cat=?, type=?, original_sign=?, reviewed=TRUE WHERE id=?', [newCat, newType, newSign, req.params.id]);
    res.json({ success: true, updated: 1 });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/transactions/:id/tags', requireAuth, async (req, res) => {
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

app.delete('/api/transactions/:id/tags/:tag', requireAuth, async (req, res) => {
  try {
    await run('DELETE FROM transaction_tags WHERE transaction_id=? AND tag=?', [req.params.id, req.params.tag]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/transactions/:id', requireAuth, async (req, res) => {
  try {
    await run('DELETE FROM transaction_tags WHERE transaction_id = ?', [req.params.id]);
    await run('DELETE FROM transactions WHERE id = ?', [req.params.id]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── CATEGORY MANAGEMENT ────────────────────────────────────────
app.get('/api/categories', requireAuth, async (req, res) => {
  try {
    const hh = req.household;
    const cats = await all(`SELECT c.*, COALESCE(s.total,0) as spent, COALESCE(s.cnt,0) as tx_count
      FROM categories c LEFT JOIN (
        SELECT cat, SUM(amount) as total, COUNT(*) as cnt FROM transactions
        WHERE household=? AND type='expense' GROUP BY cat
      ) s ON c.id = s.cat
      WHERE c.household=? AND c.is_active=TRUE ORDER BY c.sort_order`, [hh, hh]);
    res.json({ categories: cats });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/categories', requireAuth, async (req, res) => {
  const { name, icon, color, type = 'expense' } = req.body;
  if (!name) return res.status(400).json({ error: 'Missing name' });
  try {
    const hh = req.household;
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

app.put('/api/categories/:id', requireAuth, async (req, res) => {
  const { name, icon, color, sort_order, type, budget_amount } = req.body;
  try {
    const hh = req.household;
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

app.delete('/api/categories/:id', requireAuth, async (req, res) => {
  try {
    const hh = req.household;
    await run('UPDATE transactions SET cat=? WHERE household=? AND cat=?', ['Other', hh, req.params.id]);
    await run('UPDATE categories SET is_active=FALSE WHERE household=? AND id=?', [hh, req.params.id]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/categories/:id/transactions', requireAuth, async (req, res) => {
  const { group_by, startDate, endDate } = req.query;
  try {
    const hh = req.household;
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
app.get('/api/vendor-rules', requireAuth, async (req, res) => {
  try {
    const hh = req.household;
    const rules = await all('SELECT vendor, category, type FROM vendor_rules WHERE household = ?', [hh]);
    res.json({ rules });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Legacy category-icons endpoints (kept for backward compat)
app.get('/api/category-icons', requireAuth, async (req, res) => {
  try {
    const hh = req.household;
    // Serve from categories table now
    const icons = await all('SELECT id as category, icon FROM categories WHERE household=? AND is_active=TRUE', [hh]);
    res.json({ icons });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/category-icons', requireAuth, async (req, res) => {
  const { category, icon } = req.body;
  if (!category || !icon) return res.status(400).json({ error: 'Missing category or icon' });
  try {
    const hh = req.household;
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
app.get('/api/recurring', requireAuth, async (req, res) => {
  try {
    const hh = req.household;
    const rules = await all('SELECT * FROM recurring_rules WHERE household=? AND is_active=TRUE ORDER BY last_seen DESC', [hh]);
    res.json({ recurring: rules });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/recurring', requireAuth, async (req, res) => {
  const { vendor, category, expected_amount, frequency = 'monthly', is_subscription = 0 } = req.body;
  if (!vendor) return res.status(400).json({ error: 'Missing vendor' });
  try {
    const hh = req.household;
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

app.put('/api/recurring/:id', requireAuth, async (req, res) => {
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

app.post('/api/recurring/detect', requireAuth, async (req, res) => {
  try {
    const hh = req.household;
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

app.get('/api/subscriptions', requireAuth, async (req, res) => {
  try {
    const hh = req.household;
    const subs = await all('SELECT * FROM recurring_rules WHERE household=? AND is_subscription=TRUE AND is_active=TRUE', [hh]);
    const monthlyTotal = subs.reduce((a, s) => {
      const mult = s.frequency === 'weekly' ? 4.33 : s.frequency === 'quarterly' ? 1/3 : s.frequency === 'annual' ? 1/12 : 1;
      return a + (parseFloat(s.expected_amount) || 0) * mult;
    }, 0);
    res.json({ subscriptions: subs, monthly_total: monthlyTotal });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/recurring/dedup', requireAuth, async (req, res) => {
  try {
    const hh = req.household;
    // Keep the earliest rule per vendor, delete the rest
    const dupes = await all(`SELECT id FROM recurring_rules WHERE household=? AND id NOT IN (
      SELECT MIN(id) FROM recurring_rules WHERE household=? GROUP BY vendor
    )`, [hh, hh]);
    for (const d of dupes) await run('DELETE FROM recurring_rules WHERE id=?', [d.id]);
    res.json({ success: true, removed: dupes.length });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/recurring/dismiss', requireAuth, async (req, res) => {
  const { vendor } = req.body;
  if (!vendor) return res.status(400).json({ error: 'Missing vendor' });
  try {
    const hh = req.household;
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
app.get('/api/trends/categories', requireAuth, async (req, res) => {
  const { months = 6 } = req.query;
  try {
    const hh = req.household;
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

app.get('/api/trends/vendors', requireAuth, async (req, res) => {
  const { vendor, months = 6 } = req.query;
  try {
    const hh = req.household;
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

app.get('/api/trends/cashflow', requireAuth, async (req, res) => {
  const { months = '6' } = req.query;
  try {
    const hh = req.household;
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

app.get('/api/trends/daily-average', requireAuth, async (req, res) => {
  const { month } = req.query;
  try {
    const hh = req.household;
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

app.get('/api/income/breakdown', requireAuth, async (req, res) => {
  const { startDate, endDate } = req.query;
  try {
    const hh = req.household;
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
app.get('/api/review-queue', requireAuth, async (req, res) => {
  try {
    const hh = req.household;
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

app.post('/api/review-queue/resolve', requireAuth, async (req, res) => {
  const { vendor, category, type = 'expense' } = req.body;
  if (!vendor || !category) return res.status(400).json({ error: 'Missing vendor or category' });
  try {
    const hh = req.household;
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

app.get('/api/wizard/uncategorized-vendors', requireAuth, async (req, res) => {
  try {
    const hh = req.household;
    const vendors = await all(`SELECT t."desc" as vendor, COUNT(*) as cnt, SUM(t.amount) as total, t.cat as current_cat
      FROM transactions t
      LEFT JOIN vendor_rules vr ON vr.household=t.household AND vr.vendor=t."desc"
      WHERE t.household=? AND vr.vendor IS NULL
      GROUP BY t."desc", t.cat ORDER BY SUM(t.amount) DESC LIMIT 25`, [hh]);
    res.json({ vendors });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/wizard/bulk-assign', requireAuth, async (req, res) => {
  const { assignments } = req.body;
  if (!assignments || !assignments.length) return res.status(400).json({ error: 'Missing assignments' });
  try {
    const hh = req.household;
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

app.get('/api/status', requireAuth, async (req, res) => {
  try {
    const household = req.household;
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
app.get('/api/budgets', requireAuth, async (req, res) => {
  try {
    const hh = req.household;
    const rows = await all('SELECT id as category, budget_amount as amount FROM categories WHERE household = ? AND budget_amount > 0 AND is_active = TRUE ORDER BY sort_order', [hh]);
    res.json({ budgets: rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/budgets', requireAuth, async (req, res) => {
  const { category, amount } = req.body;
  if (!category || amount == null) return res.status(400).json({ error: 'Missing category or amount' });
  try {
    const hh = req.household;
    // Upsert category with budget amount
    await pool.query(
      `INSERT INTO categories (id, household, budget_amount) VALUES ($1, $2, $3)
       ON CONFLICT (id, household) DO UPDATE SET budget_amount = EXCLUDED.budget_amount`,
      [category, hh, parseFloat(amount)]
    );
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/budgets', requireAuth, async (req, res) => {
  const { category } = req.body;
  if (!category) return res.status(400).json({ error: 'Missing category' });
  try {
    const hh = req.household;
    await run('UPDATE categories SET budget_amount = 0 WHERE id = ? AND household = ?', [category, hh]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── VENDOR SPENDING ─────────────────────────────────────────────
app.get('/api/spending/by-vendor', requireAuth, async (req, res) => {
  const { startDate, endDate } = req.query;
  try {
    const hh = req.household;
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
  {
    name: 'search_transactions',
    description: 'Search for transactions matching criteria. Use when user asks about specific charges, vendors, amounts, or date ranges.',
    input_schema: {
      type: 'object',
      properties: {
        vendor: { type: 'string', description: 'Vendor/merchant name to search (partial match)' },
        category: { type: 'string', description: 'Category to filter by' },
        min_amount: { type: 'number', description: 'Minimum transaction amount' },
        max_amount: { type: 'number', description: 'Maximum transaction amount' },
        start_date: { type: 'string', description: 'Start date (YYYY-MM-DD)' },
        end_date: { type: 'string', description: 'End date (YYYY-MM-DD)' },
        type: { type: 'string', enum: ['expense', 'income', 'transfer'], description: 'Transaction type filter' },
        limit: { type: 'integer', description: 'Max results to return (default 20)' }
      }
    }
  },
  {
    name: 'get_spending_summary',
    description: 'Get spending breakdown by category or vendor for a date range. Use for questions like "how much did I spend on food" or "what are my biggest categories".',
    input_schema: {
      type: 'object',
      properties: {
        start_date: { type: 'string', description: 'Start date (YYYY-MM-DD)' },
        end_date: { type: 'string', description: 'End date (YYYY-MM-DD)' },
        group_by: { type: 'string', enum: ['category', 'vendor'], description: 'How to group the results' }
      },
      required: ['start_date', 'end_date', 'group_by']
    }
  },
  {
    name: 'get_vendor_history',
    description: 'Get all transactions from a specific vendor. Use when user asks about a specific merchant or store.',
    input_schema: {
      type: 'object',
      properties: {
        vendor: { type: 'string', description: 'Vendor name (partial match)' },
        months: { type: 'integer', description: 'How many months back to look (default 6)' }
      },
      required: ['vendor']
    }
  },
  {
    name: 'recategorize_vendor',
    description: 'Change the category for ALL transactions from a vendor. Creates a vendor rule for future transactions too. ALWAYS confirm with user before executing.',
    input_schema: {
      type: 'object',
      properties: {
        vendor: { type: 'string', description: 'Exact vendor name' },
        category: { type: 'string', description: 'New category to assign' },
        type: { type: 'string', enum: ['expense', 'income', 'transfer'], description: 'Transaction type' }
      },
      required: ['vendor', 'category']
    }
  },
  {
    name: 'set_budget',
    description: 'Create or update a monthly budget for a category.',
    input_schema: {
      type: 'object',
      properties: {
        category: { type: 'string', description: 'Category name' },
        amount: { type: 'number', description: 'Monthly budget amount in dollars' }
      },
      required: ['category', 'amount']
    }
  },
  {
    name: 'create_category',
    description: 'Create a new spending category.',
    input_schema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Category name' },
        icon: { type: 'string', description: 'Emoji icon for the category' }
      },
      required: ['name']
    }
  },
  {
    name: 'add_note',
    description: 'Add a note to a specific transaction.',
    input_schema: {
      type: 'object',
      properties: {
        transaction_id: { type: 'string', description: 'Transaction ID' },
        note: { type: 'string', description: 'Note text to add' }
      },
      required: ['transaction_id', 'note']
    }
  },
  {
    name: 'add_tags',
    description: 'Add a tag to one or more transactions. Use for bulk tagging like "tag all Costco as tax-deductible".',
    input_schema: {
      type: 'object',
      properties: {
        vendor: { type: 'string', description: 'If tagging by vendor, the vendor name' },
        transaction_ids: { type: 'array', items: { type: 'string' }, description: 'Specific transaction IDs to tag' },
        tag: { type: 'string', description: 'Tag name' }
      },
      required: ['tag']
    }
  },
  {
    name: 'get_recurring',
    description: 'List all confirmed recurring charges and subscriptions with amounts and frequency.',
    input_schema: { type: 'object', properties: {} }
  },
  {
    name: 'get_budget_status',
    description: 'Get current budget vs actual spending for all categories.',
    input_schema: {
      type: 'object',
      properties: {
        month: { type: 'string', description: 'Month in YYYY-MM format (default current month)' }
      }
    }
  },
  {
    name: 'forecast_balance',
    description: 'Project what the user\'s balance will be at a future date based on income/expense patterns.',
    input_schema: {
      type: 'object',
      properties: {
        target_date: { type: 'string', description: 'Future date to project to (YYYY-MM-DD)' }
      },
      required: ['target_date']
    }
  },
  {
    name: 'compare_periods',
    description: 'Compare spending between two time periods. Use for "how does this month compare to last month" questions.',
    input_schema: {
      type: 'object',
      properties: {
        period1_start: { type: 'string', description: 'First period start date' },
        period1_end: { type: 'string', description: 'First period end date' },
        period2_start: { type: 'string', description: 'Second period start date' },
        period2_end: { type: 'string', description: 'Second period end date' },
        group_by: { type: 'string', enum: ['category', 'vendor', 'total'], description: 'Comparison grouping' }
      },
      required: ['period1_start', 'period1_end', 'period2_start', 'period2_end']
    }
  },
  {
    name: 'suggest_budgets',
    description: 'Analyze spending history and suggest budget amounts for each category. Optionally apply a savings target percentage.',
    input_schema: {
      type: 'object',
      properties: {
        savings_target_pct: { type: 'number', description: 'Desired savings percentage (e.g. 15 for 15%)' },
        months_to_analyze: { type: 'integer', description: 'How many months of history to analyze (default 3)' }
      }
    }
  },
  {
    name: 'merge_vendors',
    description: 'Merge duplicate vendor names that refer to the same merchant. Keeps the primary vendor name and recategorizes all transactions from the merged names.',
    input_schema: {
      type: 'object',
      properties: {
        primary_vendor: { type: 'string', description: 'The vendor name to keep' },
        merge_vendors: { type: 'array', items: { type: 'string' }, description: 'Vendor names to merge into the primary' }
      },
      required: ['primary_vendor', 'merge_vendors']
    }
  }
];

const WRITE_TOOLS = new Set(['recategorize_vendor', 'set_budget', 'create_category', 'add_note', 'add_tags', 'merge_vendors']);

async function executeAgentTool(toolName, toolInput, hh) {
  const now = new Date();
  const currentMonthStart = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-01`;
  const currentMonthEnd = new Date(now.getFullYear(), now.getMonth()+1, 0).toISOString().split('T')[0];

  switch (toolName) {

    case 'search_transactions': {
      let conditions = ['t.household = $1'];
      let params = [hh];
      let idx = 2;
      if (toolInput.vendor) { conditions.push(`t."desc" ILIKE $${idx}`); params.push(`%${toolInput.vendor}%`); idx++; }
      if (toolInput.category) { conditions.push(`t.cat = $${idx}`); params.push(toolInput.category); idx++; }
      if (toolInput.min_amount) { conditions.push(`t.amount >= $${idx}`); params.push(toolInput.min_amount); idx++; }
      if (toolInput.max_amount) { conditions.push(`t.amount <= $${idx}`); params.push(toolInput.max_amount); idx++; }
      if (toolInput.start_date) { conditions.push(`t.date >= $${idx}`); params.push(toolInput.start_date); idx++; }
      if (toolInput.end_date) { conditions.push(`t.date <= $${idx}`); params.push(toolInput.end_date); idx++; }
      if (toolInput.type) { conditions.push(`t.type = $${idx}`); params.push(toolInput.type); idx++; }
      const limit = toolInput.limit || 20;
      const rows = await all(`SELECT t.id, t."desc", t.amount, t.type, t.cat, t.date, t.pending
        FROM transactions t WHERE ${conditions.join(' AND ')}
        ORDER BY t.date DESC LIMIT ${limit}`, params);
      return { transactions: rows, count: rows.length };
    }

    case 'get_spending_summary': {
      const { start_date, end_date, group_by } = toolInput;
      const groupCol = group_by === 'vendor' ? '"desc"' : 'cat';
      const rows = await all(`SELECT ${groupCol} as name, SUM(CASE WHEN type='expense' THEN amount ELSE 0 END) as debits,
        SUM(CASE WHEN type='income' THEN amount ELSE 0 END) as credits, COUNT(*) as tx_count
        FROM transactions WHERE household = $1 AND date >= $2 AND date <= $3
        GROUP BY ${groupCol} ORDER BY SUM(CASE WHEN type='expense' THEN amount ELSE 0 END) DESC`,
        [hh, start_date, end_date]);
      const total = rows.reduce((a, r) => a + parseFloat(r.debits) - parseFloat(r.credits), 0);
      return { breakdown: rows.map(r => ({ ...r, net: parseFloat(r.debits) - parseFloat(r.credits) })), total };
    }

    case 'get_vendor_history': {
      const months = toolInput.months || 6;
      const cutoff = new Date(); cutoff.setMonth(cutoff.getMonth() - months);
      const rows = await all(`SELECT id, "desc", amount, type, cat, date
        FROM transactions WHERE household = $1 AND "desc" ILIKE $2 AND date >= $3
        ORDER BY date DESC`, [hh, `%${toolInput.vendor}%`, cutoff.toISOString().split('T')[0]]);
      const total = rows.reduce((a, r) => a + (r.type === 'expense' ? parseFloat(r.amount) : -parseFloat(r.amount)), 0);
      return { vendor: toolInput.vendor, transactions: rows, total, count: rows.length };
    }

    case 'recategorize_vendor': {
      const { vendor, category, type: txType } = toolInput;
      const resolvedType = txType || 'expense';

      // Find all matching transactions using partial case-insensitive match
      const matchRows = await all(
        'SELECT id, "desc" FROM transactions WHERE household = $1 AND "desc" ILIKE $2',
        [hh, `%${vendor}%`]
      );

      if (matchRows.length === 0) {
        return { success: false, vendor, category, type: resolvedType, transactions_updated: 0,
          message: `No transactions found matching "${vendor}". Try a different search term.` };
      }

      // Ensure the target category exists
      await run('INSERT INTO categories (id, household) VALUES ($1, $2) ON CONFLICT DO NOTHING', [category, hh]);

      // Create vendor rules for each unique variant
      const uniqueVendors = [...new Set(matchRows.map(r => r.desc))];
      for (const v of uniqueVendors) {
        await run(`INSERT INTO vendor_rules (household, vendor, category, type, updated_at)
          VALUES ($1, $2, $3, $4, NOW()) ON CONFLICT (household, vendor) DO UPDATE SET category=$3, type=$4, updated_at=NOW()`,
          [hh, v, category, resolvedType]);
      }

      // Update all matching transactions
      const result = await run(
        'UPDATE transactions SET cat=$1, type=$2, reviewed=TRUE WHERE household=$3 AND "desc" ILIKE $4',
        [category, resolvedType, hh, `%${vendor}%`]);

      return { success: true, vendor, category, type: resolvedType,
        transactions_updated: result.changes || matchRows.length,
        unique_vendor_names_matched: uniqueVendors.length,
        sample_vendors: uniqueVendors.slice(0, 5) };
    }

    case 'set_budget': {
      await pool.query(`INSERT INTO categories (id, household, budget_amount) VALUES ($1,$2,$3)
        ON CONFLICT (id, household) DO UPDATE SET budget_amount=EXCLUDED.budget_amount`,
        [toolInput.category, hh, toolInput.amount]);
      return { success: true, category: toolInput.category, amount: toolInput.amount };
    }

    case 'create_category': {
      await pool.query('INSERT INTO categories (id, household, icon) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING',
        [toolInput.name, hh, toolInput.icon || '📌']);
      return { success: true, category: toolInput.name };
    }

    case 'add_note': {
      await run('UPDATE transactions SET notes=$1 WHERE id=$2 AND household=$3',
        [toolInput.note, toolInput.transaction_id, hh]);
      return { success: true };
    }

    case 'add_tags': {
      let txIds = toolInput.transaction_ids || [];
      if (toolInput.vendor && !txIds.length) {
        const rows = await all('SELECT id FROM transactions WHERE household=$1 AND "desc" ILIKE $2',
          [hh, `%${toolInput.vendor}%`]);
        txIds = rows.map(r => r.id);
      }
      for (const id of txIds) {
        await run('INSERT INTO transaction_tags (transaction_id, tag, household) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING',
          [id, toolInput.tag, hh]);
      }
      return { success: true, tagged: txIds.length, tag: toolInput.tag };
    }

    case 'get_recurring': {
      const recurring = await all('SELECT vendor, category, expected_amount, frequency, is_subscription, last_seen FROM recurring_rules WHERE household=$1 AND is_active=TRUE ORDER BY expected_amount DESC', [hh]);
      const total = recurring.reduce((a, r) => a + parseFloat(r.expected_amount || 0), 0);
      return { recurring, monthly_total: total };
    }

    case 'get_budget_status': {
      const month = toolInput.month || `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
      const [y, m] = month.split('-').map(Number);
      const mStart = `${y}-${String(m).padStart(2,'0')}-01`;
      const mEnd = new Date(y, m, 0).toISOString().split('T')[0];
      const budgets = await all('SELECT id as category, budget_amount as amount FROM categories WHERE household=$1 AND budget_amount > 0', [hh]);
      const spending = await all(`SELECT cat, SUM(amount) as spent FROM transactions WHERE household=$1 AND type='expense' AND date>=$2 AND date<=$3 GROUP BY cat`, [hh, mStart, mEnd]);
      const spendMap = {};
      spending.forEach(s => { spendMap[s.cat] = parseFloat(s.spent); });
      return {
        month,
        budgets: budgets.map(b => ({
          category: b.category,
          budget: parseFloat(b.amount),
          spent: spendMap[b.category] || 0,
          remaining: parseFloat(b.amount) - (spendMap[b.category] || 0),
          pct_used: b.amount > 0 ? Math.round((spendMap[b.category] || 0) / parseFloat(b.amount) * 100) : 0
        }))
      };
    }

    case 'forecast_balance': {
      const accounts = await all('SELECT SUM(balance) as total FROM accounts WHERE household=$1', [hh]);
      const currentBalance = parseFloat(accounts[0]?.total) || 0;
      const targetDate = new Date(toolInput.target_date);
      const daysAhead = Math.ceil((targetDate - now) / (1000*60*60*24));
      const thirtyAgo = new Date(); thirtyAgo.setDate(thirtyAgo.getDate() - 30);
      const recentTxs = await all(`SELECT type, SUM(amount) as total FROM transactions WHERE household=$1 AND date>=$2 GROUP BY type`, [hh, thirtyAgo.toISOString().split('T')[0]]);
      const income30 = parseFloat(recentTxs.find(t => t.type === 'income')?.total || 0);
      const expenses30 = parseFloat(recentTxs.find(t => t.type === 'expense')?.total || 0);
      const dailyNet = (income30 - expenses30) / 30;
      const projected = currentBalance + (dailyNet * daysAhead);
      return { current_balance: currentBalance, target_date: toolInput.target_date, days_ahead: daysAhead, daily_net_avg: dailyNet, projected_balance: projected };
    }

    case 'compare_periods': {
      const getData = async (start, end) => {
        const groupCol = toolInput.group_by === 'vendor' ? '"desc"' : (toolInput.group_by === 'category' ? 'cat' : null);
        if (groupCol) {
          return await all(`SELECT ${groupCol} as name, SUM(CASE WHEN type='expense' THEN amount ELSE 0 END) as expenses,
            SUM(CASE WHEN type='income' THEN amount ELSE 0 END) as income, COUNT(*) as tx_count
            FROM transactions WHERE household=$1 AND date>=$2 AND date<=$3 GROUP BY ${groupCol} ORDER BY SUM(amount) DESC`,
            [hh, start, end]);
        } else {
          return await all(`SELECT type, SUM(amount) as total, COUNT(*) as cnt FROM transactions WHERE household=$1 AND date>=$2 AND date<=$3 GROUP BY type`, [hh, start, end]);
        }
      };
      const p1 = await getData(toolInput.period1_start, toolInput.period1_end);
      const p2 = await getData(toolInput.period2_start, toolInput.period2_end);
      return { period1: { start: toolInput.period1_start, end: toolInput.period1_end, data: p1 }, period2: { start: toolInput.period2_start, end: toolInput.period2_end, data: p2 } };
    }

    case 'suggest_budgets': {
      const months = toolInput.months_to_analyze || 3;
      const cutoff = new Date(); cutoff.setMonth(cutoff.getMonth() - months);
      const rows = await all(`SELECT cat, AVG(monthly_total) as avg_spend FROM (
        SELECT cat, TO_CHAR(date::date, 'YYYY-MM') as month, SUM(amount) as monthly_total
        FROM transactions WHERE household=$1 AND type='expense' AND date>=$2
        GROUP BY cat, TO_CHAR(date::date, 'YYYY-MM')
      ) sub GROUP BY cat ORDER BY avg_spend DESC`, [hh, cutoff.toISOString().split('T')[0]]);
      const totalAvg = rows.reduce((a, r) => a + parseFloat(r.avg_spend), 0);
      const savingsTarget = toolInput.savings_target_pct || 0;
      const multiplier = savingsTarget > 0 ? (100 - savingsTarget) / 100 : 1.1;
      return {
        suggestions: rows.map(r => ({
          category: r.cat,
          avg_monthly_spend: Math.round(parseFloat(r.avg_spend) * 100) / 100,
          suggested_budget: Math.round(parseFloat(r.avg_spend) * multiplier * 100) / 100
        })),
        total_avg_spend: totalAvg,
        savings_target_pct: savingsTarget
      };
    }

    case 'merge_vendors': {
      const { primary_vendor, merge_vendors } = toolInput;
      const primaryRule = await get('SELECT category, type FROM vendor_rules WHERE household=$1 AND vendor=$2', [hh, primary_vendor]);
      if (!primaryRule) return { error: `No vendor rule found for "${primary_vendor}". Categorize the primary vendor first.` };
      let totalUpdated = 0;
      for (const v of merge_vendors) {
        const r = await run('UPDATE transactions SET cat=$1, type=$2 WHERE household=$3 AND "desc"=$4',
          [primaryRule.category, primaryRule.type, hh, v]);
        totalUpdated += r.changes;
        await run('DELETE FROM vendor_rules WHERE household=$1 AND vendor=$2', [hh, v]);
      }
      return { success: true, primary_vendor, merged: merge_vendors.length, transactions_updated: totalUpdated };
    }

    default:
      return { error: `Unknown tool: ${toolName}` };
  }
}

app.post('/api/chat', requireAuth, async (req, res) => {
  if (!anthropic) return res.status(500).json({ error: 'ANTHROPIC_API_KEY not configured' });
  const { message, history = [], confirmAction } = req.body;
  if (!message && !confirmAction) return res.status(400).json({ error: 'Missing message' });

  try {
    const hh = req.household;

    // If confirming a pending action, execute it
    if (confirmAction) {
      const result = await executeAgentTool(confirmAction.tool, confirmAction.input, hh);
      const t = confirmAction.tool;
      const inp = confirmAction.input;
      let reply;

      if (result.error) {
        reply = `⚠️ I couldn't complete that action: ${result.error}`;
      } else if (t === 'recategorize_vendor') {
        if (result.transactions_updated === 0) {
          reply = `I couldn't find any transactions matching "${inp.vendor}". ${result.message || 'Try a different search term.'}`;
        } else {
          const variantNote = result.unique_vendor_names_matched > 1
            ? ` across ${result.unique_vendor_names_matched} vendor name variants` : '';
          const typeNote = inp.type && inp.type !== 'expense' ? ` as ${inp.type}` : '';
          reply = `✅ Updated ${result.transactions_updated} "${inp.vendor}" transactions to **${inp.category}**${typeNote}${variantNote}. Future ones will auto-categorize.`;
        }
      } else if (t === 'set_budget') {
        reply = `✅ Budget set — **${inp.category}**: $${parseFloat(inp.amount).toFixed(2)}/month`;
      } else if (t === 'create_category') {
        reply = `✅ Category "${inp.name}" created.`;
      } else if (t === 'add_note') {
        reply = `✅ Note added to transaction.`;
      } else if (t === 'add_tags') {
        reply = `✅ Tagged ${result.tagged || 0} transactions with "${inp.tag}".`;
      } else if (t === 'merge_vendors') {
        reply = `✅ Merged ${result.merged || 0} vendor variants into "${inp.primary_vendor}". ${result.transactions_updated || 0} transactions updated.`;
      } else {
        reply = '✅ Done.';
      }

      return res.json({ reply, actionExecuted: true });
    }

    const now = new Date();
    const monthStart = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-01`;
    const lastDay = new Date(now.getFullYear(), now.getMonth()+1, 0).getDate();
    const monthEnd = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-${String(lastDay).padStart(2,'0')}`;

    const monthTxs = await all(`SELECT "desc", amount, type, cat, date FROM transactions WHERE household = $1 AND date >= $2 AND date <= $3 ORDER BY date DESC`, [hh, monthStart, monthEnd]);
    const income = monthTxs.filter(t=>t.type==='income').reduce((a,t)=>a+parseFloat(t.amount),0);
    const expenses = monthTxs.filter(t=>t.type==='expense').reduce((a,t)=>a+parseFloat(t.amount),0);
    const catSpend = {};
    monthTxs.filter(t=>t.type==='expense').forEach(t=>{ catSpend[t.cat]=(catSpend[t.cat]||0)+parseFloat(t.amount); });
    const vendorSpend = {};
    monthTxs.filter(t=>t.type==='expense').forEach(t=>{ vendorSpend[t.desc]=(vendorSpend[t.desc]||0)+parseFloat(t.amount); });
    const topVendors = Object.entries(vendorSpend).sort((a,b)=>b[1]-a[1]).slice(0,15);
    const budgetRows = await all('SELECT id as category, budget_amount as amount FROM categories WHERE household = $1 AND budget_amount > 0', [hh]);
    const categories = await all('SELECT id, icon, budget_amount FROM categories WHERE household = $1 AND is_active = TRUE', [hh]);
    const accts = await all(`SELECT a.name, a.type, a.subtype, a.balance FROM accounts a JOIN users u ON a.user_id = u.id WHERE u.household = $1`, [hh]);

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

    const systemPrompt = `You are Obsidian AI, a personal finance assistant for the ${hh} household. You have access to their real financial data and can take actions on their behalf.

${context}

CAPABILITIES:
- You can search transactions, analyze spending, compare periods, and forecast balances
- You can recategorize vendors, set budgets, create categories, add notes and tags
- You can suggest budgets based on spending history
- You can review recurring charges and subscriptions

RULES:
1. Use your tools to get precise data — don't guess from the summary above when exact data is available
2. ALWAYS use tools for questions about specific vendors, amounts, or date ranges
3. For WRITE actions (recategorize, set budget, create category), CONFIRM with the user before executing. Say what you'll do and ask "Should I go ahead?"
4. After executing a write action, tell the user what changed (e.g. "Done — updated 34 Amazon transactions to Shopping")
5. Be conversational but concise. Use specific $ amounts. No fluff
6. Format amounts as $X,XXX.XX
7. When comparing periods, calculate the actual difference and percentage change
8. If the user asks to recategorize, search for matching transactions first to show them what will be affected
9. Today's date is ${now.toISOString().split('T')[0]}`;

    const messages = [...history.slice(-10), { role: 'user', content: message }];

    // Initial API call with tools
    let response = await anthropic.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 2048,
      system: systemPrompt,
      tools: agentTools,
      messages,
    });

    // Tool use loop — keep processing until we get a final text response
    let allActions = [];
    let iterations = 0;
    const MAX_ITERATIONS = 5;
    let loopMessages = [...messages];

    while (response.stop_reason === 'tool_use' && iterations < MAX_ITERATIONS) {
      iterations++;
      const toolUseBlocks = response.content.filter(c => c.type === 'tool_use');

      // Check if any write tools need confirmation (only on first encounter)
      const writeToolUse = toolUseBlocks.find(t => WRITE_TOOLS.has(t.name));
      if (writeToolUse && iterations === 1) {
        // Check if user already confirmed in natural language (e.g. "yes", "go ahead")
        const lastUserMsg = (message || '').toLowerCase().trim();
        const userAlreadyConfirmed = /^(yes|yeah|yep|yup|sure|ok|okay|k|confirmed|confirm|go ahead|do it|proceed|yes please|please do|fine|correct|right)\b/i.test(lastUserMsg);

        if (!userAlreadyConfirmed) {
          const textContent = response.content.find(c => c.type === 'text');
          return res.json({
            reply: textContent?.text || `I'd like to make a change. Should I go ahead?`,
            pendingAction: { tool: writeToolUse.name, input: writeToolUse.input }
          });
        }
        // User already confirmed — fall through to execute
      }

      // Add assistant message with tool calls
      loopMessages.push({ role: 'assistant', content: response.content });

      // Execute each tool call and collect results
      const toolResultContents = [];
      for (const tool of toolUseBlocks) {
        try {
          const result = await executeAgentTool(tool.name, tool.input, hh);
          toolResultContents.push({
            type: 'tool_result',
            tool_use_id: tool.id,
            content: JSON.stringify(result)
          });
          if (WRITE_TOOLS.has(tool.name)) {
            allActions.push({ tool: tool.name, input: tool.input, result });
          }
        } catch (e) {
          toolResultContents.push({
            type: 'tool_result',
            tool_use_id: tool.id,
            content: JSON.stringify({ error: e.message }),
            is_error: true
          });
        }
      }

      loopMessages.push({ role: 'user', content: toolResultContents });

      // Get next response
      response = await anthropic.messages.create({
        model: 'claude-sonnet-4-6',
        max_tokens: 2048,
        system: systemPrompt,
        tools: agentTools,
        messages: loopMessages,
      });
    }

    // Extract text response
    const textContent = response.content.filter(c => c.type === 'text').map(c => c.text).join('\n');

    res.json({
      reply: textContent || 'I couldn\'t generate a response.',
      actions: allActions.length > 0 ? allActions : undefined,
    });
  } catch (e) {
    console.error('Chat error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─── AUTH & ONBOARDING ENDPOINTS ────────────────────────────────
app.get('/api/me', requireAuth, async (req, res) => {
  try {
    const profile = await get('SELECT * FROM user_profiles WHERE id = $1', [req.user.id]);
    const household = req.household
      ? await get('SELECT * FROM households WHERE id = $1', [req.household])
      : null;
    const members = req.household
      ? await all(`SELECT hm.user_id, hm.role, up.display_name, up.avatar_color FROM household_members hm
          LEFT JOIN user_profiles up ON up.id = hm.user_id WHERE hm.household_id = $1`, [req.household])
      : [];
    res.json({ user: req.user, profile, household, members, needsOnboarding: req.needsOnboarding || !profile });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/me/profile', requireAuth, async (req, res) => {
  const { display_name, avatar_color } = req.body;
  try {
    await run(`INSERT INTO user_profiles (id, display_name, avatar_color, updated_at)
      VALUES ($1, $2, $3, NOW())
      ON CONFLICT (id) DO UPDATE SET display_name=$2, avatar_color=$3, updated_at=NOW()`,
      [req.user.id, display_name, avatar_color || '#E8A828']);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/households', requireAuth, async (req, res) => {
  const { name } = req.body;
  if (!name) return res.status(400).json({ error: 'Missing name' });
  try {
    const result = await pool.query(
      'INSERT INTO households (name, created_by) VALUES ($1, $2) RETURNING id',
      [name, req.user.id]
    );
    const householdId = result.rows[0].id;
    await run('INSERT INTO household_members (household_id, user_id, role) VALUES ($1, $2, $3)',
      [householdId, req.user.id, 'owner']);
    const defaultCats = {Housing:'🏠',Food:'🍽️',Transport:'🚗',Health:'💊',Entertainment:'🎬',
      Shopping:'🛍️',Utilities:'⚡',Income:'💰',Transfer:'🔁',Other:'📌',Subscriptions:'🔄',Personal:'💆'};
    for (const [cat, icon] of Object.entries(defaultCats)) {
      await pool.query('INSERT INTO categories (id, household, icon) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING',
        [cat, householdId, icon]);
    }
    res.json({ success: true, household_id: householdId });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/households/invite', requireAuth, async (req, res) => {
  const { email } = req.body;
  if (!email) return res.status(400).json({ error: 'Missing email' });
  if (!req.household) return res.status(400).json({ error: 'No household' });
  const member = await get('SELECT role FROM household_members WHERE household_id = $1 AND user_id = $2',
    [req.household, req.user.id]);
  if (member?.role !== 'owner') return res.status(403).json({ error: 'Only household owners can invite' });
  try {
    if (!supabaseAdmin) return res.status(500).json({ error: 'Supabase admin not configured' });
    const { data, error } = await supabaseAdmin.auth.admin.inviteUserByEmail(email, {
      data: { household_id: req.household, invited_by: req.user.id }
    });
    if (error) throw error;
    await run('INSERT INTO household_members (household_id, user_id, role) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING',
      [req.household, data.user.id, 'member']);
    res.json({ success: true, invited: email });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/me', requireAuth, async (req, res) => {
  try {
    const household = req.household;
    if (household) {
      const otherMembers = await all('SELECT user_id FROM household_members WHERE household_id = $1 AND user_id != $2',
        [household, req.user.id]);
      if (otherMembers.length === 0) {
        for (const tbl of ['transactions','vendor_rules','accounts','categories','recurring_rules','transaction_tags','challenges']) {
          await run(`DELETE FROM ${tbl} WHERE household = $1`, [household]);
        }
        await run('DELETE FROM plaid_items WHERE user_id = $1', [req.user.id]);
        await run('DELETE FROM households WHERE id = $1', [household]);
      }
      await run('DELETE FROM household_members WHERE user_id = $1', [req.user.id]);
    }
    await run('DELETE FROM user_profiles WHERE id = $1', [req.user.id]);
    if (supabaseAdmin) await supabaseAdmin.auth.admin.deleteUser(req.user.id);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── MAINTENANCE ────────────────────────────────────────────────
app.post('/api/maintenance/dedup', requireAuth, async (req, res) => {
  try {
    const hh = req.household;
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

    // 2. Dedup pending/posted transactions (amount+account within 3 days)
    const pendDupes = await pool.query(`
      DELETE FROM transactions WHERE id IN (
        SELECT DISTINCT p.id FROM transactions p
        INNER JOIN transactions posted ON p.amount = posted.amount
          AND p.account_id = posted.account_id AND p.household = posted.household
          AND ABS(posted.date::date - p.date::date) <= 3
        WHERE p.household = $1 AND p.pending = TRUE AND posted.pending = FALSE AND p.id != posted.id
      ) RETURNING id`, [hh]);
    removed += pendDupes.rowCount;

    // 3. Dedup cross-account duplicates — prefer records with user edits
    const crossDupes = await pool.query(`
      DELETE FROM transactions WHERE id IN (
        SELECT id FROM (
          SELECT id, ROW_NUMBER() OVER (
            PARTITION BY household, "desc", amount, date
            ORDER BY
              (CASE WHEN notes IS NOT NULL AND notes != '' THEN 0 ELSE 1 END),
              (CASE WHEN reviewed = TRUE THEN 0 ELSE 1 END),
              (CASE WHEN status IS NOT NULL AND status != 'confirmed' THEN 0 ELSE 1 END),
              (CASE WHEN is_recurring = TRUE THEN 0 ELSE 1 END),
              created_at ASC
          ) as rn
          FROM transactions WHERE household = $1
        ) ranked WHERE rn > 1
      ) RETURNING id`, [hh]);
    removed += crossDupes.rowCount;

    res.json({ success: true, removed, accounts_deduped: acctDupes.rowCount, pending_deduped: pendDupes.rowCount, cross_deduped: crossDupes.rowCount });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── VENDOR SUMMARY + BULK RULES ────────────────────────────────
app.get('/api/vendor-summary', requireAuth, async (req, res) => {
  try {
    const hh = req.household;
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

app.post('/api/vendor-rules/bulk', requireAuth, async (req, res) => {
  const { assignments } = req.body;
  if (!assignments || !assignments.length) return res.status(400).json({ error: 'No assignments' });
  try {
    const hh = req.household;
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      let totalUpdated = 0;
      for (const a of assignments) {
        // Transfer category always forces transfer type
        const finalType = a.category === 'Transfer' ? 'transfer' : (a.type || 'expense');
        await client.query(
          `INSERT INTO vendor_rules (household, vendor, category, type, updated_at) VALUES ($1, $2, $3, $4, NOW())
           ON CONFLICT (household, vendor) DO UPDATE SET category = EXCLUDED.category, type = EXCLUDED.type, updated_at = NOW()`,
          [hh, a.vendor, a.category, finalType]
        );
        // Update BOTH cat and type on existing transactions
        const result = await client.query(
          'UPDATE transactions SET cat = $1, type = $2, reviewed = TRUE WHERE household = $3 AND "desc" = $4',
          [a.category, finalType, hh, a.vendor]
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

// ─── VENDOR MERGE ──────────────────────────────────────────────
app.post('/api/vendors/merge', requireAuth, async (req, res) => {
  const { primary_vendor, merge_vendors } = req.body;
  if (!primary_vendor || !merge_vendors?.length) return res.status(400).json({ error: 'Missing primary_vendor or merge_vendors' });
  try {
    const hh = req.household;
    const primaryRule = await get('SELECT category, type FROM vendor_rules WHERE household=$1 AND vendor=$2', [hh, primary_vendor]);
    if (!primaryRule) return res.status(400).json({ error: `No vendor rule found for "${primary_vendor}". Categorize the primary vendor first.` });
    let totalUpdated = 0;
    for (const v of merge_vendors) {
      const r = await run('UPDATE transactions SET cat=$1, type=$2 WHERE household=$3 AND "desc"=$4',
        [primaryRule.category, primaryRule.type, hh, v]);
      totalUpdated += r.changes;
      await run('DELETE FROM vendor_rules WHERE household=$1 AND vendor=$2', [hh, v]);
    }
    res.json({ success: true, merged: merge_vendors.length, transactions_updated: totalUpdated });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── HEALTH SCORE ───────────────────────────────────────────────
app.get('/api/health-score', requireAuth, async (req, res) => {
  try {
    const hh = req.household;
    const now = new Date();
    const monthStart = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-01`;
    const monthEnd = new Date(now.getFullYear(), now.getMonth()+1, 0).toISOString().split('T')[0];
    const mtxs = await all('SELECT * FROM transactions WHERE household = ? AND date >= ? AND date <= ?', [hh, monthStart, monthEnd]);
    const income = mtxs.filter(t => t.type === 'income').reduce((a,t) => a + parseFloat(t.amount), 0);
    const expenses = mtxs.filter(t => t.type === 'expense').reduce((a,t) => a + parseFloat(t.amount), 0);
    const savingsRate = income > 0 ? (income - expenses) / income : 0;
    const savingsScore = Math.min(30, Math.max(0, Math.round(savingsRate * 150)));
    const budgetRows = await all('SELECT id as category, budget_amount as amount FROM categories WHERE household = ? AND budget_amount > 0 AND is_active = TRUE', [hh]);
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
app.post('/api/streak/check-in', requireAuth, async (req, res) => {
  try {
    const user = await get('SELECT * FROM users WHERE id = ?', [req.user.id]);
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
    await run('UPDATE users SET streak_count = ?, streak_best = ?, last_active_date = ? WHERE id = ?', [streak, best, today, req.user.id]);
    res.json({ streak, best, is_new_day: user.last_active_date !== today, milestone: [7,30,100,365].includes(streak) });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── CHALLENGES ─────────────────────────────────────────────────
app.get('/api/challenges', requireAuth, async (req, res) => {
  try {
    const hh = req.household;
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
app.get('/api/weekly-recap', requireAuth, async (req, res) => {
  try {
    const hh = req.household;
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
