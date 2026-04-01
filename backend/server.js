/**
 * LEDGER — Backend Server
 * Express + better-sqlite3 + Plaid
 */

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');
const { Configuration, PlaidApi, PlaidEnvironments, Products, CountryCode } = require('plaid');
const Anthropic = require('@anthropic-ai/sdk');

const app = express();
app.use(express.json());
app.use(cors({ origin: '*' }));

// ─── DATABASE ────────────────────────────────────────────────────
const dbPath = process.env.DB_PATH || path.join(__dirname, 'ledger.db');
const dbDir = path.dirname(dbPath);
if (!fs.existsSync(dbDir)) fs.mkdirSync(dbDir, { recursive: true });
const db = new Database(dbPath);
db.pragma('journal_mode = WAL');
console.log(`  DB: ${dbPath} opened`);

const run = (sql, params = []) => db.prepare(sql).run(...params);
const get = (sql, params = []) => db.prepare(sql).get(...params);
const all = (sql, params = []) => db.prepare(sql).all(...params);

function initDB() {
  db.exec(`CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY, name TEXT NOT NULL,
    household TEXT NOT NULL DEFAULT 'default',
    created_at TEXT DEFAULT (datetime('now')))`);
  db.exec(`CREATE TABLE IF NOT EXISTS plaid_items (
    item_id TEXT PRIMARY KEY, user_id TEXT NOT NULL,
    access_token TEXT NOT NULL, institution TEXT,
    created_at TEXT DEFAULT (datetime('now')))`);
  db.exec(`CREATE TABLE IF NOT EXISTS transactions (
    id TEXT PRIMARY KEY, household TEXT NOT NULL DEFAULT 'default',
    user_id TEXT, desc TEXT NOT NULL, amount REAL NOT NULL,
    type TEXT NOT NULL, cat TEXT NOT NULL, date TEXT NOT NULL,
    pending INTEGER DEFAULT 0, account_id TEXT,
    source TEXT DEFAULT 'manual',
    created_at TEXT DEFAULT (datetime('now')))`);
  db.exec(`CREATE TABLE IF NOT EXISTS accounts (
    account_id TEXT PRIMARY KEY, user_id TEXT,
    household TEXT NOT NULL DEFAULT 'default',
    name TEXT, mask TEXT, type TEXT, subtype TEXT,
    balance REAL, updated_at TEXT DEFAULT (datetime('now')))`);
  db.exec(`CREATE TABLE IF NOT EXISTS budgets (
    household TEXT NOT NULL, category TEXT NOT NULL, amount REAL NOT NULL,
    updated_at TEXT DEFAULT (datetime('now')), PRIMARY KEY (household, category)
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS vendor_rules (
    household TEXT NOT NULL, vendor TEXT NOT NULL, category TEXT NOT NULL,
    type TEXT NOT NULL DEFAULT 'expense', updated_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (household, vendor)
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS category_icons (
    household TEXT NOT NULL, category TEXT NOT NULL, icon TEXT NOT NULL,
    PRIMARY KEY (household, category)
  )`);

  // ── SAFE COLUMN ADDITIONS ──
  const addCol = (table, col, type) => {
    try { db.exec(`ALTER TABLE ${table} ADD COLUMN ${col} ${type}`); }
    catch(e) { /* column already exists */ }
  };
  addCol('transactions', 'notes', 'TEXT');
  addCol('transactions', 'is_recurring', 'INTEGER DEFAULT 0');
  addCol('transactions', 'recurring_group_id', 'TEXT');
  addCol('transactions', 'reviewed', 'INTEGER DEFAULT 0');
  addCol('transactions', 'original_cat', 'TEXT');
  addCol('transactions', 'original_sign', 'INTEGER DEFAULT -1');
  addCol('transactions', 'status', "TEXT DEFAULT 'confirmed'");
  addCol('users', 'health_score', 'INTEGER DEFAULT 0');
  addCol('users', 'health_updated', 'TEXT');
  addCol('users', 'streak_count', 'INTEGER DEFAULT 0');
  addCol('users', 'streak_best', 'INTEGER DEFAULT 0');
  addCol('users', 'last_active_date', 'TEXT');

  // ── NEW TABLES ──
  db.exec(`CREATE TABLE IF NOT EXISTS transaction_tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id TEXT NOT NULL, tag TEXT NOT NULL, household TEXT NOT NULL,
    UNIQUE(transaction_id, tag)
  )`);

  db.exec(`CREATE TABLE IF NOT EXISTS categories (
    id TEXT NOT NULL, household TEXT NOT NULL,
    icon TEXT DEFAULT '📌', color TEXT DEFAULT '#7a78a0',
    type TEXT DEFAULT 'expense', sort_order INTEGER DEFAULT 0, is_active INTEGER DEFAULT 1,
    PRIMARY KEY (id, household)
  )`);

  db.exec(`CREATE TABLE IF NOT EXISTS recurring_rules (
    id TEXT PRIMARY KEY, household TEXT NOT NULL, vendor TEXT NOT NULL,
    category TEXT, expected_amount REAL, frequency TEXT,
    is_subscription INTEGER DEFAULT 0, last_seen TEXT, is_active INTEGER DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now'))
  )`);

  db.exec(`CREATE TABLE IF NOT EXISTS challenges (
    id TEXT PRIMARY KEY, household TEXT NOT NULL, month TEXT NOT NULL,
    title TEXT NOT NULL, description TEXT, target_value REAL,
    current_value REAL DEFAULT 0, challenge_type TEXT NOT NULL, category TEXT,
    is_completed INTEGER DEFAULT 0, created_at TEXT DEFAULT (datetime('now'))
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
  const seedCat = db.prepare('INSERT OR IGNORE INTO categories (id, household, icon, color) VALUES (?, ?, ?, ?)');
  for (const [name, info] of Object.entries(defaultCats)) {
    seedCat.run(name, 'spenziero', info.icon, info.color);
  }
  // Migrate existing category_icons
  try {
    const existingIcons = all('SELECT category, icon FROM category_icons WHERE household = ?', ['spenziero']);
    const updateCatIcon = db.prepare('UPDATE categories SET icon = ? WHERE id = ? AND household = ?');
    for (const row of existingIcons) updateCatIcon.run(row.icon, row.category, 'spenziero');
  } catch(e) {}

  // ── MIGRATE REFUND → INCOME (Phase 4) ──
  run("UPDATE transactions SET type = 'income' WHERE type = 'refund'");
  run("UPDATE vendor_rules SET type = 'income' WHERE type = 'refund'");
  // Ensure all amounts are positive (Math.abs) — undo any negative amount migration
  run("UPDATE transactions SET amount = ABS(amount) WHERE amount < 0");

  run(`INSERT OR IGNORE INTO users (id, name, household) VALUES (?, ?, ?)`, ['christian', 'Christian', 'spenziero']);
  run(`INSERT OR IGNORE INTO users (id, name, household) VALUES (?, ?, ?)`, ['wife', 'Marisol', 'spenziero']);
  const defaultBudgets = {Housing:2000,Food:800,Transport:400,Health:300,Entertainment:200,Shopping:400,Utilities:250};
  for (const [cat, amt] of Object.entries(defaultBudgets)) {
    run('INSERT OR IGNORE INTO budgets (household, category, amount) VALUES (?, ?, ?)', ['spenziero', cat, amt]);
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

function getHousehold(userId) {
  const user = get('SELECT household FROM users WHERE id = ?', [userId]);
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
  if (c.includes('TRANSFER') || c.includes('WIRE') || c.includes('ACH'))          return 'Transfer';
  if (c.includes('PAYROLL') || c.includes('INCOME') || c.includes('DEPOSIT'))     return 'Income';
  return 'Other';
}

async function fetchAndStorePlaidTransactions(accessToken, userId, itemId) {
  const household = getHousehold(userId);
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
  all('SELECT vendor, category, type FROM vendor_rules WHERE household = ?', [household])
    .forEach(r => { vendorRules[r.vendor] = { cat: r.category, type: r.type }; });

  // ── UPSERT WITH THREE-TIER CATEGORIZATION ──
  const upsertTx = db.prepare(`INSERT OR REPLACE INTO transactions
    (id, household, user_id, desc, amount, type, cat, date, pending, account_id, source, original_cat, reviewed)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'plaid', ?, ?)`);
  const upsertAcct = db.prepare(`INSERT OR REPLACE INTO accounts
    (account_id, user_id, household, name, mask, type, subtype, balance, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))`);

  const insertMany = db.transaction(() => {
    for (const t of allTransactions) {
      const rule = vendorRules[t.name];
      let cat, type, reviewed;

      if (rule) {
        // TIER 1: Vendor rule — highest priority
        cat = rule.cat;
        type = rule.type;
        reviewed = 1;
      } else {
        // TIER 2: Check if this transaction was manually reviewed
        const existing = get('SELECT cat, type, reviewed FROM transactions WHERE id = ?', [t.transaction_id]);
        if (existing && existing.reviewed) {
          cat = existing.cat;
          type = existing.type;
          reviewed = 1;
        } else {
          // TIER 3: Plaid categorization
          cat = mapCategory(t.personal_finance_category?.primary || t.category?.[0]);
          const desc = (t.name || '').toUpperCase();
          if (cat !== 'Transfer' && (
            desc.includes('TRANSFER') || (desc.includes('CREDIT CARD') && desc.includes('PAYMENT')) ||
            desc.includes('CD DEPOSIT') || (desc.includes('SAVINGS') && desc.includes('WITHDRAWAL')) ||
            desc.includes('AUTOMATIC PAYMENT') || desc.includes('AUTOPAY') ||
            desc.includes('PAYMENT - THANK')
          )) cat = 'Transfer';
          // NO REFUND TYPE — only income, expense, transfer
          type = cat === 'Transfer' ? 'transfer' : (t.amount > 0 ? 'expense' : 'income');
          reviewed = 0;
        }
      }

      const originalCat = t.personal_finance_category?.primary || t.category?.[0] || null;

      upsertTx.run(
        t.transaction_id, household, userId, t.name,
        Math.abs(t.amount), type, cat,
        t.date, t.pending ? 1 : 0, t.account_id,
        originalCat, reviewed
      );
    }
    for (const a of allAccounts) {
      upsertAcct.run(
        a.account_id, userId, household,
        a.name, a.mask, a.type, a.subtype, a.balances.current
      );
    }

    // Remove duplicate pending transactions (posted version exists)
    const dupes = all(`
      SELECT p.id as pending_id FROM transactions p
      INNER JOIN transactions posted ON p.desc = posted.desc AND p.amount = posted.amount
        AND p.date = posted.date AND p.account_id = posted.account_id AND p.household = posted.household
      WHERE p.household = ? AND p.pending = 1 AND posted.pending = 0 AND p.id != posted.id`, [household]);
    for (const d of dupes) run('DELETE FROM transactions WHERE id = ?', [d.pending_id]);
    if (dupes.length) console.log(`  Removed ${dupes.length} duplicate pending transactions`);
  });
  insertMany();
  return { count: allTransactions.length, accounts: allAccounts.length };
}

// ─── ROUTES ──────────────────────────────────────────────────────
app.get('/api/users', (req, res) => {
  try { res.json({ users: all('SELECT id, name, household FROM users') }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/create_link_token', async (req, res) => {
  try {
    const linkConfig = {
      user: { client_user_id: req.body.userId || 'christian' },
      client_name: 'Ledger — Household Finance',
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
    const resp = await plaid.itemPublicTokenExchange({ public_token });
    const { access_token, item_id } = resp.data;
    run(`INSERT OR REPLACE INTO plaid_items (item_id, user_id, access_token, institution)
      VALUES (?, ?, ?, ?)`, [item_id, userId, access_token, institution || 'Unknown']);
    console.log(`✓ Bank connected: user=${userId}, item=${item_id}`);
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
        const hh = getHousehold(userId);
        txs = all(`SELECT t.*, u.name as user_name FROM transactions t
          LEFT JOIN users u ON t.user_id = u.id
          WHERE t.household = ? AND t.date >= ? AND t.date <= ?
          ORDER BY t.date DESC, t.created_at DESC`, [hh, startDate, endDate]);
      } else {
        txs = all(`SELECT * FROM transactions WHERE user_id = ? AND date >= ? AND date <= ?
          ORDER BY date DESC, created_at DESC`, [userId, startDate, endDate]);
      }
    } else {
      const cutoff = new Date(); cutoff.setDate(cutoff.getDate() - parseInt(days));
      const cutoffStr = cutoff.toISOString().split('T')[0];
      if (hhParam === 'true') {
        const hh = getHousehold(userId);
        txs = all(`SELECT t.*, u.name as user_name FROM transactions t
          LEFT JOIN users u ON t.user_id = u.id
          WHERE t.household = ? AND t.date >= ?
          ORDER BY t.date DESC, t.created_at DESC`, [hh, cutoffStr]);
      } else {
        txs = all(`SELECT * FROM transactions WHERE user_id = ? AND date >= ?
          ORDER BY date DESC, created_at DESC`, [userId, cutoffStr]);
      }
    }
    res.json({ transactions: txs, total: txs.length });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/accounts', (req, res) => {
  try {
    const hh = getHousehold(req.query.userId || 'christian');
    const accounts = all(`SELECT a.*, u.name as user_name FROM accounts a
      LEFT JOIN users u ON a.user_id = u.id
      WHERE a.household = ? ORDER BY a.type, a.name`, [hh]);
    res.json({ accounts });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/sync', async (req, res) => {
  const userId = req.body.userId || 'christian';
  try {
    const household = getHousehold(userId);
    const members = all('SELECT id FROM users WHERE household = ?', [household]);
    let totalTx = 0;
    for (const member of members) {
      const items = all('SELECT * FROM plaid_items WHERE user_id = ?', [member.id]);
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

app.post('/api/transactions', (req, res) => {
  const { userId = 'christian', desc, amount, type, cat, date } = req.body;
  if (!desc || !amount || !type || !cat || !date)
    return res.status(400).json({ error: 'Missing required fields' });
  try {
    const household = getHousehold(userId);
    const id = 'manual_' + Date.now() + '_' + Math.random().toString(36).slice(2);
    run(`INSERT INTO transactions (id, household, user_id, desc, amount, type, cat, date, source)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'manual')`,
      [id, household, userId, desc, parseFloat(amount), type, cat, date]);
    res.json({ success: true, id });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── TRANSACTION DETAIL ─────────────────────────────────────────
// Static routes MUST come before parameterized /:id routes
app.get('/api/transactions/unsure', (req, res) => {
  try {
    const hh = getHousehold(req.query.userId || 'christian');
    const txs = all(`SELECT t.*, u.name as user_name FROM transactions t
      LEFT JOIN users u ON t.user_id=u.id
      WHERE t.household=? AND t.status='unsure' ORDER BY t.date DESC`, [hh]);
    res.json({ transactions: txs });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/transactions/:id/status', (req, res) => {
  const { status } = req.body;
  if (!status) return res.status(400).json({ error: 'Missing status' });
  try {
    run('UPDATE transactions SET status=? WHERE id=?', [status, req.params.id]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/transactions/:id', (req, res) => {
  try {
    const tx = get(`SELECT t.*, u.name as user_name FROM transactions t
      LEFT JOIN users u ON t.user_id = u.id WHERE t.id = ?`, [req.params.id]);
    if (!tx) return res.status(404).json({ error: 'Transaction not found' });
    const tags = all('SELECT tag FROM transaction_tags WHERE transaction_id = ?', [req.params.id]).map(r => r.tag);
    const recurring = tx.recurring_group_id ? get('SELECT * FROM recurring_rules WHERE id = ?', [tx.recurring_group_id]) : null;
    res.json({ ...tx, tags, recurring });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/transactions/:id', (req, res) => {
  const { category, type, notes, is_recurring, applyToVendor, original_sign, status } = req.body;
  try {
    const tx = get('SELECT * FROM transactions WHERE id = ?', [req.params.id]);
    if (!tx) return res.status(404).json({ error: 'Transaction not found' });
    const newCat = category || tx.cat;
    const newType = type || tx.type;
    const newNotes = notes !== undefined ? notes : tx.notes;
    const newRecurring = is_recurring !== undefined ? (is_recurring ? 1 : 0) : tx.is_recurring;
    const newSign = original_sign !== undefined ? original_sign : tx.original_sign;
    const newStatus = status || tx.status || 'confirmed';
    run('UPDATE transactions SET cat=?, type=?, notes=?, is_recurring=?, original_sign=?, status=?, reviewed=1 WHERE id=?',
      [newCat, newType, newNotes, newRecurring, newSign, newStatus, req.params.id]);
    let updatedCount = 1;
    if (applyToVendor) {
      // Vendor rule stores ONLY category
      run(`INSERT OR REPLACE INTO vendor_rules (household, vendor, category, type, updated_at) VALUES (?,?,?,'expense',datetime('now'))`,
        [tx.household, tx.desc, newCat]);
      // Apply category only — preserve each tx's type and sign
      const result = run('UPDATE transactions SET cat=?, reviewed=1 WHERE household=? AND desc=?',
        [newCat, tx.household, tx.desc]);
      updatedCount = result.changes || 1;
    }
    res.json({ success: true, updated: updatedCount });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Legacy endpoint (frontend compat)
app.put('/api/transactions/:id/category', (req, res) => {
  const { category, type, applyToVendor, original_sign } = req.body;
  if (!category && !type && original_sign === undefined) return res.status(400).json({ error: 'Missing category, type, or sign' });
  try {
    const tx = get('SELECT * FROM transactions WHERE id = ?', [req.params.id]);
    if (!tx) return res.status(404).json({ error: 'Transaction not found' });
    const newCat = category || tx.cat;
    const newType = type || tx.type;
    const newSign = original_sign !== undefined ? original_sign : tx.original_sign;
    // Always update this specific transaction (type + sign + category)
    run('UPDATE transactions SET cat=?, type=?, original_sign=?, reviewed=1 WHERE id=?', [newCat, newType, newSign, req.params.id]);
    let updatedCount = 1;
    if (applyToVendor) {
      // Vendor rule stores ONLY category — not type, not sign
      run(`INSERT OR REPLACE INTO vendor_rules (household, vendor, category, type, updated_at) VALUES (?,?,?,'expense',datetime('now'))`,
        [tx.household, tx.desc, newCat]);
      // Apply category to all matching, but PRESERVE their type and sign
      const result = run('UPDATE transactions SET cat=?, reviewed=1 WHERE household=? AND desc=?',
        [newCat, tx.household, tx.desc]);
      updatedCount = result.changes || 1;
    }
    res.json({ success: true, updated: updatedCount });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/transactions/:id/tags', (req, res) => {
  const { tag } = req.body;
  if (!tag) return res.status(400).json({ error: 'Missing tag' });
  try {
    const tx = get('SELECT household FROM transactions WHERE id = ?', [req.params.id]);
    if (!tx) return res.status(404).json({ error: 'Transaction not found' });
    run('INSERT OR IGNORE INTO transaction_tags (transaction_id, tag, household) VALUES (?,?,?)',
      [req.params.id, tag.trim(), tx.household]);
    run('INSERT OR IGNORE INTO tags (name, household) VALUES (?,?)', [tag.trim(), tx.household]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/transactions/:id/tags/:tag', (req, res) => {
  try {
    run('DELETE FROM transaction_tags WHERE transaction_id=? AND tag=?', [req.params.id, req.params.tag]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/transactions/:id', (req, res) => {
  try {
    run('DELETE FROM transaction_tags WHERE transaction_id = ?', [req.params.id]);
    run('DELETE FROM transactions WHERE id = ?', [req.params.id]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── CATEGORY MANAGEMENT ────────────────────────────────────────
app.get('/api/categories', (req, res) => {
  try {
    const hh = getHousehold(req.query.userId || 'christian');
    const cats = all(`SELECT c.*, COALESCE(s.total,0) as spent, COALESCE(s.cnt,0) as tx_count
      FROM categories c LEFT JOIN (
        SELECT cat, SUM(amount) as total, COUNT(*) as cnt FROM transactions
        WHERE household=? AND type='expense' GROUP BY cat
      ) s ON c.id = s.cat
      WHERE c.household=? AND c.is_active=1 ORDER BY c.sort_order`, [hh, hh]);
    res.json({ categories: cats });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/categories', (req, res) => {
  const { userId = 'christian', name, icon, color, type = 'expense' } = req.body;
  if (!name) return res.status(400).json({ error: 'Missing name' });
  try {
    const hh = getHousehold(userId);
    const maxOrder = get('SELECT MAX(sort_order) as m FROM categories WHERE household=?', [hh]);
    run('INSERT OR IGNORE INTO categories (id,household,icon,color,type,sort_order) VALUES (?,?,?,?,?,?)',
      [name, hh, icon || '📌', color || '#7a78a0', type, (maxOrder?.m || 0) + 1]);
    // Also sync to category_icons for backward compat
    if (icon) run('INSERT OR REPLACE INTO category_icons (household,category,icon) VALUES (?,?,?)', [hh, name, icon]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/categories/:id', (req, res) => {
  const { userId = 'christian', name, icon, color, sort_order, type } = req.body;
  try {
    const hh = getHousehold(userId);
    const cat = get('SELECT * FROM categories WHERE household=? AND id=?', [hh, req.params.id]);
    if (!cat) return res.status(404).json({ error: 'Category not found' });
    const updates = [];
    const params = [];
    if (icon !== undefined) { updates.push('icon=?'); params.push(icon); }
    if (color !== undefined) { updates.push('color=?'); params.push(color); }
    if (sort_order !== undefined) { updates.push('sort_order=?'); params.push(sort_order); }
    if (type !== undefined) { updates.push('type=?'); params.push(type); }
    if (name && name !== req.params.id) {
      // Rename: update all references
      run('UPDATE transactions SET cat=? WHERE household=? AND cat=?', [name, hh, req.params.id]);
      run('UPDATE budgets SET category=? WHERE household=? AND category=?', [name, hh, req.params.id]);
      run('UPDATE vendor_rules SET category=? WHERE household=? AND category=?', [name, hh, req.params.id]);
      run('DELETE FROM categories WHERE household=? AND id=?', [hh, req.params.id]);
      run('INSERT OR REPLACE INTO categories (id,household,icon,color,type,sort_order,is_active) VALUES (?,?,?,?,?,?,1)',
        [name, hh, icon||cat.icon, color||cat.color, type||cat.type, sort_order!==undefined?sort_order:cat.sort_order]);
    } else if (updates.length) {
      params.push(hh, req.params.id);
      run(`UPDATE categories SET ${updates.join(',')} WHERE household=? AND id=?`, params);
    }
    // Sync icon to category_icons
    const finalIcon = icon || cat.icon;
    if (finalIcon) run('INSERT OR REPLACE INTO category_icons (household,category,icon) VALUES (?,?,?)', [hh, name || req.params.id, finalIcon]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/categories/:id', (req, res) => {
  const { userId = 'christian' } = req.query;
  try {
    const hh = getHousehold(userId);
    run('UPDATE transactions SET cat=? WHERE household=? AND cat=?', ['Other', hh, req.params.id]);
    run('UPDATE categories SET is_active=0 WHERE household=? AND id=?', [hh, req.params.id]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/categories/:id/transactions', (req, res) => {
  const { userId = 'christian', group_by, startDate, endDate } = req.query;
  try {
    const hh = getHousehold(userId);
    let txs;
    if (startDate && endDate) {
      txs = all(`SELECT t.*, u.name as user_name FROM transactions t
        LEFT JOIN users u ON t.user_id=u.id
        WHERE t.household=? AND t.cat=? AND t.date>=? AND t.date<=?
        ORDER BY t.date DESC`, [hh, req.params.id, startDate, endDate]);
    } else {
      txs = all(`SELECT t.*, u.name as user_name FROM transactions t
        LEFT JOIN users u ON t.user_id=u.id
        WHERE t.household=? AND t.cat=?
        ORDER BY t.date DESC LIMIT 200`, [hh, req.params.id]);
    }
    if (group_by === 'vendor') {
      const groups = {};
      txs.forEach(t => {
        if (!groups[t.desc]) groups[t.desc] = { vendor: t.desc, total: 0, count: 0, transactions: [] };
        groups[t.desc].total += t.amount;
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
app.get('/api/vendor-rules', (req, res) => {
  try {
    const hh = getHousehold(req.query.userId || 'christian');
    const rules = all('SELECT vendor, category, type FROM vendor_rules WHERE household = ?', [hh]);
    res.json({ rules });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Legacy category-icons endpoints (kept for backward compat)
app.get('/api/category-icons', (req, res) => {
  try {
    const hh = getHousehold(req.query.userId || 'christian');
    // Serve from categories table now
    const icons = all('SELECT id as category, icon FROM categories WHERE household=? AND is_active=1', [hh]);
    res.json({ icons });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/category-icons', (req, res) => {
  const { userId = 'christian', category, icon } = req.body;
  if (!category || !icon) return res.status(400).json({ error: 'Missing category or icon' });
  try {
    const hh = getHousehold(userId);
    run('INSERT OR REPLACE INTO category_icons (household, category, icon) VALUES (?, ?, ?)', [hh, category, icon]);
    // Also update categories table
    run('UPDATE categories SET icon=? WHERE household=? AND id=?', [icon, hh, category]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── RECURRING & SUBSCRIPTIONS ──────────────────────────────────
app.get('/api/recurring', (req, res) => {
  try {
    const hh = getHousehold(req.query.userId || 'christian');
    const rules = all('SELECT * FROM recurring_rules WHERE household=? AND is_active=1 ORDER BY last_seen DESC', [hh]);
    res.json({ recurring: rules });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/recurring', (req, res) => {
  const { userId = 'christian', vendor, category, expected_amount, frequency = 'monthly', is_subscription = 0 } = req.body;
  if (!vendor) return res.status(400).json({ error: 'Missing vendor' });
  try {
    const hh = getHousehold(userId);
    // Check if rule already exists for this vendor
    const existing = get('SELECT id FROM recurring_rules WHERE household=? AND vendor=? AND is_active=1', [hh, vendor]);
    if (existing) return res.json({ success: true, id: existing.id, existing: true });
    const id = 'rec_' + Date.now() + '_' + Math.random().toString(36).slice(2);
    run(`INSERT INTO recurring_rules (id,household,vendor,category,expected_amount,frequency,is_subscription,last_seen)
      VALUES (?,?,?,?,?,?,?,date('now'))`, [id, hh, vendor, category, expected_amount, frequency, is_subscription ? 1 : 0]);
    run('UPDATE transactions SET is_recurring=1, recurring_group_id=? WHERE household=? AND desc=?', [id, hh, vendor]);
    res.json({ success: true, id });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/recurring/:id', (req, res) => {
  const { frequency, expected_amount, is_active, is_subscription } = req.body;
  try {
    const updates = [], params = [];
    if (frequency !== undefined) { updates.push('frequency=?'); params.push(frequency); }
    if (expected_amount !== undefined) { updates.push('expected_amount=?'); params.push(expected_amount); }
    if (is_active !== undefined) { updates.push('is_active=?'); params.push(is_active ? 1 : 0); }
    if (is_subscription !== undefined) { updates.push('is_subscription=?'); params.push(is_subscription ? 1 : 0); }
    if (!updates.length) return res.json({ success: true });
    params.push(req.params.id);
    run(`UPDATE recurring_rules SET ${updates.join(',')} WHERE id=?`, params);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/recurring/detect', (req, res) => {
  const { userId = 'christian' } = req.body;
  try {
    const hh = getHousehold(userId);
    // Group transactions by vendor, analyze date patterns
    const vendors = all(`SELECT desc as vendor, GROUP_CONCAT(date) as dates, GROUP_CONCAT(amount) as amounts,
      COUNT(*) as cnt, AVG(amount) as avg_amt, cat
      FROM transactions WHERE household=? AND type='expense'
      GROUP BY desc HAVING cnt >= 3 ORDER BY cnt DESC`, [hh]);

    const suggestions = [];
    for (const v of vendors) {
      // Check if already a rule
      const existing = get('SELECT id FROM recurring_rules WHERE household=? AND vendor=?', [hh, v.vendor]);
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
        vendor: v.vendor, category: v.cat, expected_amount: v.avg_amt,
        frequency, is_subscription, tx_count: v.cnt,
        last_seen: dates[dates.length-1].toISOString().split('T')[0]
      });
    }
    res.json({ suggestions });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/subscriptions', (req, res) => {
  try {
    const hh = getHousehold(req.query.userId || 'christian');
    const subs = all('SELECT * FROM recurring_rules WHERE household=? AND is_subscription=1 AND is_active=1', [hh]);
    const monthlyTotal = subs.reduce((a, s) => {
      const mult = s.frequency === 'weekly' ? 4.33 : s.frequency === 'quarterly' ? 1/3 : s.frequency === 'annual' ? 1/12 : 1;
      return a + (s.expected_amount || 0) * mult;
    }, 0);
    res.json({ subscriptions: subs, monthly_total: monthlyTotal });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/recurring/dedup', (req, res) => {
  const { userId = 'christian' } = req.body;
  try {
    const hh = getHousehold(userId);
    // Keep the earliest rule per vendor, delete the rest
    const dupes = all(`SELECT id FROM recurring_rules WHERE household=? AND id NOT IN (
      SELECT MIN(id) FROM recurring_rules WHERE household=? GROUP BY vendor
    )`, [hh, hh]);
    for (const d of dupes) run('DELETE FROM recurring_rules WHERE id=?', [d.id]);
    res.json({ success: true, removed: dupes.length });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/recurring/dismiss', (req, res) => {
  const { userId = 'christian', vendor } = req.body;
  if (!vendor) return res.status(400).json({ error: 'Missing vendor' });
  try {
    const hh = getHousehold(userId);
    const id = 'rec_dismissed_' + Date.now() + '_' + Math.random().toString(36).slice(2);
    run(`INSERT OR REPLACE INTO recurring_rules (id,household,vendor,is_active,frequency) VALUES (?,?,?,0,'dismissed')`,
      [id, hh, vendor]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── ANALYTICS & TRENDS ─────────────────────────────────────────
app.get('/api/trends/categories', (req, res) => {
  const { userId = 'christian', months = 6 } = req.query;
  try {
    const hh = getHousehold(userId);
    const data = all(`SELECT cat, strftime('%Y-%m', date) as month, SUM(amount) as total
      FROM transactions WHERE household=? AND type='expense'
        AND date >= date('now', '-' || ? || ' months')
      GROUP BY cat, month ORDER BY month, total DESC`, [hh, months]);
    // Compute MoM change per category
    const byCategory = {};
    data.forEach(r => {
      if (!byCategory[r.cat]) byCategory[r.cat] = [];
      byCategory[r.cat].push({ month: r.month, amount: r.total });
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

app.get('/api/trends/vendors', (req, res) => {
  const { userId = 'christian', vendor, months = 6 } = req.query;
  try {
    const hh = getHousehold(userId);
    const where = vendor ? 'AND desc=?' : '';
    const params = vendor ? [hh, months, vendor] : [hh, months];
    const data = all(`SELECT desc as vendor, strftime('%Y-%m', date) as month, SUM(amount) as total, COUNT(*) as cnt
      FROM transactions WHERE household=? AND type='expense'
        AND date >= date('now', '-' || ? || ' months') ${where}
      GROUP BY desc, month ORDER BY month`, params);
    res.json({ data });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/trends/cashflow', (req, res) => {
  const { userId = 'christian', months = '6' } = req.query;
  try {
    const hh = getHousehold(userId);
    const numMonths = parseInt(months) || 6;
    const now = new Date();
    const data = [];
    for (let i = numMonths - 1; i >= 0; i--) {
      const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
      const start = d.toISOString().split('T')[0];
      const end = new Date(d.getFullYear(), d.getMonth() + 1, 0).toISOString().split('T')[0];
      const month = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
      const txs = all('SELECT type, SUM(amount) as total FROM transactions WHERE household = ? AND date >= ? AND date <= ? GROUP BY type', [hh, start, end]);
      const income = txs.find(t => t.type === 'income')?.total || 0;
      const expenses = txs.find(t => t.type === 'expense')?.total || 0;
      data.push({ month, income, expenses, net: income - expenses });
    }
    res.json({ data });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/trends/daily-average', (req, res) => {
  const { userId = 'christian', month } = req.query;
  try {
    const hh = getHousehold(userId);
    const now = new Date();
    const targetMonth = month || `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
    const [y, m] = targetMonth.split('-').map(Number);
    const daysInMonth = new Date(y, m, 0).getDate();
    const isCurrentMonth = y === now.getFullYear() && m === now.getMonth()+1;
    const daysElapsed = isCurrentMonth ? now.getDate() : daysInMonth;

    const result = get(`SELECT SUM(amount) as total FROM transactions
      WHERE household=? AND type='expense' AND strftime('%Y-%m', date)=?`, [hh, targetMonth]);
    const total = result?.total || 0;
    const dailyAvg = daysElapsed > 0 ? total / daysElapsed : 0;

    // Previous month for comparison
    const prevDate = new Date(y, m - 2, 1);
    const prevMonth = `${prevDate.getFullYear()}-${String(prevDate.getMonth()+1).padStart(2,'0')}`;
    const prevDays = new Date(prevDate.getFullYear(), prevDate.getMonth()+1, 0).getDate();
    const prevResult = get(`SELECT SUM(amount) as total FROM transactions
      WHERE household=? AND type='expense' AND strftime('%Y-%m', date)=?`, [hh, prevMonth]);
    const prevDailyAvg = prevDays > 0 ? (prevResult?.total || 0) / prevDays : 0;

    res.json({ daily_average: dailyAvg, total, days: daysElapsed,
      prev_daily_average: prevDailyAvg, change_pct: prevDailyAvg > 0 ? ((dailyAvg - prevDailyAvg)/prevDailyAvg*100) : null });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/income/breakdown', (req, res) => {
  const { userId = 'christian', startDate, endDate } = req.query;
  try {
    const hh = getHousehold(userId);
    const dateFilter = startDate && endDate ? 'AND date>=? AND date<=?' : "AND date >= date('now', '-12 months')";
    const params = startDate && endDate ? [hh, startDate, endDate] : [hh];
    const byCategory = all(`SELECT cat as category, SUM(amount) as total, COUNT(*) as cnt
      FROM transactions WHERE household=? AND type='income' ${dateFilter}
      GROUP BY cat ORDER BY total DESC`, params);
    const byMerchant = all(`SELECT desc as merchant, SUM(amount) as total, COUNT(*) as cnt, cat
      FROM transactions WHERE household=? AND type='income' ${dateFilter}
      GROUP BY desc ORDER BY total DESC`, params);
    res.json({ by_category: byCategory, by_merchant: byMerchant });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── REVIEW QUEUE & WIZARD ──────────────────────────────────────
app.get('/api/review-queue', (req, res) => {
  try {
    const hh = getHousehold(req.query.userId || 'christian');
    const vendorRows = all(`SELECT desc as vendor, COUNT(*) as cnt, SUM(amount) as total,
      GROUP_CONCAT(DISTINCT cat) as current_cats, MIN(date) as first_seen, MAX(date) as last_seen
      FROM transactions WHERE household=? AND cat='Other' AND reviewed=0
      GROUP BY desc ORDER BY total DESC`, [hh]);
    // Include individual transactions per vendor
    const vendors = vendorRows.map(v => {
      const txs = all(`SELECT id, date, desc, amount, type FROM transactions
        WHERE household=? AND desc=? AND cat='Other' AND reviewed=0
        ORDER BY date DESC LIMIT 20`, [hh, v.vendor]);
      return { ...v, transactions: txs };
    });
    res.json({ queue: vendors, total_unreviewed: vendors.reduce((a,v) => a + v.cnt, 0) });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/review-queue/resolve', (req, res) => {
  const { userId = 'christian', vendor, category, type = 'expense' } = req.body;
  if (!vendor || !category) return res.status(400).json({ error: 'Missing vendor or category' });
  try {
    const hh = getHousehold(userId);
    run(`INSERT OR REPLACE INTO vendor_rules (household, vendor, category, type) VALUES (?,?,?,?)`,
      [hh, vendor, category, type]);
    const result = run('UPDATE transactions SET cat=?, type=?, reviewed=1 WHERE household=? AND desc=?',
      [category, type, hh, vendor]);
    res.json({ success: true, updated: result.changes });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/wizard/uncategorized-vendors', (req, res) => {
  try {
    const hh = getHousehold(req.query.userId || 'christian');
    const vendors = all(`SELECT t.desc as vendor, COUNT(*) as cnt, SUM(t.amount) as total, t.cat as current_cat
      FROM transactions t
      LEFT JOIN vendor_rules vr ON vr.household=t.household AND vr.vendor=t.desc
      WHERE t.household=? AND vr.vendor IS NULL
      GROUP BY t.desc ORDER BY total DESC LIMIT 25`, [hh]);
    res.json({ vendors });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/wizard/bulk-assign', (req, res) => {
  const { userId = 'christian', assignments } = req.body;
  if (!assignments || !assignments.length) return res.status(400).json({ error: 'Missing assignments' });
  try {
    const hh = getHousehold(userId);
    let totalUpdated = 0;
    const assignTx = db.transaction(() => {
      for (const { vendor, category, type = 'expense' } of assignments) {
        run('INSERT OR REPLACE INTO vendor_rules (household,vendor,category,type) VALUES (?,?,?,?)',
          [hh, vendor, category, type]);
        const r = run('UPDATE transactions SET cat=?, type=?, reviewed=1 WHERE household=? AND desc=?',
          [category, type, hh, vendor]);
        totalUpdated += r.changes || 0;
      }
    });
    assignTx();
    res.json({ success: true, updated: totalUpdated });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/status', (req, res) => {
  try {
    const household = getHousehold(req.query.userId || 'christian');
    const items = all(`SELECT pi.item_id, pi.user_id, pi.institution
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
      const item = get('SELECT * FROM plaid_items WHERE item_id = ?', [item_id]);
      if (item) await fetchAndStorePlaidTransactions(item.access_token, item.user_id, item_id);
    } catch (err) { console.error('Webhook error:', err.message); }
  }
});

// ─── BUDGETS ──────────────────────────────────────────────────────
app.get('/api/budgets', (req, res) => {
  try {
    const hh = getHousehold(req.query.userId || 'christian');
    const rows = all('SELECT category, amount FROM budgets WHERE household = ? ORDER BY category', [hh]);
    res.json({ budgets: rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/budgets', (req, res) => {
  const { userId = 'christian', category, amount } = req.body;
  if (!category || amount == null) return res.status(400).json({ error: 'Missing category or amount' });
  try {
    const hh = getHousehold(userId);
    run(`INSERT OR REPLACE INTO budgets (household, category, amount, updated_at) VALUES (?, ?, ?, datetime('now'))`, [hh, category, parseFloat(amount)]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/budgets', (req, res) => {
  const { userId = 'christian', category } = req.body;
  if (!category) return res.status(400).json({ error: 'Missing category' });
  try {
    const hh = getHousehold(userId);
    run('DELETE FROM budgets WHERE household = ? AND category = ?', [hh, category]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── VENDOR SPENDING ─────────────────────────────────────────────
app.get('/api/spending/by-vendor', (req, res) => {
  const { userId = 'christian', startDate, endDate } = req.query;
  try {
    const hh = getHousehold(userId);
    const rows = all(`SELECT desc as vendor, SUM(amount) as total, COUNT(*) as count
      FROM transactions
      WHERE household = ? AND type = 'expense' AND date >= ? AND date <= ?
      GROUP BY desc ORDER BY total DESC LIMIT 20`, [hh, startDate, endDate]);
    res.json({ vendors: rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── AI CHAT ─────────────────────────────────────────────────────
const anthropic = process.env.ANTHROPIC_API_KEY ? new Anthropic() : null;

app.post('/api/chat', async (req, res) => {
  if (!anthropic) return res.status(500).json({ error: 'ANTHROPIC_API_KEY not configured' });
  const { userId = 'christian', message, history = [] } = req.body;
  if (!message) return res.status(400).json({ error: 'Missing message' });

  try {
    const hh = getHousehold(userId);
    const now = new Date();
    const monthStart = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-01`;
    const lastDay = new Date(now.getFullYear(), now.getMonth()+1, 0).getDate();
    const monthEnd = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-${String(lastDay).padStart(2,'0')}`;

    // Gather financial context
    const monthTxs = all(`SELECT desc, amount, type, cat, date FROM transactions
      WHERE household = ? AND date >= ? AND date <= ? ORDER BY date DESC`, [hh, monthStart, monthEnd]);
    const income = monthTxs.filter(t=>t.type==='income').reduce((a,t)=>a+t.amount,0);
    const expenses = monthTxs.filter(t=>t.type==='expense').reduce((a,t)=>a+t.amount,0);
    const catSpend = {};
    monthTxs.filter(t=>t.type==='expense').forEach(t=>{ catSpend[t.cat]=(catSpend[t.cat]||0)+t.amount; });
    const vendorSpend = {};
    monthTxs.filter(t=>t.type==='expense').forEach(t=>{ vendorSpend[t.desc]=(vendorSpend[t.desc]||0)+t.amount; });
    const topVendors = Object.entries(vendorSpend).sort((a,b)=>b[1]-a[1]).slice(0,15);
    const budgetRows = all('SELECT category, amount FROM budgets WHERE household = ?', [hh]);
    const accts = all(`SELECT a.name, a.type, a.subtype, a.balance FROM accounts a
      JOIN users u ON a.user_id = u.id WHERE u.household = ?`, [hh]);
    const recentTxs = monthTxs.slice(0, 30);

    const context = `
## Financial Data for ${now.toLocaleString('default',{month:'long',year:'numeric'})}

### Summary
- Income: $${income.toFixed(2)}
- Expenses: $${expenses.toFixed(2)}
- Net: $${(income-expenses).toFixed(2)}
- Savings rate: ${income>0?((income-expenses)/income*100).toFixed(0):'0'}%
- Total transactions: ${monthTxs.length}

### Spending by Category
${Object.entries(catSpend).sort((a,b)=>b[1]-a[1]).map(([c,a])=>`- ${c}: $${a.toFixed(2)}`).join('\n')}

### Budgets vs Actual
${budgetRows.map(b=>{const spent=catSpend[b.category]||0;return `- ${b.category}: $${spent.toFixed(2)} / $${b.amount.toFixed(2)} (${(spent/b.amount*100).toFixed(0)}%)`;}).join('\n')}

### Top Vendors
${topVendors.map(([v,a])=>`- ${v}: $${a.toFixed(2)}`).join('\n')}

### Accounts
${accts.length?accts.map(a=>`- ${a.name} (${a.type}/${a.subtype}): $${(a.balance||0).toFixed(2)}`).join('\n'):'No linked accounts'}

### Recent Transactions (last 30)
${recentTxs.map(t=>`- ${t.date} | ${t.type} | ${t.cat} | ${t.desc} | $${t.amount.toFixed(2)}`).join('\n')}
`;

    const systemPrompt = `You are a helpful personal finance assistant for the Spenziero household. You have access to their real financial data below. Give concise, actionable advice. Use specific numbers from their data. Be conversational but brief.

${context}

When answering:
- Reference actual numbers and transactions
- Be specific about where money is going
- If asked about budgets, compare actual vs budgeted
- Keep responses under 200 words unless detail is needed
- Use $ formatting for amounts`;

    const messages = [...history.slice(-10), { role: 'user', content: message }];

    const response = await anthropic.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 1024,
      system: systemPrompt,
      messages,
    });

    res.json({ reply: response.content[0].text });
  } catch (e) {
    console.error('Chat error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─── MAINTENANCE ────────────────────────────────────────────────
app.post('/api/maintenance/dedup', (req, res) => {
  const { userId = 'christian' } = req.body;
  try {
    const hh = getHousehold(userId);
    const dupes = all(`SELECT p.id as pending_id FROM transactions p
      INNER JOIN transactions posted ON p.desc = posted.desc AND p.amount = posted.amount
        AND p.date = posted.date AND p.account_id = posted.account_id AND p.household = posted.household
      WHERE p.household = ? AND p.pending = 1 AND posted.pending = 0 AND p.id != posted.id`, [hh]);
    for (const d of dupes) run('DELETE FROM transactions WHERE id = ?', [d.pending_id]);
    res.json({ success: true, removed: dupes.length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── VENDOR SUMMARY + BULK RULES ────────────────────────────────
app.get('/api/vendor-summary', (req, res) => {
  const { userId = 'christian' } = req.query;
  try {
    const hh = getHousehold(userId);
    const vendors = all(`
      SELECT t.desc as vendor, COUNT(*) as tx_count, SUM(t.amount) as total_spend,
        MIN(t.date) as first_seen, MAX(t.date) as last_seen, t.cat as plaid_category,
        t.type as inferred_type, vr.category as rule_category, vr.type as rule_type,
        CASE WHEN vr.vendor IS NOT NULL THEN 1 ELSE 0 END as has_rule
      FROM transactions t
      LEFT JOIN vendor_rules vr ON vr.vendor = t.desc AND vr.household = t.household
      WHERE t.household = ? GROUP BY t.desc ORDER BY SUM(t.amount) DESC`, [hh]);
    const withRules = vendors.filter(v => v.has_rule);
    res.json({
      vendors,
      stats: {
        total_vendors: vendors.length, categorized: withRules.length,
        needs_review: vendors.length - withRules.length,
        total_spend: vendors.reduce((a, v) => a + v.total_spend, 0),
        coverage_pct: vendors.length > 0 ? Math.round(withRules.length / vendors.length * 100) : 0
      }
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/vendor-rules/bulk', (req, res) => {
  const { userId = 'christian', assignments } = req.body;
  if (!assignments || !assignments.length) return res.status(400).json({ error: 'No assignments' });
  try {
    const hh = getHousehold(userId);
    const upsertRule = db.prepare(`INSERT OR REPLACE INTO vendor_rules (household, vendor, category, type, updated_at) VALUES (?, ?, ?, ?, datetime('now'))`);
    const updateTxs = db.prepare('UPDATE transactions SET cat = ?, type = ?, reviewed = 1 WHERE household = ? AND desc = ?');
    const ensureCat = db.prepare('INSERT OR IGNORE INTO categories (id, household) VALUES (?, ?)');
    const applyAll = db.transaction(() => {
      let totalUpdated = 0;
      for (const a of assignments) {
        upsertRule.run(hh, a.vendor, a.category, a.type || 'expense');
        const result = updateTxs.run(a.category, a.type || 'expense', hh, a.vendor);
        totalUpdated += result.changes;
        ensureCat.run(a.category, hh);
      }
      return totalUpdated;
    });
    const updated = applyAll();
    res.json({ success: true, rules_created: assignments.length, transactions_updated: updated });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── HEALTH SCORE ───────────────────────────────────────────────
app.get('/api/health-score', (req, res) => {
  const { userId = 'christian' } = req.query;
  try {
    const hh = getHousehold(userId);
    const now = new Date();
    const monthStart = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-01`;
    const monthEnd = new Date(now.getFullYear(), now.getMonth()+1, 0).toISOString().split('T')[0];
    const mtxs = all('SELECT * FROM transactions WHERE household = ? AND date >= ? AND date <= ?', [hh, monthStart, monthEnd]);
    const income = mtxs.filter(t => t.type === 'income').reduce((a,t) => a + t.amount, 0);
    const expenses = mtxs.filter(t => t.type === 'expense').reduce((a,t) => a + t.amount, 0);
    const savingsRate = income > 0 ? (income - expenses) / income : 0;
    const savingsScore = Math.min(30, Math.max(0, Math.round(savingsRate * 150)));
    const budgetRows = all('SELECT category, amount FROM budgets WHERE household = ?', [hh]);
    let budgetScore = 25;
    for (const b of budgetRows) {
      const spent = mtxs.filter(t => t.type === 'expense' && t.cat === b.category).reduce((a,t) => a + t.amount, 0);
      if (spent > b.amount) budgetScore -= Math.min(5, Math.round((spent - b.amount) / b.amount * 10));
    }
    budgetScore = Math.max(0, budgetScore);
    const totalVendors = all('SELECT COUNT(DISTINCT desc) as cnt FROM transactions WHERE household = ?', [hh])[0]?.cnt || 1;
    const ruledVendors = all('SELECT COUNT(*) as cnt FROM vendor_rules WHERE household = ?', [hh])[0]?.cnt || 0;
    const coverageScore = Math.round((ruledVendors / totalVendors) * 15);
    const recurringTotal = all('SELECT SUM(expected_amount) as total FROM recurring_rules WHERE household = ? AND is_active = 1', [hh])[0]?.total || 0;
    const recurringPct = income > 0 ? recurringTotal / income : 0;
    const recurringScore = recurringPct < 0.5 ? 15 : Math.max(0, 15 - Math.round((recurringPct - 0.5) * 30));
    const daySpends = {};
    mtxs.filter(t => t.type === 'expense').forEach(t => { daySpends[t.date] = (daySpends[t.date] || 0) + t.amount; });
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
app.post('/api/streak/check-in', (req, res) => {
  const { userId = 'christian' } = req.query;
  try {
    const user = get('SELECT * FROM users WHERE id = ?', [userId]);
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
    run('UPDATE users SET streak_count = ?, streak_best = ?, last_active_date = ? WHERE id = ?', [streak, best, today, userId]);
    res.json({ streak, best, is_new_day: user.last_active_date !== today, milestone: [7,30,100,365].includes(streak) });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── CHALLENGES ─────────────────────────────────────────────────
app.get('/api/challenges', (req, res) => {
  const { userId = 'christian' } = req.query;
  try {
    const hh = getHousehold(userId);
    const now = new Date();
    const currentMonth = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
    let challenges = all('SELECT * FROM challenges WHERE household = ? AND month = ?', [hh, currentMonth]);
    if (!challenges.length) {
      const prev = new Date(now.getFullYear(), now.getMonth() - 1, 1);
      const prevStart = prev.toISOString().split('T')[0];
      const prevEnd = new Date(prev.getFullYear(), prev.getMonth()+1, 0).toISOString().split('T')[0];
      const prevTxs = all('SELECT * FROM transactions WHERE household = ? AND date >= ? AND date <= ?', [hh, prevStart, prevEnd]);
      const prevExpenses = prevTxs.filter(t => t.type === 'expense');
      const prevTotal = prevExpenses.reduce((a,t) => a + t.amount, 0);
      const prevIncome = prevTxs.filter(t => t.type === 'income').reduce((a,t) => a + t.amount, 0);
      const daysInPrev = new Date(prev.getFullYear(), prev.getMonth()+1, 0).getDate();
      const prevDailyAvg = daysInPrev > 0 ? prevTotal / daysInPrev : 0;
      const uuid = () => 'ch_' + Date.now() + '_' + Math.random().toString(36).slice(2,8);
      const newChallenges = [];
      if (prevDailyAvg > 0) newChallenges.push({ id: uuid(), household: hh, month: currentMonth, title: `Daily avg under $${Math.round(prevDailyAvg)}`, description: `Beat last month's $${Math.round(prevDailyAvg)}/day average`, target_value: prevDailyAvg, challenge_type: 'daily_average' });
      if (prevIncome > 0) { const prevRate = (prevIncome - prevTotal) / prevIncome; const target = Math.min(0.30, prevRate + 0.05); newChallenges.push({ id: uuid(), household: hh, month: currentMonth, title: `Save ${Math.round(target * 100)}% of income`, description: `Improve from ${Math.round(prevRate * 100)}% last month`, target_value: target, challenge_type: 'savings_rate' }); }
      newChallenges.push({ id: uuid(), household: hh, month: currentMonth, title: 'Stay organized', description: '100% vendor coverage', target_value: 100, challenge_type: 'coverage' });
      const insert = db.prepare('INSERT INTO challenges (id, household, month, title, description, target_value, challenge_type, category) VALUES (?,?,?,?,?,?,?,?)');
      for (const c of newChallenges) insert.run(c.id, c.household, c.month, c.title, c.description, c.target_value, c.challenge_type, c.category || null);
      challenges = newChallenges;
    }
    const monthStart = `${currentMonth}-01`;
    const monthEnd = new Date(now.getFullYear(), now.getMonth()+1, 0).toISOString().split('T')[0];
    const currTxs = all('SELECT * FROM transactions WHERE household = ? AND date >= ? AND date <= ?', [hh, monthStart, monthEnd]);
    const currExpenses = currTxs.filter(t => t.type === 'expense');
    const currIncome = currTxs.filter(t => t.type === 'income').reduce((a,t) => a + t.amount, 0);
    const currTotal = currExpenses.reduce((a,t) => a + t.amount, 0);
    const daysElapsed = now.getDate();
    challenges = challenges.map(c => {
      let current = 0, progress = 0;
      switch(c.challenge_type) {
        case 'daily_average': current = daysElapsed > 0 ? currTotal / daysElapsed : 0; progress = c.target_value > 0 ? Math.min(100, Math.max(0, (1 - current / c.target_value) * 100)) : 0; break;
        case 'savings_rate': current = currIncome > 0 ? (currIncome - currTotal) / currIncome : 0; progress = c.target_value > 0 ? Math.min(100, (current / c.target_value) * 100) : 0; break;
        case 'coverage': const tv = all('SELECT COUNT(DISTINCT desc) as cnt FROM transactions WHERE household = ?', [hh])[0]?.cnt || 1; const rv = all('SELECT COUNT(*) as cnt FROM vendor_rules WHERE household = ?', [hh])[0]?.cnt || 0; current = Math.round(rv / tv * 100); progress = current; break;
      }
      return { ...c, current_value: current, progress: Math.max(0, progress), is_completed: progress >= 100 && daysElapsed >= 28 };
    });
    res.json({ challenges });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── WEEKLY RECAP ───────────────────────────────────────────────
app.get('/api/weekly-recap', (req, res) => {
  const { userId = 'christian' } = req.query;
  try {
    const hh = getHousehold(userId);
    const now = new Date();
    const weekAgo = new Date(now); weekAgo.setDate(weekAgo.getDate() - 7);
    const prevWeekStart = new Date(weekAgo); prevWeekStart.setDate(prevWeekStart.getDate() - 7);
    const thisWeek = all('SELECT * FROM transactions WHERE household = ? AND date >= ? AND date <= ?', [hh, weekAgo.toISOString().split('T')[0], now.toISOString().split('T')[0]]);
    const prevWeek = all('SELECT * FROM transactions WHERE household = ? AND date >= ? AND date <= ?', [hh, prevWeekStart.toISOString().split('T')[0], weekAgo.toISOString().split('T')[0]]);
    const thisExp = thisWeek.filter(t => t.type === 'expense');
    const prevExp = prevWeek.filter(t => t.type === 'expense');
    const thisTotal = thisExp.reduce((a,t) => a + t.amount, 0);
    const prevTotal = prevExp.reduce((a,t) => a + t.amount, 0);
    const change = prevTotal > 0 ? ((thisTotal - prevTotal) / prevTotal * 100) : 0;
    const biggest = thisExp.sort((a,b) => b.amount - a.amount)[0];
    const vendorCounts = {}; thisExp.forEach(t => { vendorCounts[t.desc] = (vendorCounts[t.desc]||0) + 1; });
    const topVendor = Object.entries(vendorCounts).sort((a,b) => b[1] - a[1])[0];
    res.json({ period: { start: weekAgo.toISOString().split('T')[0], end: now.toISOString().split('T')[0] }, total_spent: thisTotal, prev_total_spent: prevTotal, change_pct: change, transaction_count: thisWeek.length, biggest_purchase: biggest ? { vendor: biggest.desc, amount: biggest.amount, date: biggest.date } : null, most_frequent_vendor: topVendor ? { vendor: topVendor[0], count: topVendor[1] } : null });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Serve frontend
const frontendPath = path.join(__dirname, '../frontend');
app.use(express.static(frontendPath));
app.get('*', (req, res) => res.sendFile(path.join(frontendPath, 'index.html')));

// ─── START ────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3001;
initDB();
app.listen(PORT, () => {
  console.log(`\n✓ Ledger running → http://localhost:${PORT}`);
  console.log(`  Plaid env:  ${process.env.PLAID_ENV || 'sandbox'}`);
  console.log(`  Client ID:  ${process.env.PLAID_CLIENT_ID ? '✓ set' : '✗ MISSING'}`);
  console.log(`  Webhook:    ${process.env.WEBHOOK_URL || 'not set'}\n`);
});
