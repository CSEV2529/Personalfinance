/**
 * LEDGER — Backend Server
 * Express + better-sqlite3 + Plaid
 */

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const Database = require('better-sqlite3');
const path = require('path');
const { Configuration, PlaidApi, PlaidEnvironments, Products, CountryCode } = require('plaid');
const Anthropic = require('@anthropic-ai/sdk');

const app = express();
app.use(express.json());
app.use(cors({ origin: '*' }));

// ─── DATABASE ────────────────────────────────────────────────────
const dbPath = process.env.DB_PATH || path.join(__dirname, 'ledger.db');
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
    household TEXT NOT NULL,
    category TEXT NOT NULL,
    amount REAL NOT NULL,
    updated_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (household, category)
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS vendor_rules (
    household TEXT NOT NULL,
    vendor TEXT NOT NULL,
    category TEXT NOT NULL,
    type TEXT NOT NULL DEFAULT 'expense',
    updated_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (household, vendor)
  )`);
  run(`INSERT OR IGNORE INTO users (id, name, household) VALUES (?, ?, ?)`,
    ['christian', 'Christian', 'spenziero']);
  run(`INSERT OR IGNORE INTO users (id, name, household) VALUES (?, ?, ?)`,
    ['wife', 'Marisol', 'spenziero']);
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
  const start = new Date(); start.setDate(today.getDate() - 90);
  const fmt = d => d.toISOString().split('T')[0];
  const resp = await plaid.transactionsGet({
    access_token: accessToken,
    start_date: fmt(start), end_date: fmt(today),
    options: { count: 500, offset: 0 },
  });
  const upsertTx = db.prepare(`INSERT OR REPLACE INTO transactions
    (id, household, user_id, desc, amount, type, cat, date, pending, account_id, source)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'plaid')`);
  const upsertAcct = db.prepare(`INSERT OR REPLACE INTO accounts
    (account_id, user_id, household, name, mask, type, subtype, balance, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))`);
  // Load vendor rules to override Plaid categories
  const vendorRules = {};
  all('SELECT vendor, category, type FROM vendor_rules WHERE household = ?', [household])
    .forEach(r => { vendorRules[r.vendor] = { cat: r.category, type: r.type }; });

  const insertMany = db.transaction(() => {
    for (const t of resp.data.transactions) {
      // Check vendor rules first — user overrides take priority
      const rule = vendorRules[t.name];
      let cat, type;
      if (rule) {
        cat = rule.cat;
        type = rule.type;
      } else {
        cat = mapCategory(t.personal_finance_category?.primary || t.category?.[0]);
        const desc = (t.name || '').toUpperCase();
        if (cat !== 'Transfer' && (
          desc.includes('TRANSFER') || desc.includes('CREDIT CARD') && desc.includes('PAYMENT') ||
          desc.includes('CD DEPOSIT') || desc.includes('SAVINGS') && desc.includes('WITHDRAWAL') ||
          desc.includes('AUTOMATIC PAYMENT') || desc.includes('AUTOPAY') ||
          desc.includes('PAYMENT - THANK')
        )) cat = 'Transfer';
        type = cat === 'Transfer' ? 'transfer' : (t.amount > 0 ? 'expense' : 'income');
      }
      upsertTx.run(
        t.transaction_id, household, userId, t.name,
        Math.abs(t.amount), type, cat,
        t.date, t.pending ? 1 : 0, t.account_id
      );
    }
    for (const a of resp.data.accounts) {
      upsertAcct.run(
        a.account_id, userId, household,
        a.name, a.mask, a.type, a.subtype, a.balances.current
      );
    }
  });
  insertMany();
  return { count: resp.data.transactions.length, accounts: resp.data.accounts.length };
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

app.put('/api/transactions/:id/category', (req, res) => {
  const { category, type, applyToVendor } = req.body;
  if (!category && !type) return res.status(400).json({ error: 'Missing category or type' });
  try {
    const tx = get('SELECT * FROM transactions WHERE id = ?', [req.params.id]);
    if (!tx) return res.status(404).json({ error: 'Transaction not found' });
    const newCat = category || tx.cat;
    const newType = type || tx.type;
    // Update this transaction
    run('UPDATE transactions SET cat = ?, type = ? WHERE id = ?', [newCat, newType, req.params.id]);
    let updatedCount = 1;
    // If applyToVendor, save the rule and update all matching transactions
    if (applyToVendor) {
      run(`INSERT OR REPLACE INTO vendor_rules (household, vendor, category, type)
        VALUES (?, ?, ?, ?)`, [tx.household, tx.desc, newCat, newType]);
      const result = run('UPDATE transactions SET cat = ?, type = ? WHERE household = ? AND desc = ?',
        [newCat, newType, tx.household, tx.desc]);
      updatedCount = result.changes || 1;
    }
    res.json({ success: true, updated: updatedCount });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Vendor rules
app.get('/api/vendor-rules', (req, res) => {
  try {
    const hh = getHousehold(req.query.userId || 'christian');
    const rules = all('SELECT vendor, category, type FROM vendor_rules WHERE household = ?', [hh]);
    res.json({ rules });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/transactions/:id', (req, res) => {
  try { run('DELETE FROM transactions WHERE id = ?', [req.params.id]); res.json({ success: true }); }
  catch (e) { res.status(500).json({ error: e.message }); }
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
