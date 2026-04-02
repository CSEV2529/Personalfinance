/**
 * One-time migration: SQLite → Supabase PostgreSQL
 * Run: node migrate.js
 */
require('dotenv').config();
const Database = require('better-sqlite3');
const { Pool } = require('pg');

const sqlite = new Database('./ledger.db');
const pg = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('supabase') ? { rejectUnauthorized: false } : false,
});

async function migrate() {
  console.log('Starting migration...');

  // Users
  const users = sqlite.prepare('SELECT * FROM users').all();
  for (const u of users) {
    await pg.query(
      'INSERT INTO users (id, name, household, streak_count, streak_best, last_active_date) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING',
      [u.id, u.name, u.household, u.streak_count || 0, u.streak_best || 0, u.last_active_date]
    );
  }
  console.log(`✓ ${users.length} users`);

  // Plaid items
  const items = sqlite.prepare('SELECT * FROM plaid_items').all();
  for (const i of items) {
    await pg.query(
      'INSERT INTO plaid_items (item_id, user_id, access_token, institution) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING',
      [i.item_id, i.user_id, i.access_token, i.institution]
    );
  }
  console.log(`✓ ${items.length} plaid_items`);

  // Transactions
  const txs = sqlite.prepare('SELECT * FROM transactions').all();
  let txCount = 0;
  for (const t of txs) {
    try {
      await pg.query(
        `INSERT INTO transactions (id, household, user_id, "desc", amount, type, cat, date, pending, account_id, source, notes, is_recurring, reviewed, original_cat, status)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) ON CONFLICT DO NOTHING`,
        [t.id, t.household, t.user_id, t.desc, t.amount, t.type, t.cat, t.date, !!t.pending, t.account_id, t.source, t.notes, !!t.is_recurring, !!t.reviewed, t.original_cat, t.status || 'confirmed']
      );
      txCount++;
    } catch (e) { /* skip dupes */ }
  }
  console.log(`✓ ${txCount} transactions`);

  // Accounts
  const accts = sqlite.prepare('SELECT * FROM accounts').all();
  for (const a of accts) {
    await pg.query(
      'INSERT INTO accounts (account_id, user_id, household, name, mask, type, subtype, balance) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING',
      [a.account_id, a.user_id, a.household, a.name, a.mask, a.type, a.subtype, a.balance]
    );
  }
  console.log(`✓ ${accts.length} accounts`);

  // Budgets
  const budgets = sqlite.prepare('SELECT * FROM budgets').all();
  for (const b of budgets) {
    await pg.query('INSERT INTO budgets (household, category, amount) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING', [b.household, b.category, b.amount]);
  }
  console.log(`✓ ${budgets.length} budgets`);

  // Vendor rules
  const rules = sqlite.prepare('SELECT * FROM vendor_rules').all();
  for (const r of rules) {
    await pg.query('INSERT INTO vendor_rules (household, vendor, category, type) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING', [r.household, r.vendor, r.category, r.type]);
  }
  console.log(`✓ ${rules.length} vendor_rules`);

  // Categories
  const cats = sqlite.prepare('SELECT * FROM categories').all();
  for (const c of cats) {
    await pg.query('INSERT INTO categories (id, household, icon, color, type, sort_order, is_active) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING',
      [c.id, c.household, c.icon, c.color, c.type, c.sort_order, !!c.is_active]);
  }
  console.log(`✓ ${cats.length} categories`);

  // Category icons
  try {
    const icons = sqlite.prepare('SELECT * FROM category_icons').all();
    for (const i of icons) {
      await pg.query('INSERT INTO category_icons (household, category, icon) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING', [i.household, i.category, i.icon]);
    }
    console.log(`✓ ${icons.length} category_icons`);
  } catch (e) { console.log('Skipped category_icons'); }

  // Recurring rules
  try {
    const recs = sqlite.prepare('SELECT * FROM recurring_rules').all();
    for (const r of recs) {
      await pg.query('INSERT INTO recurring_rules (id, household, vendor, category, expected_amount, frequency, is_subscription, last_seen, is_active) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING',
        [r.id, r.household, r.vendor, r.category, r.expected_amount, r.frequency, !!r.is_subscription, r.last_seen, !!r.is_active]);
    }
    console.log(`✓ ${recs.length} recurring_rules`);
  } catch (e) { console.log('Skipped recurring_rules'); }

  // Transaction tags
  try {
    const tags = sqlite.prepare('SELECT * FROM transaction_tags').all();
    for (const t of tags) {
      await pg.query('INSERT INTO transaction_tags (transaction_id, tag, household) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING', [t.transaction_id, t.tag, t.household]);
    }
    console.log(`✓ ${tags.length} transaction_tags`);
  } catch (e) { console.log('Skipped transaction_tags'); }

  console.log('\n✓ Migration complete!');
  await pg.end();
  process.exit(0);
}

migrate().catch(e => { console.error('Migration failed:', e); process.exit(1); });
