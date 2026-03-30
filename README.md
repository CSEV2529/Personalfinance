# LEDGER — Household Finance App
## Setup, Hosting & Sharing Guide

---

## What's in this folder

```
ledger-app/
├── backend/
│   ├── server.js          ← Express + SQLite + Plaid + webhooks
│   ├── package.json
│   └── .env.example
└── frontend/
    ├── index.html         ← Full PWA (mobile + desktop)
    ├── manifest.json      ← PWA install config
    ├── sw.js              ← Service worker (offline support)
    ├── generate-icons.js  ← Run once to make app icons
    └── icons/             ← Generated icon files go here
```

---

## Step 1 — Get Plaid Keys (5 min, free)

1. Sign up at https://dashboard.plaid.com
2. Go to **Team Settings → Keys**
3. Copy **Client ID** and **Sandbox Secret**

---

## Step 2 — Configure backend

```bash
cd ledger-app/backend
cp .env.example .env
```

Edit `.env`:
```
PLAID_CLIENT_ID=your_client_id
PLAID_SECRET=your_sandbox_secret
PLAID_ENV=sandbox
PORT=3001
```

---

## Step 3 — Install and start

```bash
cd ledger-app/backend
npm install
node server.js
```

Output:
```
✓ Ledger backend running → http://localhost:3001
  Plaid env:  sandbox
  Client ID:  ✓ set
  DB:         ledger.db (SQLite)
```

Now open http://localhost:3001 in your browser.

---

## Step 4 — Generate app icons (optional)

```bash
cd ledger-app/frontend
npm install canvas
node generate-icons.js
```

---

## Step 5 — Connect your banks

1. Open the app → tap **Accounts** tab
2. Tap **Connect Bank**
3. Sandbox: use `user_good` / `pass_good` with any test bank
4. Your wife does the same on her phone (as user "wife")

Both phones see all household transactions combined.

---

## HOSTING (so both phones can access it)

### Option A: Render.com (recommended, free tier)

1. Push to GitHub:
   ```bash
   git init && git add . && git commit -m "init"
   git remote add origin https://github.com/YOUR_USERNAME/ledger.git
   git push
   ```

2. Go to https://render.com → **New Web Service**
3. Connect your repo
4. Settings:
   - **Root Directory:** `ledger-app/backend`
   - **Build command:** `npm install`
   - **Start command:** `node server.js`
5. Add environment variables (PLAID_CLIENT_ID, PLAID_SECRET, PLAID_ENV=sandbox)
6. Click **Deploy**

You get a URL like `https://ledger-xyz.onrender.com`

5. Update the API variable in `frontend/index.html`:
   Change:
   ```js
   const API = window.location.hostname === 'localhost' ...
   ```
   The empty string `''` handles same-origin automatically once frontend is served from backend.

### Option B: Railway ($5/mo, always on)

1. Go to https://railway.app → Deploy from GitHub
2. Same env vars as above
3. Railway keeps the server always on (Render free tier sleeps after 15min)

### Switching from Sandbox to real banks

1. Apply for **Development** mode in Plaid dashboard (approved same day, free, up to 100 banks)
2. Update `.env`:
   ```
   PLAID_ENV=development
   PLAID_SECRET=your_development_secret
   ```
3. Redeploy

---

## SHARING WITH YOUR WIFE

Once hosted, she just opens the URL on her phone.

**To install on her phone (iOS):**
1. Open the URL in Safari
2. Tap Share → "Add to Home Screen"
3. Name it "Ledger" → Add
4. Tap the icon on her home screen — it opens like a native app
5. She connects her bank via the Accounts tab

**To install on your phone (Android):**
1. Open in Chrome
2. Chrome shows "Add to Home Screen" banner automatically
3. Or tap ⋮ → "Install App"

Both phones share all transaction data via the SQLite database on your server.

---

## REAL-TIME WEBHOOKS (near-instant transaction sync)

Without webhooks: data syncs when you tap the Sync button.
With webhooks: Plaid pushes new transactions within ~2 minutes of them posting.

### Setup:

1. Your backend must be publicly accessible (i.e., deployed on Render/Railway)

2. In your `.env`:
   ```
   WEBHOOK_URL=https://your-app.onrender.com/api/webhook
   ```

3. In Plaid Dashboard → Developers → Webhooks: add the same URL

4. Reconnect your banks (webhooks only apply to new Link sessions)

That's it. Plaid will POST to `/api/webhook` whenever transactions update.

---

## CUSTOMIZING HOUSEHOLD MEMBERS

Edit the seed users in `backend/server.js`:

```js
seedUser.run('christian', 'Christian', 'spenziero');
seedUser.run('wife', 'Your Wife Name Here', 'spenziero');
```

Both users share the `'spenziero'` household. All their Plaid-linked transactions
are pooled together in the dashboard.

---

## TOMORROW'S CHECKLIST (pick up on desktop)

- [ ] Run `npm install` in `ledger-app/backend`
- [ ] Copy `.env.example` → `.env` and fill in Plaid keys
- [ ] Run `node server.js`
- [ ] Open http://localhost:3001
- [ ] Connect your bank (sandbox or development)
- [ ] Push to GitHub and deploy to Render
- [ ] Share URL with wife, she installs as PWA on her phone
- [ ] Apply for Plaid Development access for real bank connections
- [ ] (Optional) Enable webhooks for real-time sync
