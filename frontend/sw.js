/**
 * Ledger Service Worker
 * Caches the app shell for offline use.
 * Transactions always try network first, fall back to cache.
 */

const CACHE = 'obsidian-v72';
const SHELL = [
  '/',
  '/manifest.json',
];

// Install: cache app shell
self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE).then(cache => cache.addAll(SHELL))
  );
  self.skipWaiting();
});

// Activate: clean old caches
self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    )
  );
  self.clients.claim();
});

// Fetch strategy:
// - API calls: network first, no cache
// - Everything else: cache first, fallback to network
self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);

  // Never cache API calls
  if (url.pathname.startsWith('/api/')) {
    e.respondWith(fetch(e.request).catch(() => new Response(
      JSON.stringify({ error: 'Offline — connect to sync' }),
      { headers: { 'Content-Type': 'application/json' } }
    )));
    return;
  }

  // App shell: network first, fall back to cache
  e.respondWith(
    fetch(e.request).then(res => {
      const clone = res.clone();
      caches.open(CACHE).then(cache => cache.put(e.request, clone));
      return res;
    }).catch(() => caches.match(e.request))
  );
});

// Push notifications (for future budget alerts)
self.addEventListener('push', e => {
  if (!e.data) return;
  const data = e.data.json();
  self.registration.showNotification(data.title || 'Ledger', {
    body: data.body || '',
    icon: '/icons/icon-192.png',
    badge: '/icons/icon-192.png',
    data: data,
  });
});
