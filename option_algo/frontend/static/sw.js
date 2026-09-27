// frontend/static/sw.js
// ================================================================
// Service worker for Web Push notifications.
//
// Scope: must be served from the site root (/sw.js) so it can
// receive push events for the whole origin. Registered from
// dashboard.html via navigator.serviceWorker.register('/sw.js').
//
// This worker does ONLY push-notification handling — no caching/
// offline support (out of scope for this trading dashboard, where
// stale cached data would be actively misleading).
// ================================================================

self.addEventListener('install', (event) => {
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(self.clients.claim());
});

// Incoming push message — payload is JSON: {title, body, data, tag}
// (see backend/services/push_notifications.py)
self.addEventListener('push', (event) => {
  let payload = { title: 'Optiscalper', body: 'New trading event' };
  try {
    if (event.data) payload = event.data.json();
  } catch (e) {
    if (event.data) payload.body = event.data.text();
  }

  const options = {
    body: payload.body || '',
    icon: '/static/icons/icon-192.png',
    badge: '/static/icons/icon-192.png',
    tag: payload.tag || 'optiscalper',
    renotify: true,
    data: payload.data || {},
    timestamp: Date.now(),
  };

  event.waitUntil(
    self.registration.showNotification(payload.title || 'Optiscalper', options)
  );
});

// Clicking a notification focuses/opens the dashboard
self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  event.waitUntil(
    self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then((clients) => {
      for (const client of clients) {
        if (client.url.includes('/dashboard') && 'focus' in client) {
          return client.focus();
        }
      }
      if (self.clients.openWindow) {
        return self.clients.openWindow('/dashboard');
      }
    })
  );
});
