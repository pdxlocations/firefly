const STATIC_CACHE = "firefly-static-v3";
const STATIC_ASSETS = [
  "/static/manifest.json",
  "/static/css/style.css",
  "/static/images/favicon.svg",
  "/static/images/favicon-blk.svg",
  "/static/images/firefly.svg",
  "/static/images/firefly-logo-dark.png",
  "/static/images/firefly-logo-light.png",
];

self.addEventListener("install", event => {
  event.waitUntil(
    caches.open(STATIC_CACHE)
      .then(cache => cache.addAll(STATIC_ASSETS))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener("activate", event => {
  event.waitUntil(
    caches.keys().then(cacheNames =>
      Promise.all(
        cacheNames
          .filter(cacheName => cacheName !== STATIC_CACHE)
          .map(cacheName => caches.delete(cacheName))
      )
    ).then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", event => {
  const { request } = event;
  if (request.method !== "GET") {
    return;
  }

  const url = new URL(request.url);
  if (url.origin !== self.location.origin || !url.pathname.startsWith("/static/")) {
    return;
  }

  event.respondWith(
    caches.match(request).then(cachedResponse => {
      if (cachedResponse) {
        return cachedResponse;
      }

      return fetch(request).then(networkResponse => {
        if (!networkResponse || !networkResponse.ok) {
          return networkResponse;
        }

        const responseCopy = networkResponse.clone();
        caches.open(STATIC_CACHE).then(cache => cache.put(request, responseCopy));
        return networkResponse;
      });
    })
  );
});

self.addEventListener("notificationclick", event => {
  event.notification.close();

  const requestedUrl = event.notification?.data?.url || "/";
  const targetUrl = new URL(requestedUrl, self.location.origin).href;

  event.waitUntil(
    clients.matchAll({ type: "window", includeUncontrolled: true }).then(windowClients => {
      for (const client of windowClients) {
        if (!("focus" in client)) {
          continue;
        }
        if ("navigate" in client) {
          return client.navigate(targetUrl).then(() => client.focus());
        }
        return client.focus();
      }
      return clients.openWindow(targetUrl);
    })
  );
});
