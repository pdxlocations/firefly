const STATIC_CACHE = "firefly-static-v2";
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
