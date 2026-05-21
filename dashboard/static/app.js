// Minimal helpers used by analytics page (Chart.js is loaded via base.html).
// Most updates are HTMX-driven; this is intentionally tiny.
window.dumpbot = window.dumpbot || {};

document.addEventListener('htmx:responseError', function (evt) {
    console.warn('htmx error', evt.detail);
});
