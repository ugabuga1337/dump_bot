// HTMX error handling + small helpers.
window.dumpbot = window.dumpbot || {};

function renderHtmxError(target, status, statusText, body) {
    if (!target) return;
    const safeStatus = status || 'error';
    const safeText = (statusText || '').toString().slice(0, 160);
    let detail = (body || '').toString().trim();
    if (detail.length > 240) detail = detail.slice(0, 240) + '…';
    target.innerHTML =
        '<div class="error-banner">' +
        '<b>Request failed</b> — ' + safeStatus +
        (safeText ? ' ' + safeText : '') +
        (detail ? '<br><code>' + detail.replace(/[<>&]/g, function(c){
            return ({'<':'&lt;','>':'&gt;','&':'&amp;'})[c];
        }) + '</code>' : '') +
        '</div>';
}

document.addEventListener('htmx:responseError', function (evt) {
    const xhr = evt.detail && evt.detail.xhr;
    const target = (evt.detail && evt.detail.target) || evt.target;
    console.warn('htmx error', evt.detail);
    renderHtmxError(
        target,
        xhr ? xhr.status : '',
        xhr ? xhr.statusText : '',
        xhr ? xhr.responseText : ''
    );
});

document.addEventListener('htmx:sendError', function (evt) {
    const target = (evt.detail && evt.detail.target) || evt.target;
    console.warn('htmx send error', evt.detail);
    renderHtmxError(target, 'network', 'failed to reach server', '');
});

document.addEventListener('htmx:timeout', function (evt) {
    const target = (evt.detail && evt.detail.target) || evt.target;
    renderHtmxError(target, 'timeout', 'request timed out', '');
});
