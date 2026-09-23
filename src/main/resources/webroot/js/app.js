function fetchMetrics() {
    // 1. Fetch JVM and system metrics
    fetch('/api/metrics')
        .then(response => response.ok ? response.json() : null)
        .then(data => {
            if (!data) return;
            const heapEl = document.getElementById('valHeap');
            if (heapEl && data.heapUsedMb !== undefined) {
                heapEl.innerText = `${data.heapUsedMb} / ${data.heapMaxMb} MB`;
            }
        })
        .catch(err => console.debug('Metrics poll failed:', err));

    // 2. Fetch Monitored Targets
    fetch('/api/targets')
        .then(response => response.ok ? response.json() : null)
        .then(data => {
            if (!data) return;
            const targets = data.targets || [];
            const targetsCountEl = document.getElementById('valTargets');
            if (targetsCountEl) {
                targetsCountEl.innerText = targets.length;
            }

            const tbody = document.getElementById('targetTableBody');
            if (!tbody) return;

            if (targets.length === 0) {
                tbody.innerHTML = '<tr><td colspan="6" class="empty-state">No targets registered yet. Use POST /api/targets to add targets.</td></tr>';
                return;
            }

            tbody.innerHTML = targets.map(t => {
                const targetState = (t.state || 'HEALTHY').toUpperCase();
                const badgeClass = targetState === 'HEALTHY' ? 'badge-healthy' :
                                   targetState === 'DEGRADED' ? 'badge-degraded' : 'badge-critical';
                const endpoint = t.url || `${t.ip}:${t.port}`;
                const avg1m = typeof t.avg1m === 'number' ? t.avg1m.toFixed(2) + ' ms' : '-';
                const avg5m = typeof t.avg5m === 'number' ? t.avg5m.toFixed(2) + ' ms' : '-';

                return `
                    <tr>
                        <td><strong>${escapeHtml(t.id)}</strong></td>
                        <td>${escapeHtml(t.type || 'HTTP')}</td>
                        <td>${escapeHtml(endpoint)}</td>
                        <td>${avg1m}</td>
                        <td>${avg5m}</td>
                        <td><span class="badge ${badgeClass}">${escapeHtml(targetState)}</span></td>
                    </tr>
                `;
            }).join('');
        })
        .catch(err => console.debug('Targets poll failed:', err));

    // 3. Fetch Alerts
    fetch('/api/alerts')
        .then(response => response.ok ? response.json() : null)
        .then(data => {
            if (!data) return;
            const alerts = data.alerts || [];
            const alertsCountEl = document.getElementById('valAlerts');
            if (alertsCountEl) {
                alertsCountEl.innerText = alerts.length;
            }
        })
        .catch(err => console.debug('Alerts poll failed:', err));
}

function triggerBulkCheck() {
    fetch('/api/targets/bulk-check', { method: 'POST' })
        .then(response => {
            if (response.status === 202 || response.ok) {
                alert('⚡ Bulk re-check triggered successfully!');
            } else {
                alert('Triggering bulk check responded with: ' + response.status);
            }
        })
        .catch(err => alert('Failed to trigger bulk check: ' + err));
}

function escapeHtml(str) {
    if (!str) return '';
    return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}

// Auto-refresh every 1.5 seconds
document.addEventListener('DOMContentLoaded', () => {
    fetchMetrics();
    setInterval(fetchMetrics, 1500);
});
