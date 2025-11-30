// Zenvoyer Dashboard JavaScript - Optimized with Skeleton Loading

const API_BASE = '';
let refreshInterval = null;
let cache = {};
const CACHE_TTL = 10000; // 10 seconds cache

// Format numbers with commas
function formatNumber(num) {
    if (num === null || num === undefined || num === '-') return '-';
    return num.toLocaleString();
}

// Format percentage
function formatPercent(num) {
    if (num === null || num === undefined) return '-';
    return num.toFixed(1) + '%';
}

// Format time ago
function timeAgo(dateStr) {
    if (!dateStr) return '-';
    const date = new Date(dateStr);
    const seconds = Math.floor((new Date() - date) / 1000);
    
    if (seconds < 60) return seconds + 's ago';
    if (seconds < 3600) return Math.floor(seconds / 60) + 'm ago';
    if (seconds < 86400) return Math.floor(seconds / 3600) + 'h ago';
    return Math.floor(seconds / 86400) + 'd ago';
}

// Show skeleton loading for stat cards
function showStatsSkeleton() {
    document.querySelectorAll('.stat-card').forEach(card => {
        card.classList.add('loading');
    });
}

// Hide skeleton loading
function hideStatsSkeleton() {
    document.querySelectorAll('.stat-card').forEach(card => {
        card.classList.remove('loading');
    });
}

// Show table skeleton
function showTableSkeleton(tableId, rows = 5) {
    const tbody = document.getElementById(tableId);
    if (!tbody) return;
    
    const cols = tbody.closest('table').querySelectorAll('th').length;
    let html = '';
    for (let i = 0; i < rows; i++) {
        html += '<tr class="skeleton-row">';
        for (let j = 0; j < cols; j++) {
            html += '<td><div class="skeleton skeleton-text"></div></td>';
        }
        html += '</tr>';
    }
    tbody.innerHTML = html;
}

// Fetch with cache
async function fetchAPI(endpoint, useCache = true) {
    const cacheKey = endpoint;
    const now = Date.now();
    
    // Check cache
    if (useCache && cache[cacheKey] && (now - cache[cacheKey].time) < CACHE_TTL) {
        return cache[cacheKey].data;
    }
    
    try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 15000); // 15s timeout
        
        const response = await fetch(API_BASE + endpoint, {
            signal: controller.signal
        });
        clearTimeout(timeout);
        
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const data = await response.json();
        
        // Update cache
        cache[cacheKey] = { data, time: now };
        return data;
    } catch (error) {
        console.error(`API error (${endpoint}):`, error);
        return null;
    }
}

// Update overview stats
async function updateStats() {
    const data = await fetchAPI('/api/stats');
    if (!data) return;
    
    document.getElementById('source-total').textContent = formatNumber(data.source_total);
    document.getElementById('enriched-total').textContent = formatNumber(data.enriched_total);
    document.getElementById('pending-total').textContent = formatNumber(data.pending_total);
    
    // Success rate
    const total = data.enriched_success + data.enriched_failed;
    const rate = total > 0 ? (data.enriched_success / total * 100) : 0;
    document.getElementById('success-rate').textContent = formatPercent(rate);
    
    // Contact stats
    document.getElementById('total-emails').textContent = formatNumber(data.total_emails);
    document.getElementById('total-phones').textContent = formatNumber(data.total_phones);
    document.getElementById('total-whatsapp').textContent = formatNumber(data.total_whatsapp);
    document.getElementById('rows-with-email').textContent = formatNumber(data.rows_with_email) + ' rows';
    document.getElementById('rows-with-phone').textContent = formatNumber(data.rows_with_phone) + ' rows';
    document.getElementById('rows-with-whatsapp').textContent = formatNumber(data.rows_with_whatsapp) + ' rows';
    document.getElementById('processed-24h').textContent = formatNumber(data.processed_24h);
    
    hideStatsSkeleton();
    document.getElementById('last-updated').textContent = 'Updated: ' + new Date().toLocaleTimeString();
}

// Update servers table
async function updateServers() {
    const data = await fetchAPI('/api/servers');
    if (!data) return;
    
    const tbody = document.getElementById('servers-body');
    const servers = data.servers || [];
    
    document.getElementById('servers-online').textContent = 
        servers.filter(s => s.status === 'online').length + ' online';
    
    if (servers.length === 0) {
        tbody.innerHTML = '<tr><td colspan="8" class="loading">No servers registered</td></tr>';
        return;
    }
    
    tbody.innerHTML = servers.map(s => `
        <tr class="fade-in">
            <td><strong>${s.server_name || s.server_id}</strong></td>
            <td>${s.server_region || '-'}</td>
            <td class="status-${s.status}">${s.status}</td>
            <td><span class="health-${s.health}">${s.health}</span></td>
            <td>${formatNumber(s.total_processed)}</td>
            <td>${s.urls_per_minute ? s.urls_per_minute.toFixed(1) + '/min' : '-'}</td>
            <td>${s.success_rate ? formatPercent(s.success_rate) : '-'}</td>
            <td>${timeAgo(s.last_activity)}</td>
        </tr>
    `).join('');
}

// Update countries table
async function updateCountries() {
    const data = await fetchAPI('/api/countries');
    if (!data) return;
    
    const tbody = document.getElementById('countries-body');
    const countries = data.countries || [];
    
    document.getElementById('countries-count').textContent = countries.length + ' countries';
    
    if (countries.length === 0) {
        tbody.innerHTML = '<tr><td colspan="8" class="loading">No data yet</td></tr>';
        return;
    }
    
    tbody.innerHTML = countries.slice(0, 50).map(c => `
        <tr class="fade-in">
            <td><strong>${c.country_code}</strong></td>
            <td>${formatNumber(c.source_total)}</td>
            <td>${formatNumber(c.enriched_total)}</td>
            <td>${formatNumber(c.pending)}</td>
            <td>
                <div class="progress-bar">
                    <div class="progress-fill" style="width: ${c.progress_percent}%"></div>
                </div>
                ${formatPercent(c.progress_percent)}
            </td>
            <td>${formatNumber(c.emails_total)}</td>
            <td>${formatNumber(c.whatsapp_total)}</td>
            <td>${c.avg_scrape_time ? c.avg_scrape_time + 's' : '-'}</td>
        </tr>
    `).join('');
}

// Update recent activity
async function updateActivity() {
    const data = await fetchAPI('/api/recent?limit=50');
    if (!data) return;
    
    const tbody = document.getElementById('activity-body');
    const activity = data.activity || [];
    
    if (activity.length === 0) {
        tbody.innerHTML = '<tr><td colspan="8" class="loading">No recent activity</td></tr>';
        return;
    }
    
    tbody.innerHTML = activity.map(a => `
        <tr class="fade-in">
            <td>${(a.business_name || '').substring(0, 40)}</td>
            <td>${a.country_code}</td>
            <td class="status-${a.scrape_status}">${a.scrape_status}</td>
            <td>${a.emails_count || 0}</td>
            <td>${a.phones_count || 0}</td>
            <td>${a.whatsapp_count || 0}</td>
            <td>${a.scrape_time_seconds ? a.scrape_time_seconds.toFixed(1) + 's' : '-'}</td>
            <td>${timeAgo(a.updated_at)}</td>
        </tr>
    `).join('');
}

// Refresh all data - parallel loading
async function refreshData() {
    // Show skeleton loading
    showStatsSkeleton();
    showTableSkeleton('servers-body', 3);
    showTableSkeleton('countries-body', 5);
    showTableSkeleton('activity-body', 5);
    
    // Fetch all data in parallel
    await Promise.all([
        updateStats(),
        updateServers(),
        updateCountries(),
        updateActivity()
    ]);
}

// Auto-refresh every 30 seconds
function startAutoRefresh() {
    if (refreshInterval) clearInterval(refreshInterval);
    refreshInterval = setInterval(() => {
        // Clear cache before refresh
        cache = {};
        refreshData();
    }, 30000);
}

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    refreshData();
    startAutoRefresh();
});
