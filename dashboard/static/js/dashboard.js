// InsightHub Dashboard JavaScript - Optimized with Skeleton Loading

const API_BASE = '';
let refreshInterval = null;
let currentInterval = 60000; // default 1 min
let cache = {};

// Per-endpoint cache TTLs (milliseconds)
const CACHE_TTLS = {
    '/api/stats': 30000,      // 30s
    '/api/servers': 15000,    // 15s  
    '/api/countries': 60000,  // 60s
    '/api/recent': 10000,     // 10s
    '/api/hourly': 300000,    // 5 min
    'default': 30000          // 30s default
};

// Country pagination state
let countryState = {
    page: 1,
    limit: 20,
    sortBy: 'source_total',
    sortOrder: 'desc',
    total: 0,
    pages: 0
};

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
    
    // Get TTL for this endpoint (strip query params for lookup)
    const baseEndpoint = endpoint.split('?')[0];
    const ttl = CACHE_TTLS[baseEndpoint] || CACHE_TTLS['default'];

    // Check cache
    if (useCache && cache[cacheKey] && (now - cache[cacheKey].time) < ttl) {
        return cache[cacheKey].data;
    }

    try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 60000); // 60s timeout for slow remote DB

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
    
    // Social media stats
    const totalSocialEl = document.getElementById('total-social');
    const rowsWithSocialEl = document.getElementById('rows-with-social');
    if (totalSocialEl) totalSocialEl.textContent = formatNumber(data.total_social || 0);
    if (rowsWithSocialEl) rowsWithSocialEl.textContent = formatNumber(data.rows_with_social || 0) + ' rows';
    
    // Activity stats
    document.getElementById('processed-24h').textContent = formatNumber(data.processed_24h);
    const processed1hEl = document.getElementById('processed-1h');
    if (processed1hEl) processed1hEl.textContent = formatNumber(data.processed_1h || 0) + ' last hour';

    hideStatsSkeleton();
    document.getElementById('last-updated').textContent = 'Updated: ' + new Date().toLocaleTimeString();
}

// Show server detail modal
function showServerModal(server) {
    const modal = document.getElementById('server-modal');
    if (!modal) return;
    
    const formatDate = (d) => d ? new Date(d).toLocaleString() : '-';
    
    document.getElementById('modal-server-id').textContent = server.server_id || '-';
    document.getElementById('modal-server-name').textContent = server.server_name || '-';
    document.getElementById('modal-server-ip').textContent = server.server_ip || '-';
    document.getElementById('modal-server-hostname').textContent = server.server_hostname || '-';
    document.getElementById('modal-server-region').textContent = server.server_region || '-';
    document.getElementById('modal-server-status').textContent = server.status || '-';
    document.getElementById('modal-server-health').textContent = server.health || '-';
    document.getElementById('modal-server-workers').textContent = server.workers_count || '-';
    document.getElementById('modal-server-task').textContent = server.current_task || 'Idle';
    document.getElementById('modal-server-processed').textContent = formatNumber(server.total_processed);
    document.getElementById('modal-server-success').textContent = formatNumber(server.total_success);
    document.getElementById('modal-server-failed').textContent = formatNumber(server.total_failed);
    document.getElementById('modal-server-rate').textContent = server.urls_per_minute ? server.urls_per_minute.toFixed(2) + '/min' : '-';
    document.getElementById('modal-server-success-rate').textContent = server.success_rate ? formatPercent(server.success_rate) : '-';
    document.getElementById('modal-server-session-processed').textContent = formatNumber(server.session_processed);
    document.getElementById('modal-server-session-errors').textContent = formatNumber(server.session_errors);
    document.getElementById('modal-server-started').textContent = formatDate(server.started_at);
    document.getElementById('modal-server-session-started').textContent = formatDate(server.session_started);
    document.getElementById('modal-server-heartbeat').textContent = formatDate(server.last_heartbeat);
    document.getElementById('modal-server-activity').textContent = formatDate(server.last_activity);
    
    modal.classList.add('show');
}

// Close modal
function closeServerModal() {
    const modal = document.getElementById('server-modal');
    if (modal) modal.classList.remove('show');
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

    tbody.innerHTML = servers.map((s, i) => `
        <tr class="fade-in">
            <td><strong class="server-link" data-server-idx="${i}">${s.server_name || s.server_id}</strong></td>
            <td>${s.server_region || '-'}</td>
            <td class="status-${s.status}">${s.status}</td>
            <td><span class="health-${s.health}">${s.health}</span></td>
            <td>${formatNumber(s.total_processed)}</td>
            <td>${s.urls_per_minute ? s.urls_per_minute.toFixed(1) + '/min' : '-'}</td>
            <td>${s.success_rate ? formatPercent(s.success_rate) : '-'}</td>
            <td>${timeAgo(s.last_activity)}</td>
        </tr>
    `).join('');
    
    // Store servers data for modal
    window._serversData = servers;
    
    // Add click handlers
    tbody.querySelectorAll('.server-link').forEach(el => {
        el.addEventListener('click', () => {
            const idx = parseInt(el.dataset.serverIdx);
            if (window._serversData && window._serversData[idx]) {
                showServerModal(window._serversData[idx]);
            }
        });
    });
}

// Update countries table with pagination and sorting
async function updateCountries(resetPage = false) {
    if (resetPage) countryState.page = 1;
    
    const params = new URLSearchParams({
        page: countryState.page,
        limit: countryState.limit,
        sort_by: countryState.sortBy,
        sort_order: countryState.sortOrder
    });
    
    const data = await fetchAPI(`/api/countries?${params}`, false);
    if (!data) return;

    const tbody = document.getElementById('countries-body');
    const countries = data.countries || [];
    countryState.total = data.total || 0;
    countryState.pages = data.pages || 0;

    document.getElementById('countries-count').textContent = countryState.total + ' countries';

    if (countries.length === 0) {
        tbody.innerHTML = '<tr><td colspan="8" class="loading">No data yet</td></tr>';
        renderCountryPagination();
        return;
    }

    tbody.innerHTML = countries.map(c => `
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
    
    renderCountryPagination();
    updateSortIndicators();
}

// Render pagination controls for countries
function renderCountryPagination() {
    let paginationEl = document.getElementById('countries-pagination');
    if (!paginationEl) {
        const section = document.getElementById('countries-section');
        paginationEl = document.createElement('div');
        paginationEl.id = 'countries-pagination';
        paginationEl.className = 'pagination';
        section.appendChild(paginationEl);
    }
    
    if (countryState.pages <= 1) {
        paginationEl.innerHTML = '';
        return;
    }
    
    const { page, pages, total, limit } = countryState;
    const start = (page - 1) * limit + 1;
    const end = Math.min(page * limit, total);
    
    let html = `<span class="pagination-info">Showing ${start}-${end} of ${total}</span>`;
    html += '<div class="pagination-controls">';
    
    html += `<button class="btn btn-sm" onclick="goToCountryPage(1)" ${page === 1 ? 'disabled' : ''}>«</button>`;
    html += `<button class="btn btn-sm" onclick="goToCountryPage(${page - 1})" ${page === 1 ? 'disabled' : ''}>‹</button>`;
    
    // Page numbers
    const maxVisible = 5;
    let startPage = Math.max(1, page - Math.floor(maxVisible / 2));
    let endPage = Math.min(pages, startPage + maxVisible - 1);
    if (endPage - startPage < maxVisible - 1) {
        startPage = Math.max(1, endPage - maxVisible + 1);
    }
    
    for (let i = startPage; i <= endPage; i++) {
        html += `<button class="btn btn-sm ${i === page ? 'btn-primary' : ''}" onclick="goToCountryPage(${i})">${i}</button>`;
    }
    
    html += `<button class="btn btn-sm" onclick="goToCountryPage(${page + 1})" ${page === pages ? 'disabled' : ''}>›</button>`;
    html += `<button class="btn btn-sm" onclick="goToCountryPage(${pages})" ${page === pages ? 'disabled' : ''}>»</button>`;
    html += '</div>';
    
    paginationEl.innerHTML = html;
}

// Go to specific country page
function goToCountryPage(page) {
    if (page < 1 || page > countryState.pages) return;
    countryState.page = page;
    updateCountries();
}

// Sort countries by column
function sortCountries(column) {
    if (countryState.sortBy === column) {
        countryState.sortOrder = countryState.sortOrder === 'desc' ? 'asc' : 'desc';
    } else {
        countryState.sortBy = column;
        countryState.sortOrder = 'desc';
    }
    countryState.page = 1;
    updateCountries();
}

// Update sort indicators in table headers
function updateSortIndicators() {
    const headers = document.querySelectorAll('#countries-table th[data-sort]');
    headers.forEach(th => {
        const col = th.dataset.sort;
        th.classList.remove('sort-asc', 'sort-desc');
        if (col === countryState.sortBy) {
            th.classList.add(countryState.sortOrder === 'asc' ? 'sort-asc' : 'sort-desc');
        }
    });
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
async function refreshData(resetPagination = true) {
    // Show skeleton loading
    showStatsSkeleton();
    showTableSkeleton('servers-body', 3);
    showTableSkeleton('countries-body', 5);
    showTableSkeleton('activity-body', 5);

    // Fetch all data in parallel
    await Promise.all([
        updateStats(),
        updateServers(),
        updateCountries(resetPagination),
        updateActivity()
    ]);
}

// Change refresh interval
function changeRefreshInterval() {
    const select = document.getElementById('refresh-interval');
    currentInterval = parseInt(select.value);

    // Clear existing interval
    if (refreshInterval) {
        clearInterval(refreshInterval);
        refreshInterval = null;
    }

    // Set new interval (0 = manual only)
    if (currentInterval > 0) {
        refreshInterval = setInterval(() => {
            cache = {}; // Clear cache before refresh
            refreshData(false); // Don't reset pagination on auto-refresh
        }, currentInterval);
    }

    // Save preference to localStorage
    localStorage.setItem('dashboardRefreshInterval', currentInterval);
}

// Start auto-refresh with saved or default interval
function startAutoRefresh() {
    // Restore saved preference
    const saved = localStorage.getItem('dashboardRefreshInterval');
    if (saved !== null) {
        currentInterval = parseInt(saved);
        const select = document.getElementById('refresh-interval');
        if (select) select.value = currentInterval;
    }

    // Start interval if not manual
    if (currentInterval > 0) {
        refreshInterval = setInterval(() => {
            cache = {};
            refreshData(false); // Don't reset pagination on auto-refresh
        }, currentInterval);
    }
}

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    refreshData();
    startAutoRefresh();
});
