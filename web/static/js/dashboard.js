// Dashboard page: system overview cards, link utilization and per-path load charts.
// Data sources: /api/v1/system/overview, /api/v1/stats/utilization, /api/v1/stats/paths.

const MAX_UTIL_BARS = 40;   // busiest links shown in the utilization chart
const MAX_PATH_BARS = 30;   // most loaded paths shown in the path chart

function refreshData() {
    loadSystemOverview();
    loadUtilizationChart();
    loadPathChart();
}

function fmt(value, digits) {
    return (typeof value === 'number' && isFinite(value)) ? value.toFixed(digits) : '-';
}

function loadSystemOverview() {
    $.get('/api/v1/system/overview', function(data) {
        const statsGrid = document.getElementById('statsGrid');
        if (!statsGrid) return;

        statsGrid.innerHTML = '';

        // Topology stats
        if (data.topology) {
            const topoCard = createStatCard('Network Topology', [
                `Switches: ${data.topology.switch_count || 0}`,
                `Links: ${data.topology.link_count || 0}`,
                `Hosts: ${data.topology.host_count || 0}`,
                `Paths: ${data.topology.path_count || 0}`
            ]);
            statsGrid.appendChild(topoCard);
        }

        // Network stats
        if (data.network_stats) {
            const netCard = createStatCard('Network Monitoring', [
                `Monitored Switches: ${data.network_stats.monitored_switches || 0}`,
                `Path Stats: ${data.network_stats.path_stats_count || 0}`
            ]);
            statsGrid.appendChild(netCard);
        }

        // Energy row of the latest monitor cycle (null before the first cycle)
        if (data.energy) {
            const e = data.energy;
            const pNorm = (typeof e.p_norm === 'number') ? e.p_norm
                : ((e.p_max > 0) ? e.p_total / e.p_max : NaN);
            const energyCard = createStatCard('Network Energy', [
                `Cycle: ${e.cycle !== undefined ? e.cycle : '-'}`,
                `Power: ${fmt(e.p_total, 1)} / ${fmt(e.p_max, 1)} (${fmt(pNorm * 100, 1)}%)`,
                `Average link utilization: ${fmt(e.alu_net * 100, 2)}%`,
                `Max link utilization: ${fmt(e.mlu_net * 100, 2)}%`
            ]);
            statsGrid.appendChild(energyCard);
        }

        // Agent status
        if (data.agents) {
            const agentCard = createStatCard('RL Agent', [
                `Agent1: ${data.agents.agent1_enabled ? 'Enabled' : 'Disabled'}`,
                `MPTCP Flows: ${data.agents.mptcp_flows || 0}`,
                `Active Meters: ${data.meters_active !== null && data.meters_active !== undefined ? data.meters_active : '-'}`
            ]);
            statsGrid.appendChild(agentCard);
        }

        // Module status
        if (data.modules) {
            const moduleCard = createStatCard('System Modules', [
                `Topology Manager: ${data.modules.topology_manager ? '✓' : '✗'}`,
                `Network Monitor: ${data.modules.network_monitor ? '✓' : '✗'}`,
                `Delay Detector: ${data.modules.delay_detector ? '✓' : '✗'}`,
                `Forwarding: ${data.modules.forwarding ? '✓' : '✗'}`
            ]);
            statsGrid.appendChild(moduleCard);
        }

    }).fail(function() {
        console.error('Failed to load system overview');
        const statsGrid = document.getElementById('statsGrid');
        if (statsGrid) {
            statsGrid.innerHTML = '<p>Failed to load system overview</p>';
        }
    });
}

function createStatCard(title, stats) {
    const card = document.createElement('div');
    card.className = 'stat-card';

    const cardTitle = document.createElement('h3');
    cardTitle.textContent = title;
    card.appendChild(cardTitle);

    const statsList = document.createElement('ul');
    stats.forEach(stat => {
        const listItem = document.createElement('li');
        listItem.textContent = stat;
        statsList.appendChild(listItem);
    });
    card.appendChild(statsList);

    return card;
}

// /api/v1/stats/utilization: {link_utilization: {"s<src>:s<dst>": u}, port_utilization: {"<dpid>:<port>": u}}
// with u a fraction of the link capacity. Directed links are preferred; ports are the
// fallback when no link mapping is known. Only the busiest MAX_UTIL_BARS are drawn.
function loadUtilizationChart() {
    $.get('/api/v1/stats/utilization', function(data) {
        const chartDiv = document.getElementById('utilizationChart');
        if (!chartDiv) return;

        let entries = Object.entries(data.link_utilization || {});
        let title = 'Link Utilization';
        if (entries.length === 0) {
            entries = Object.entries(data.port_utilization || {});
            title = 'Port Utilization';
        }
        if (entries.length === 0) {
            chartDiv.innerHTML = '<p style="text-align: center; padding: 50px;">No utilization data available</p>';
            return;
        }

        entries.sort((a, b) => b[1] - a[1]);
        const shown = entries.slice(0, MAX_UTIL_BARS);
        if (shown.length < entries.length) {
            title += ` (top ${shown.length} of ${entries.length})`;
        }

        const trace = {
            x: shown.map(e => e[0]),
            y: shown.map(e => (Number(e[1]) || 0) * 100),
            type: 'bar',
            marker: { color: '#1f77b4' },
            name: 'Utilization'
        };

        const layout = {
            title: title,
            xaxis: { title: 'Link', tickangle: -45, automargin: true },
            yaxis: { title: 'Utilization (%)', rangemode: 'tozero' },
            showlegend: false
        };

        Plotly.newPlot(chartDiv, [trace], layout);

    }).fail(function() {
        const chartDiv = document.getElementById('utilizationChart');
        if (chartDiv) {
            chartDiv.innerHTML = '<p style="text-align: center; padding: 50px;">Failed to load utilization data</p>';
        }
    });
}

// /api/v1/stats/paths: {path_stats: {"<src>-<dst>-pathN": [p_norm, alu, mlu, delay_rank]}}.
// Draws the average (ALU) and maximum (MLU) link utilization of the MAX_PATH_BARS most
// loaded paths.
function loadPathChart() {
    $.get('/api/v1/stats/paths', function(data) {
        const chartDiv = document.getElementById('pathChart');
        if (!chartDiv) return;

        const stats = data.path_stats || {};
        const rows = Object.keys(stats).map(key => {
            const state = stats[key] || [];
            return {
                key: key,
                alu: (Number(state[1]) || 0) * 100,
                mlu: (Number(state[2]) || 0) * 100
            };
        });
        if (rows.length === 0) {
            chartDiv.innerHTML = '<p style="text-align: center; padding: 50px;">No path data available</p>';
            return;
        }

        rows.sort((a, b) => b.mlu - a.mlu);
        const shown = rows.slice(0, MAX_PATH_BARS);
        let title = 'Path Load';
        if (shown.length < rows.length) {
            title += ` (top ${shown.length} of ${rows.length} by MLU)`;
        }

        const traces = [
            {
                x: shown.map(r => r.key),
                y: shown.map(r => r.alu),
                type: 'bar',
                name: 'Average link utilization',
                marker: { color: '#2ca02c' }
            },
            {
                x: shown.map(r => r.key),
                y: shown.map(r => r.mlu),
                type: 'bar',
                name: 'Max link utilization',
                marker: { color: '#d62728' }
            }
        ];

        const layout = {
            title: title,
            barmode: 'group',
            xaxis: { title: 'Path', tickangle: -45, automargin: true },
            yaxis: { title: 'Utilization (%)', rangemode: 'tozero' }
        };

        Plotly.newPlot(chartDiv, traces, layout);

    }).fail(function() {
        const chartDiv = document.getElementById('pathChart');
        if (chartDiv) {
            chartDiv.innerHTML = '<p style="text-align: center; padding: 50px;">Failed to load path data</p>';
        }
    });
}

// Auto-refresh every 30 seconds
setInterval(refreshData, 30000);

// Initial load
refreshData();
