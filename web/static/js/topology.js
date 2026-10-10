// Topology page: force-directed drawing of /api/v1/topology/details, which returns
// {nodes: [{id, type: 'switch'|'host', label, ...}], links: [{from, to, type: 'switch_link'|'host_link', ...}]}.

let topology_svg, simulation;

function refreshTopology() {
    $.get('/api/v1/topology/details', function(data) {
        drawTopology(data);
    }).fail(function(xhr, status, error) {
        console.error('Failed to load topology:', error);
        $('#topology').html('<p style="padding: 20px; text-align: center;">Failed to load topology data. Error: ' + error + '</p>');
    });
}

function drawTopology(topologyData) {
    // Clear existing topology
    d3.select("#topology").selectAll("*").remove();

    const rawNodes = (topologyData && topologyData.nodes) || [];
    if (rawNodes.length === 0) {
        d3.select("#topology").append("p")
            .style("text-align", "center")
            .style("padding", "50px")
            .text("No topology data available");
        return;
    }

    const width = document.getElementById('topology').clientWidth;
    const height = 600;

    // Create SVG
    topology_svg = d3.select("#topology")
        .append("svg")
        .attr("width", width)
        .attr("height", height);

    // Prepare nodes: switches and hosts share one list, keyed by the API's node id
    const nodes = rawNodes.map(n => ({
        id: String(n.id),
        name: n.label || String(n.id),
        type: n.type,
        x: width/2 + Math.random() * 100 - 50,
        y: height/2 + Math.random() * 100 - 50
    }));
    const knownIds = new Set(nodes.map(n => n.id));

    // Prepare links; skip any whose endpoint is not a known node (d3.forceLink would throw)
    const links = [];
    (topologyData.links || []).forEach(l => {
        const source = String(l.from);
        const target = String(l.to);
        if (!knownIds.has(source) || !knownIds.has(target)) return;
        links.push({ source: source, target: target, type: l.type });
    });

    // Create force simulation
    simulation = d3.forceSimulation(nodes)
        .force("link", d3.forceLink(links).id(d => d.id).distance(100))
        .force("charge", d3.forceManyBody().strength(-300))
        .force("center", d3.forceCenter(width / 2, height / 2));

    // Draw links
    const link = topology_svg.append("g")
        .selectAll("line")
        .data(links)
        .enter().append("line")
        .attr("stroke", d => d.type === 'host_link' ? "#999" : "#333")
        .attr("stroke-dasharray", d => d.type === 'host_link' ? "4 2" : null)
        .attr("stroke-width", 2);

    // Draw nodes
    const node = topology_svg.append("g")
        .selectAll("circle")
        .data(nodes)
        .enter().append("circle")
        .attr("r", d => d.type === 'switch' ? 15 : 8)
        .attr("fill", d => d.type === 'switch' ? "#1f77b4" : "#ff7f0e")
        .call(d3.drag()
            .on("start", dragstarted)
            .on("drag", dragged)
            .on("end", dragended));

    // Add labels
    const label = topology_svg.append("g")
        .selectAll("text")
        .data(nodes)
        .enter().append("text")
        .text(d => d.name)
        .attr("font-size", "12px")
        .attr("text-anchor", "middle")
        .attr("dy", "0.35em");

    // Update positions on tick
    simulation.on("tick", () => {
        link
            .attr("x1", d => d.source.x)
            .attr("y1", d => d.source.y)
            .attr("x2", d => d.target.x)
            .attr("y2", d => d.target.y);

        node
            .attr("cx", d => d.x)
            .attr("cy", d => d.y);

        label
            .attr("x", d => d.x)
            .attr("y", d => d.y);
    });

    // Drag functions
    function dragstarted(event, d) {
        if (!event.active) simulation.alphaTarget(0.3).restart();
        d.fx = d.x;
        d.fy = d.y;
    }

    function dragged(event, d) {
        d.fx = event.x;
        d.fy = event.y;
    }

    function dragended(event, d) {
        if (!event.active) simulation.alphaTarget(0);
        d.fx = null;
        d.fy = null;
    }
}

function updateLayout() {
    const layoutType = document.getElementById('layoutSelect').value;

    if (!simulation) return;

    const width = document.getElementById('topology').clientWidth;
    const height = 600;

    switch (layoutType) {
        case 'circular':
            simulation.force("charge", null);
            simulation.force("center", null);
            simulation.nodes().forEach((d, i) => {
                const angle = (i / simulation.nodes().length) * 2 * Math.PI;
                const radius = Math.min(width, height) / 3;
                d.fx = width/2 + radius * Math.cos(angle);
                d.fy = height/2 + radius * Math.sin(angle);
            });
            break;

        case 'hierarchical':
            simulation.force("charge", d3.forceManyBody().strength(-200));
            simulation.force("center", d3.forceCenter(width / 2, height / 2));
            const switches = simulation.nodes().filter(d => d.type === 'switch');
            const hosts = simulation.nodes().filter(d => d.type === 'host');

            switches.forEach((d, i) => {
                d.fx = width/2 + (i - switches.length/2) * 100;
                d.fy = height/3;
            });

            hosts.forEach((d, i) => {
                d.fx = width/2 + (i - hosts.length/2) * 80;
                d.fy = 2 * height/3;
            });
            break;

        case 'force':
        default:
            simulation.force("charge", d3.forceManyBody().strength(-300));
            simulation.force("center", d3.forceCenter(width / 2, height / 2));
            simulation.nodes().forEach(d => {
                d.fx = null;
                d.fy = null;
            });
            break;
    }

    simulation.alpha(1).restart();
}

// Initial load
refreshTopology();
