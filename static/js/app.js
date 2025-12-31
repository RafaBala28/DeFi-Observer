// Basic utilities
function $(sel){ return document.querySelector(sel); }
function formatNumber(n, decimals=2){ if(n===null||n===undefined||isNaN(n)) return 'N/A'; return Number(n).toLocaleString('en-US', {minimumFractionDigits:0, maximumFractionDigits:decimals}); }
function formatUSD(n){ if(n===null||n===undefined||isNaN(n)) return 'N/A'; return '$'+formatNumber(n); }
function shortAddr(a){ if(!a) return 'N/A'; return a.slice(0,6)+'…'+a.slice(-4); }
function fmtPct(n){ if(n===null||n===undefined||isNaN(n)) return 'N/A'; return Number(n).toFixed(2)+'%'; }

// Active chain (persisted in localStorage)
let activeChain = localStorage.getItem('activeChain') || 'ethereum';
const chainNames = {
        'ethereum': 'Ethereum',
        'arbitrum': 'Arbitrum',
        'optimism': 'Optimism',
        'base': 'Base'
};

// Helper to append current chain to API calls
function withChain(url){
        const sep = url.includes('?') ? '&' : '?';
        return `${url}${sep}chain=${encodeURIComponent(activeChain)}`;
}

function toggleEthereumOnlySections(){
        const isEthereum = activeChain === 'ethereum';

        const uni2Card = document.getElementById('uniswap-card');
        if(uni2Card){
                uni2Card.style.display = isEthereum ? '' : 'none';
        }

        const ethHeader = document.getElementById('eth-header');
        if(ethHeader){
                if(isEthereum){
                        if(!ethHeader.innerHTML){
                                ethHeader.innerHTML = '<div class="loading"><div class="spinner"></div>Loading Network…</div>';
                        }
                }else{
                        ethHeader.innerHTML = '<div class="muted">ETH network data only available on Ethereum.</div>';
                }
        }
}

// Chain Switcher with animated slider
function switchChain(chain) {
        if(chain === activeChain) return;
	
	activeChain = chain;
	localStorage.setItem('activeChain', chain);
	
	// Update active tab
	document.querySelectorAll('.chain-tab').forEach(tab => {
		tab.classList.remove('active');
		if(tab.getAttribute('data-chain') === chain) {
			tab.classList.add('active');
		}
	});
	
        // Animate slider
        updateChainSlider();

        // Show notification
        showNotification(`Switching to ${chainNames[chain]}...`, 'info');

        toggleEthereumOnlySections();

        // Reload all data for new chain
        setTimeout(() => {
                loadDashboardBatch(false);
                loadLiquidations(false);
                loadEthNetwork();
        }, 300);
}

// Update slider position
function updateChainSlider() {
	const activeTab = document.querySelector(`.chain-tab[data-chain="${activeChain}"]`);
	const slider = document.querySelector('.chain-slider');
	if(!activeTab || !slider) return;
	
	const tabsContainer = document.querySelector('.chain-tabs');
	const offsetLeft = activeTab.offsetLeft - tabsContainer.offsetLeft;
	const width = activeTab.offsetWidth;
	
	slider.style.width = `${width}px`;
	slider.style.transform = `translateX(${offsetLeft}px)`;
}

// Format current date/time for display (German locale)
function formatDateTimeNow(){
	try{
		return new Date().toLocaleString('en-US', { day: '2-digit', month: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' });
	}catch(e){
		return new Date().toISOString();
	}
}

// (Removed compare live-clock and footer — totals/last-updated are shown on the right only)

// Theme toggle
function initTheme(){
	const btn = $('#themeToggle');
	if(!btn) return;
	const apply = (t)=>document.documentElement.setAttribute('data-theme', t);
	const stored = localStorage.getItem('theme') || 'dark';
	apply(stored);
	if(stored==='light') btn.textContent = '🌙';
	btn.addEventListener('click', ()=>{
		const current = document.documentElement.getAttribute('data-theme');
		const newTheme = current === 'dark' ? 'light' : 'dark';
		apply(newTheme);
		localStorage.setItem('theme', newTheme);
		btn.textContent = newTheme === 'dark' ? '🌓' : '🌙';
	});
}

// ETH header
async function loadEthNetwork(){
        const el = $('#eth-header'); if(!el) return;
        if(activeChain !== 'ethereum'){
                el.innerHTML = '<div class="muted">ETH network data only available on Ethereum.</div>';
                return;
        }
        try{
                const r = await fetch('/api/eth/network');
                const d = await r.json();
                if(d.error){ throw new Error(d.error); }
				// Format with more precision for low values (< 1 gwei)
				const gas = d.gas_price_gwei!=null ? (d.gas_price_gwei < 1 ? d.gas_price_gwei.toFixed(2) : d.gas_price_gwei.toFixed(1))+' gwei' : '-';
				const base = d.avg_base_fee_gwei!=null ? (d.avg_base_fee_gwei < 1 ? d.avg_base_fee_gwei.toFixed(2) : d.avg_base_fee_gwei.toFixed(1))+' gwei' : '-';
		const blk = d.latest_block? d.latest_block : '-';

			// Keep a live copy of the latest network head for progress calculations
			state.latestBlock = d.latest_block || 0;
		el.innerHTML = `
			<div class="eth-grid">
				<div class="eth-item"><div class="eth-label">Gas</div><div class="eth-value">${gas}</div></div>
						<div class="eth-item"><div class="eth-label">Base Fee</div><div class="eth-value">${base}</div></div>
				<div class="eth-item"><div class="eth-label">Block</div><div class="eth-value mono">${blk}</div></div>
			</div>`;
	}catch(e){
		// Silent fail on updates - only show error on first load
		if(!el.innerHTML || el.innerHTML.includes('not available')){
			el.innerHTML = '<div class="error">ETH network not available</div>';
		}
	}
}

// ---------------- Dashboard Loaders ----------------
let uniswapChart = null;
let compareChart = null;
let currentHistoryRange = 24; // Aktuell gewählte Zeitspanne in Stunden
const state = { 
	latestAaveTVL: 0, 
	latestUniV3TVL: 0,
	ethPrice: 0,
	newLiquidationsCount: 0,
	liquidationCounts: []  // Speichert {timestamp, count}
};
let lastLiqCacheKey = '';
let lastLiqFilter = '100';

// Skeleton Loader HTML
function getSkeletonLoader(type='grid') {
	if(type === 'grid') {
		return `
			<div class="skeleton-card">
				<div class="skeleton skeleton-title"></div>
				<div class="skeleton-grid">
					<div class="skeleton skeleton-box"></div>
					<div class="skeleton skeleton-box"></div>
					<div class="skeleton skeleton-box"></div>
					<div class="skeleton skeleton-box"></div>
				</div>
			</div>`;
	} else if(type === 'list') {
		return `
			<div class="skeleton-card">
				<div class="skeleton skeleton-title"></div>
				<div class="skeleton skeleton-row"></div>
				<div class="skeleton skeleton-row"></div>
				<div class="skeleton skeleton-row"></div>
			</div>`;
	}
	return '<div class="skeleton skeleton-row"></div>';
}

// Batch-Loader für Dashboard (lädt alle Daten auf einmal)
async function loadDashboardBatch(silent = false) {
        const isEthereum = activeChain === 'ethereum';
        // Zeige Skeleton Loader nur beim ersten Laden
        const uni2 = $('#uniswap-content');
        const aaveReserves = $('#aave-reserves-content');
        const uni3 = $('#uniswap-v3-content');

        // 🔇 SILENT LOADING: Kein Skeleton mehr - existierende Daten bleiben sichtbar während Update
        // if(!silent){
        //         if(isEthereum && uni2) uni2.innerHTML = getSkeletonLoader('grid');
        //         if(aaveReserves) aaveReserves.innerHTML = getSkeletonLoader('list');
        //         if(uni3) uni3.innerHTML = getSkeletonLoader('list');
        // }
	
        try {
                const r = await fetch(withChain('/api/dashboard/summary'));
                const d = await r.json();

                if(d.error) throw new Error(d.error);

                // Aktive Chain anzeigen (für Fehlermeldungen und Debug)
                const chainLabel = chainNames[d.chain] || chainNames[activeChain] || activeChain;
		
		// Render Uniswap V2
                if(isEthereum && uni2) {
                        const data = d.uniswap_v2;
                        if(data && data.error) {
                                uni2.innerHTML = `<div class="muted">Uniswap V2 is not available on ${chainLabel}.</div>`;
                        } else if(data) {
                                uni2.innerHTML = `
                                        <div class="stat-grid">
                                                <div class="stat-box"><div class="stat-label">ETH Reserve</div><div class="stat-value">${formatNumber(data.eth_reserve)} <span class="muted">ETH</span></div></div>
                                                <div class="stat-box"><div class="stat-label">USDC Reserve</div><div class="stat-value">${formatNumber(data.usdc_reserve)} <span class="muted">USDC</span></div></div>
                                                <div class="stat-box"><div class="stat-label">ETH Price</div><div class="stat-value">${formatUSD(data.eth_price)}</div></div>
                                                <div class="stat-box"><div class="stat-label">TVL</div><div class="stat-value">${formatUSD(data.tvl_usd)}</div></div>
                                        </div>
                                        <div class="timestamp">${data.timestamp || ''}</div>`;
                                state.ethPrice = data.eth_price || 0;
                                appendUniswapChartPoint(Date.now(), data.tvl_usd||0);
                                updatePriceChart();
                        }
                }
		
		// Render Aave V3 Reserves
                if(d.aave && aaveReserves) {
                        const data = d.aave;
                        state.latestAaveTVL = data.total_tvl_usd || data.total_liquidity_usd || 0;
			const assets = (data.assets||[]).map(a=>`
				<div class="asset-item">
					<div class="asset-header"><div class="asset-name">${a.name}</div><div class="asset-price">${formatUSD(a.price_usd)}</div></div>
					<div class="asset-stats">
						<div class="asset-stat"><div class="stat-label">Liquidity</div><div class="stat-value">${formatUSD(a.liquidity_usd)}</div></div>
						<div class="asset-stat"><div class="stat-label">Borrowed</div><div class="stat-value">${formatUSD(a.borrowed_usd)}</div></div>
						<div class="asset-stat"><div class="stat-label">Deposit APY</div><div class="stat-value">${fmtPct(a.deposit_apy)}</div></div>
						<div class="asset-stat"><div class="stat-label">Borrow APY</div><div class="stat-value">${fmtPct(a.borrow_apy)}</div></div>
					</div>
				</div>`).join('');
			aaveReserves.innerHTML = assets || '<div class="muted">No assets found</div>';
		}
		
		// Render Uniswap V3
		if(d.uniswap_v3 && uni3) {
			const data = d.uniswap_v3;
			const pools = (data.pools||[]).slice(0,5);
			let total = 0;
			const rows = pools.map(p=>{ 
				total += (p.tvl_usd||0); 
				return `
				<div class="asset-item">
					<div class="asset-header">
						<div class="asset-name">${p.pair} <span class="muted">· ${p.fee? (p.fee/10000).toFixed(2)+'%':''}${p.pool_price ? ` · ${p.pool_price}` : ''}</span></div>
						<div class="asset-value">${formatUSD(p.tvl_usd)}</div>
					</div>
					<div class="asset-stats">
						<div class="asset-stat"><div class="stat-value">${formatNumber(p.token0?.amount)} <span class="muted">${p.token0?.symbol||''}</span></div></div>
						<div class="asset-stat"><div class="stat-value">${formatNumber(p.token1?.amount)} <span class="muted">${p.token1?.symbol||''}</span></div></div>
					</div>
				</div>`; 
			}).join('');
			state.latestUniV3TVL = total;
			uni3.innerHTML = rows || '<div class="muted">No pools found</div>';
			updatePriceChart();
		}
		
	} catch(e) {
		console.error('Dashboard Batch Load Error:', e);
		// Silent fail bei Updates - nur beim ersten Laden Fallback
		if(!silent){
			// Fallback: Lade einzeln
			loadUniswapData();
			loadAaveData();
			loadUniswapV3Data();
		}
	}
}

async function loadUniswapData(){
        const el = $('#uniswap-content'); if(!el) return;
        if(activeChain !== 'ethereum') {
                el.innerHTML = `<div class="muted">Uniswap V2 is not available on ${chainNames[activeChain] || activeChain}.</div>`;
                return;
        }
        try{
                const r = await fetch(withChain('/api/uniswap'));
                const d = await r.json();
                if(d.error){ throw new Error(d.error); }
		el.innerHTML = `
			<div class="stat-grid">
				<div class="stat-box"><div class="stat-label">ETH Reserve</div><div class="stat-value">${formatNumber(d.eth_reserve)} <span class="muted">ETH</span></div></div>
				<div class="stat-box"><div class="stat-label">USDC Reserve</div><div class="stat-value">${formatNumber(d.usdc_reserve)} <span class="muted">USDC</span></div></div>
				<div class="stat-box"><div class="stat-label">ETH Price</div><div class="stat-value">${formatUSD(d.eth_price)}</div></div>
				<div class="stat-box"><div class="stat-label">TVL</div><div class="stat-value">${formatUSD(d.tvl_usd)}</div></div>
			</div>
			<div class="timestamp">${d.timestamp || ''}</div>`;
		// Store ETH price for chart
		state.ethPrice = d.eth_price || 0;
		appendUniswapChartPoint(Date.now(), d.tvl_usd||0);
		updatePriceChart();
	}catch(e){
		el.innerHTML = `<div class="error">Uniswap V2 not available</div>`;
	}
}

function ensureUniswapChart(){
	if(uniswapChart) return uniswapChart;
	const ctx = document.getElementById('uniswapChart'); if(!ctx) return null;
	const empty = document.getElementById('uniswapEmpty'); if(empty) empty.style.display='flex';
	uniswapChart = new Chart(ctx, {
		type:'line',
		data:{ datasets:[{ label:'TVL (USD)', data:[], borderColor:'#8b5cf6', backgroundColor:'rgba(139,92,246,0.15)', tension:0.25, fill:true, pointRadius:0 } ] },
		options:{ responsive:true, maintainAspectRatio:false, scales:{ x:{ type:'time', time:{ unit:'minute' }, grid:{ color:'rgba(255,255,255,0.06)' } }, y:{ grid:{ color:'rgba(255,255,255,0.06)' }, ticks:{ callback:(v)=>'$'+formatNumber(v) } } }, plugins:{ legend:{ display:false } } }
	});
	return uniswapChart;
}

function appendUniswapChartPoint(t, y){
	const chart = ensureUniswapChart(); if(!chart) return;
	if(chart.data.datasets[0].data.length===0){ const empty = document.getElementById('uniswapEmpty'); if(empty) empty.style.display='none'; }
	chart.data.datasets[0].data.push({x:t, y:y});
	// keep last 200 points
	if(chart.data.datasets[0].data.length > 200){ chart.data.datasets[0].data.shift(); }
	chart.update('none');
}

async function loadAaveData(){
        const el = $('#aave-content'); if(!el) return;
        try{
                const r = await fetch(withChain('/api/aave'));
                const d = await r.json();
                if(d.error){ throw new Error(d.error); }
		state.latestAaveTVL = d.total_tvl_usd || d.total_liquidity_usd || 0;
		const assets = (d.assets||[]).slice(0,6).map(a=>`
			<div class="asset-item">
				<div class="asset-header"><div class="asset-name">${a.name}</div><div class="asset-price">${formatUSD(a.price_usd)}</div></div>
				<div class="asset-stats-2x2">
					 <div class="asset-stat"><div class="stat-label">Liquidity</div><div class="stat-value">${formatUSD(a.liquidity_usd)}</div></div>
					 <div class="asset-stat"><div class="stat-label">Borrowed</div><div class="stat-value">${formatUSD(a.borrowed_usd)}</div></div>
					 <div class="asset-stat"><div class="stat-label">Deposit APY</div><div class="stat-value">${fmtPct(a.deposit_apy)}</div></div>
					 <div class="asset-stat"><div class="stat-label">Borrow APY</div><div class="stat-value">${fmtPct(a.borrow_apy)}</div></div>
				</div>
			</div>`).join('');
		el.innerHTML = `
			<div class="stat-grid">
				<div class="stat-box"><div class="stat-label">Total Liquidity</div><div class="stat-value">${formatUSD(d.total_liquidity_usd)}</div></div>
				<div class="stat-box"><div class="stat-label">Total Borrowed</div><div class="stat-value">${formatUSD(d.total_borrowed_usd)}</div></div>
				<div class="stat-box"><div class="stat-label">TVL</div><div class="stat-value">${formatUSD(d.total_tvl_usd)}</div></div>
				<div class="stat-box"><div class="stat-label">Avg Utilization</div><div class="stat-value">${fmtPct(d.avg_utilization)}</div></div>
			</div>
			<div class="asset-list">${assets}</div>`;
	}catch(e){ el.innerHTML = `<div class="error">Aave V3 not available</div>`; }
}

async function loadUniswapV3Data(){
        const el = $('#uniswap-v3-content'); if(!el) return;
        try{
                const r = await fetch(withChain('/api/uniswap_v3'));
                const d = await r.json();
                if(d.error){ throw new Error(d.error); }
		const pools = (d.pools||[]).slice(0,5);
		let total = 0; const rows = pools.map(p=>{ total += (p.tvl_usd||0); return `
			<div class="asset-item">
				<div class="asset-header"><div class="asset-name">${p.pair} <span class="muted">· ${p.fee? (p.fee/10000).toFixed(2)+'%':''}</span></div></div>
				<div class="asset-stats">
					<div class="asset-stat"><div class="stat-label">Token0</div><div class="stat-value">${formatNumber(p.token0?.amount)} <span class="muted">${p.token0?.symbol||''}</span></div></div>
					<div class="asset-stat"><div class="stat-label">Token1</div><div class="stat-value">${formatNumber(p.token1?.amount)} <span class="muted">${p.token1?.symbol||''}</span></div></div>
				</div>
				<div class="muted" style="margin-top:8px">TVL: ${formatUSD(p.tvl_usd)}</div>
			</div>`; }).join('');
		state.latestUniV3TVL = total;
		el.innerHTML = rows || '<div class="muted">No pools found</div>';
		updatePriceChart();
	}catch(e){ el.innerHTML = `<div class="error">Uniswap V3 not available</div>`; }
}

function ensureCompareChart(){
	if(compareChart) return compareChart;
	const ctx = document.getElementById('compareChart'); if(!ctx) return null;
	const empty = document.getElementById('compareEmpty'); if(empty) empty.style.display='flex';
	compareChart = new Chart(ctx, {
		type:'bar',
		data:{ datasets:[
			{
				label:'ETH Preis (USD)', 
				data:[], 
				type: 'line',
				borderColor:'#22d3ee', 
				backgroundColor:'rgba(34,211,238,0.15)', 
				tension:0.25, 
				fill:true, 
				pointRadius:2,
				yAxisID: 'y'
			},
			{
				label:'Liquidations per hour', 
				data:[], 
				type: 'bar',
				backgroundColor:'rgba(244,63,94,0.7)', 
				borderColor:'#f43f5e',
				borderWidth: 1,
				barPercentage: 0.9,
				categoryPercentage: 0.95,
				yAxisID: 'y1'
			}
		]},
		options:{ 
			responsive:true, 
			maintainAspectRatio:false, 
			interaction: {
				mode: 'index',
				intersect: false,
			},
			scales:{ 
				x:{ 
					type:'time', 
					time:{ 
						unit:'hour',  // Hauptbeschriftung bleibt stündlich
						displayFormats: {
							minute: 'HH:mm',  // Stunden-Punkte formatieren
							hour: 'HH:mm',
							day: 'DD.MM'
						},
						stepSize: 1  // Zeige jede Stunde
					}, 
					ticks: {
						maxTicksLimit: 12,  // Max 12 Stunden-Labels sichtbar
						autoSkip: true
					},
					grid:{ color:'rgba(255,255,255,0.06)'} 
				}, 
				y:{ 
					type: 'linear',
					display: true,
					position: 'left',
					beginAtZero: false,
					grace: '10%',
					grid:{ color:'rgba(255,255,255,0.06)' }, 
					ticks:{ 
						callback: function(value) {
							return '$' + value.toLocaleString('en-US', {maximumFractionDigits: 0});
						}
					},
					title: {
						display: true,
						text: 'ETH Preis (USD)',
						color: '#22d3ee'
					}
				},
				y1: {
					type: 'linear',
					display: true,
					position: 'right',
					beginAtZero: true,
					grid: {
						drawOnChartArea: false,
					},
					ticks:{ 
						callback:(v)=>Math.round(v),
						stepSize: 1
					},
					title: {
						display: true,
						text: 'Liquidations (per hour)',
						color: '#f43f5e'
					}
				}
			}, 
			plugins:{ 
				legend:{ 
					display:true, 
					labels:{ color:'#cbd5e1' } 
				},
				tooltip: {
					callbacks: {
						title: function(context) {
							const date = new Date(context[0].parsed.x);
							return date.toLocaleString('en-US', { 
								day: '2-digit', 
								month: '2-digit', 
								hour: '2-digit', 
								minute: '2-digit' 
							});
						}
					}
				},
				// ZOOM & PAN Plugin - horizontales Scrollen/Zoomen
				zoom: {
					pan: {
						enabled: true,
						mode: 'x',  // Nur horizontal
						modifierKey: null,  // Kein Modifier nötig (immer aktiv)
					},
					zoom: {
						wheel: {
							enabled: true,
							speed: 0.1
						},
						pinch: {
							enabled: true
						},
						mode: 'x',  // Nur horizontal zoomen
					},
					limits: {
						x: { min: 'original', max: 'original' }  // Limitiere auf Datensatz-Bereich
					}
				}
			} 
		}
	});
	return compareChart;
}

async function loadHistoricalData(hours = 24){
	const empty = document.getElementById('compareEmpty');
	const subtitle = document.getElementById('chartSubtitle');
	if(empty) {
		empty.style.display = 'flex';
		empty.innerHTML = 'Loading historical data…';
		empty.style.color = '#a1a1aa';
	}

	console.log(`📊 Loading ${hours}h history...`);

	try{
		// Mapping: hours -> timeWindow for backend
		const timeWindowMap = {
			1: '1h',
			6: '6h',
			24: '24h',
			168: '7d',
			720: '30d'
		};
		
		const timeWindow = timeWindowMap[hours] || null;
		
		// Always use timeWindow if available (backend is optimized for this)
		const url = timeWindow 
			? `/api/history/eth_price_liquidations?timeWindow=${timeWindow}`
			: `/api/history/eth_price_liquidations?hours=${hours}`;
		
		console.log(`Fetching: ${url}`);

		// Load combined history (ETH price + liquidations) with optimized aggregation
		const r = await fetch(url);
		const d = await r.json();
		
		console.log(`📦 Received data:`, d.stats || {});

		const c = ensureCompareChart(); if(!c) return;

		// Determine time unit, stepSize and maxTicks based on time range
		let timeUnit, stepSize, maxTicks, displayFormats;
		
		if(hours <= 1) {
			// 1 hour: Show every minute
			timeUnit = 'minute';
			stepSize = 5;  // Every 5 minutes
			maxTicks = 12;
			displayFormats = { minute: 'HH:mm' };
		} else if(hours <= 6) {
			// 6 hours: Show every 30 minutes
			timeUnit = 'minute';
			stepSize = 30;
			maxTicks = 12;
			displayFormats = { minute: 'HH:mm' };
		} else if(hours <= 24) {
			// 24 hours: Show every 2 hours
			timeUnit = 'hour';
			stepSize = 2;
			maxTicks = 12;
			displayFormats = { hour: 'HH:mm' };
		} else if(hours <= 168) {
			// 7 days: Show every day
			timeUnit = 'day';
			stepSize = 1;
			maxTicks = 7;
			displayFormats = { day: 'MMM DD', hour: 'MMM DD' };
		} else {
			// 30 days: Show every 2-3 days
			timeUnit = 'day';
			stepSize = 2;
			maxTicks = 15;
			displayFormats = { day: 'MMM DD', week: 'MMM DD' };
		}

		// Update Chart configuration dynamically
		c.options.scales.x.time.unit = timeUnit;
		c.options.scales.x.time.stepSize = stepSize;
		c.options.scales.x.time.displayFormats = displayFormats;
		c.options.scales.x.ticks.maxTicksLimit = maxTicks;
		c.options.scales.x.ticks.autoSkip = true;

		// --- Synchronize time axes ---
		const priceSeries = d.eth_price_series || [];
		const liqSeries = d.liquidation_series || [];

		// Convert timestamps to milliseconds for Chart.js
		const priceData = priceSeries.map(p => ({ 
			x: p.t * 1000,  // Backend sends seconds, Chart.js needs milliseconds
			y: p.eth_price 
		}));
		
		const liqData = liqSeries.map(l => ({ 
			x: l.x,  // Already in milliseconds from backend
			y: l.y 
		}));

		c.data.datasets[0].data = priceData;
		c.data.datasets[1].data = liqData;

		// Subtitle removed - stats shown in console
		if(subtitle){
			subtitle.textContent = '';
		}

		if(empty) {
			if (priceData.length === 0 && liqData.length === 0) {
				empty.style.display = 'flex';
				empty.innerHTML = 'No data in selected time range.';
				empty.style.color = '#a1a1aa';
			} else {
				empty.style.display = 'none';
			}
		}

		c.update();
		
		console.log(`✅ Chart updated: ${priceData.length} price points, ${liqData.length} liquidation bars`);

	}catch(e){
		console.error('❌ Error loading historical data:', e);
		if(empty) {
			empty.innerHTML = 'Error loading data - check console';
			empty.style.color = '#ef4444';
		}
	}
}

function changeHistoryRange(){
	const select = document.getElementById('historyRangeSelect');
	const hours = parseInt(select.value);
	currentHistoryRange = hours;
	
	const empty = document.getElementById('compareEmpty');
	if(empty) {
		empty.style.display = 'flex';
		empty.innerHTML = 'Loading data…';
		empty.style.color = '#a1a1aa';
	}
	
	console.log(`🔄 Time range changed: ${hours}h`);
	loadHistoricalData(hours);
}

function updatePriceChart(){
	const c = ensureCompareChart(); if(!c) return;
	const now = Date.now();
	
	// Nur hinzufügen wenn wir neue Daten haben
	if(!state.ethPrice && !state.newLiquidationsCount) return;
	
	// ETH Preis-Punkt hinzufügen (nur wenn wir einen neuen Preis haben)
	if(state.ethPrice > 0){
		// Prüfe ob wir schon einen Punkt in der letzten Minute haben
		const lastPoint = c.data.datasets[0].data[c.data.datasets[0].data.length - 1];
		const oneMinute = 60 * 1000;
		
		if(!lastPoint || (now - lastPoint.x) >= oneMinute){
			// Füge neuen Punkt hinzu
			c.data.datasets[0].data.push({x: now, y: state.ethPrice});
			
			if(c.data.datasets[0].data.length === 1){
				const empty = document.getElementById('compareEmpty'); 
				if(empty) empty.style.display='none'; 
			}
			
			// Keep last 1440 points (24h bei 1min Intervall)
			if(c.data.datasets[0].data.length > 1440){ 
				c.data.datasets[0].data.shift(); 
			}
		}
	}
	
	// Liquidationen nach Stunde gruppieren
	if(state.newLiquidationsCount > 0){
		const hour = Math.floor(now / 3600000) * 3600000; // Runde auf volle Stunde
		const existingBar = c.data.datasets[1].data.find(d => d.x === hour);
		if(existingBar){
			// Stunde existiert schon - addiere Liquidationen
			existingBar.y += state.newLiquidationsCount;
		} else {
			// Neue Stunde - erstelle neuen Balken
			c.data.datasets[1].data.push({x: hour, y: state.newLiquidationsCount});
			// Sortiere nach Zeit
			c.data.datasets[1].data.sort((a, b) => a.x - b.x);
		}
		
		// Keep last 48 bars für Liquidationen (48 Stunden)
		if(c.data.datasets[1].data.length > 48){ 
			c.data.datasets[1].data.shift(); 
		}
	}
	
	// Reset counter nach Update
	state.newLiquidationsCount = 0;
	
	c.update('none');
}

function appendComparePoint(t, uni, aave){
	// Legacy function - jetzt durch updatePriceChart ersetzt
	updatePriceChart();
}

async function loadUniExtended(){
        const el = $('#uni-ext-content'); if(!el) return;
        if(activeChain !== 'ethereum'){
                el.innerHTML = '<div class="muted">Uniswap Extended is only available on Ethereum.</div>';
                return;
        }
        try{
                const r = await fetch('/api/uniswap/extended');
                const d = await r.json();
                if(d.error){ throw new Error(d.error); }
                el.innerHTML = `
			<div class="stat-grid">
				<div class="stat-box"><div class="stat-label">Price ETH/USD</div><div class="stat-value">${formatUSD(d.price_eth_usd)}</div></div>
				<div class="stat-box"><div class="stat-label">TVL</div><div class="stat-value">${formatUSD(d.tvl_usd)}</div></div>
				<div class="stat-box"><div class="stat-label">Vol 24h</div><div class="stat-value">${formatUSD(d.volume_24h_usd)}</div></div>
				<div class="stat-box"><div class="stat-label">Fee</div><div class="stat-value">${(d.fee_tier/10000).toFixed(2)}%</div></div>
			</div>`;
	}catch(e){ el.innerHTML = `<div class="error">Uniswap Extended not available</div>`; }
}

async function loadAaveRisk(){
        const el = $('#aave-risk-content'); if(!el) return;
        if(activeChain !== 'ethereum'){
                el.innerHTML = '<div class="muted">Aave Risk Monitor is only available on Ethereum.</div>';
                return;
        }
        try{
                const r = await fetch('/api/aave/risk');
                const d = await r.json();
                if(d.error){ throw new Error(d.error); }
                const rows = (d.assets||[]).slice(0,8).map(a=>`<div class="asset-stat"><div class="stat-label">${a.symbol}</div><div class="stat-value">LTV ${fmtPct(a.ltv)}</div><div class="muted">Liq. Th: ${fmtPct(a.liq_threshold)} · Util: ${fmtPct(a.utilization)}</div></div>`).join('');
		el.innerHTML = `<div class="asset-stats" style="grid-template-columns:repeat(2,1fr)">${rows}</div>`;
	}catch(e){ el.innerHTML = `<div class="error">Aave Risk Monitor not available</div>`; }
}

function showLiqModal(liq){
  const modal = $('#liqModal'); if(!modal) return;
  const body = $('#liqModalBody'); if(!body) return;
		// Silent mode - no visual feedback
  const dt = liq.time ? new Date(liq.time*1000).toLocaleString('en-US', {dateStyle:'medium',timeStyle:'medium'}) : 'Unknown';
  // Werte kommen bereits als Token-Mengen (nicht Wei!)
  const collOut = Number(liq.collateralOut||0).toFixed(8);
  const debtCov = Number(liq.debtToCover||0).toFixed(8);
  
  body.innerHTML = `
    <div class="detail-row"><div class="detail-label">Time</div><div class="detail-value">${dt}</div></div>
    <div class="detail-row"><div class="detail-label">Block</div><div class="detail-value">${liq.block||'—'}</div></div>
    <div class="detail-row"><div class="detail-label">User</div><div class="detail-value"><a href="https://etherscan.io/address/${liq.user||''}" target="_blank" rel="noopener">${liq.user||'—'}</a></div></div>
    <div class="detail-row"><div class="detail-label">Liquidator</div><div class="detail-value"><a href="https://etherscan.io/address/${liq.liquidator||''}" target="_blank" rel="noopener">${liq.liquidator||'—'}</a></div></div>
    <div class="detail-row"><div class="detail-label">Collateral Asset</div><div class="detail-value"><a href="https://etherscan.io/token/${liq.collateralAsset||''}" target="_blank" rel="noopener">${shortAddr(liq.collateralAsset||'')}</a></div></div>
    <div class="detail-row"><div class="detail-label">Collateral Amount</div><div class="detail-value">${collOut}</div></div>
    <div class="detail-row"><div class="detail-label">Debt Asset</div><div class="detail-value"><a href="https://etherscan.io/token/${liq.debtAsset||''}" target="_blank" rel="noopener">${shortAddr(liq.debtAsset||'')}</a></div></div>
    <div class="detail-row"><div class="detail-label">Debt Covered</div><div class="detail-value">${debtCov}</div></div>
    <div class="detail-row"><div class="detail-label">Receive AToken?</div><div class="detail-value">${liq.receiveAToken?'Yes':'No'}</div></div>
    <div class="detail-row"><div class="detail-label">Transaction</div><div class="detail-value"><a href="https://etherscan.io/tx/${liq.tx||''}" target="_blank" rel="noopener">${shortAddr(liq.tx||'')}</a></div></div>
  `;
  modal.classList.add('show');
}

function closeLiqModal(){
  const modal = $('#liqModal'); if(modal) modal.classList.remove('show');
}

function downloadLiquidationsExcel(){
  // Exportiere aktuelle Liquidationen als Excel (CSV mit Tab-Trennung für Excel)
  const items = window._liqData || [];
  if(items.length === 0){
    alert('No liquidations to export.');
    return;
  }
  
  // CSV Header mit Windows-Zeilenumbruch - jede Spalte separat
  let csv = 'Zeit;Block;User;Liquidator;Collateral Asset;Collateral Symbol;Collateral Out;Debt Asset;Debt Symbol;Debt ToCover;Receive AToken;Transaktion\r\n';
  
  // Daten - jede Liquidation in einer eigenen Zeile
  items.forEach(item => {
    if(item.error) return; // Skip fehlerhafte Items
    const time = item.time ? new Date(item.time*1000).toLocaleString('en-US') : '';
    // Werte kommen bereits als Token-Mengen (nicht Wei!)
    const collOut = Number(item.collateralOut||0).toFixed(8);
    const debtCov = Number(item.debtToCover||0).toFixed(8);
    
    // Jede Zeile mit \r\n (Windows-Format) beenden - Semikolon als Trennzeichen
    const row = [
      time,
      item.block || '',
      item.user || '',
      item.liquidator || '',
      item.collateralAsset || '',
      item.collateralSymbol || '',
      collOut,
      item.debtAsset || '',
      item.debtSymbol || '',
      debtCov,
      item.receiveAToken ? 'Ja' : 'Nein',
      item.tx || ''
    ].join(';');
    
    csv += row + '\r\n';
  });
  
  // Download mit BOM für Excel-Kompatibilität
  const BOM = '\uFEFF';
  const blob = new Blob([BOM + csv], { type: 'text/csv;charset=utf-8;' });
  const link = document.createElement('a');
  const url = URL.createObjectURL(blob);
  const now = new Date();
  const timestamp = now.toISOString().slice(0,19).replace(/[T:]/g, '-');
  link.setAttribute('href', url);
  link.setAttribute('download', `aave_liquidations_${timestamp}.csv`);
  link.style.visibility = 'hidden';
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}

async function loadLiquidations(silent = false){
  const el = $('#liquidations-content'); if(!el) return;
  try{
    // Zeige animierten Loader beim Laden (außer bei silent Auto-Updates)
    if(!silent){
      el.innerHTML = `
        <div class="table-loader">
          <div class="loader-spinner"></div>
          <div class="loader-text">Loading liquidations...</div>
        </div>
      `;
    }
    
    // Hole Liquidationen basierend auf Filter
    const filterEl = $('#liqTimeFilter');
    const filterValue = filterEl ? filterEl.value : '100';
    lastLiqFilter = filterValue;
    
    let url;
    if(filterValue === 'all'){
      // Alle Liquidationen seit Scan-Start
      url = `/api/aave/liquidations/recent?blocks=0&limit=10000&forced_only=0&since_start=1`;
    } else if(filterValue === '100'){
      // Letzte 100 (keine Zeit-Filterung)
      url = `/api/aave/liquidations/recent?blocks=0&limit=100&forced_only=0&since_start=0`;
    } else {
      // Zeitbasiert (z.B. 1h, 6h, 24h, 7d, 30d)
      const hours = parseInt(filterValue);
      url = `/api/aave/liquidations/recent?blocks=0&limit=10000&forced_only=0&since_start=0&hours=${hours}`;
    }
    
    const r = await fetch(url);
    const d = await r.json();
    renderLiquidations(d, !silent);
    return Promise.resolve(d); // Return data for chaining
  }catch(e){
    console.error('Liquidations laden fehlgeschlagen:', e);
    if(!silent){
      // Kein Error anzeigen, lass leere Tabelle stehen
      // el.innerHTML = `<div class="error">Error loading der Liquidationen</div>`;
    }
    return Promise.reject(e);
  }
}

function changeLiqTimeFilter(){
  loadLiquidations(false);
}

// Auto-refresh Liquidationen (für Live-Updates, ressourcenschonend)
let lastLiqCount = 0;
let lastLiqUpdateTime = 0;
const LIQ_REFRESH_INTERVAL = 20000;
setInterval(() => {
  // Skip teure Auto-Updates, wenn alle Events angezeigt werden
  if(lastLiqFilter === 'all') {
    return;
  }
  loadLiquidations(true).then((data) => {
    // Prüfe ob new liquidation(s) hinzugekommen sind
    // Nutze direkt stats.total_count aus API Response
    if(data && data.stats && data.stats.total_count) {
      const newCount = data.stats.total_count;
      const hasNewData = newCount !== lastLiqCount;
      
      if(newCount > lastLiqCount && lastLiqCount > 0) {
        // new liquidation(s)! Zeige visuelles Feedback
        const diff = newCount - lastLiqCount;
        showNotification(`+${diff} neue Liquidation${diff > 1 ? 'en' : ''}!`, 'success');
      }
      
      lastLiqCount = newCount;
      
      // Update nur wenn neue Daten da sind
      if(hasNewData) {
        lastLiqUpdateTime = Date.now();
      }
    }
  }).catch(err => {
    console.error('Auto-reload failed:', err);
  });
}, LIQ_REFRESH_INTERVAL); // Alle 20 Sekunden

// Funktion für Benachrichtigungen
function showNotification(msg, type = 'info') {
  const notif = document.createElement('div');
  const bgColor = type === 'success' ? '#10b981' : (type === 'error' ? '#ef4444' : '#3b82f6');
  notif.style.cssText = 'position:fixed;top:80px;right:20px;background:' + bgColor + ';color:white;padding:12px 20px;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.3);z-index:9999;animation:slideIn 0.3s ease-out';
  notif.textContent = msg;
  document.body.appendChild(notif);
  setTimeout(() => {
    notif.style.animation = 'slideOut 0.3s ease-out';
    setTimeout(() => notif.remove(), 300);
  }, 3000);
}

function downloadLiquidationsCSV(){
  const filterEl = $('#liqTimeFilter');
  const filterValue = filterEl ? filterEl.value : 'all';
  
  // Timestamp für Dateiname: YYYY-MM-DD_HH-MM-SS
  const now = new Date();
  const timestamp = now.toISOString().slice(0, 19).replace('T', '_').replace(/:/g, '-');
  
  let url;
  if(filterValue === 'all'){
    url = `/api/aave/liquidations/export?timestamp=${encodeURIComponent(timestamp)}`;
  } else if(filterValue === '100'){
    url = `/api/aave/liquidations/export?limit=100&timestamp=${encodeURIComponent(timestamp)}`;
  } else {
    const hours = parseInt(filterValue);
    url = `/api/aave/liquidations/export?hours=${hours}&timestamp=${encodeURIComponent(timestamp)}`;
  }
  
  // Trigger Download
  window.open(url, '_blank');
}function renderLiquidations(d, forceRender = false){
  const el = $('#liquidations-content'); if(!el) return;
  const items = d.items||[];
  
  // Cache entfernt für Live-Updates - Tabelle wird bei jedem neuen Event sofort aktualisiert
  
  // Speichere Scroll-Position vor dem Update
  const tableWrapper = el.querySelector('.table-wrapper');
  const scrollTop = tableWrapper ? tableWrapper.scrollTop : 0;
  
  if(items.length===0 || (items[0] && items[0].error)){
    const errInfo = (d.errors && d.errors.length) ? `<div class="muted" style="margin-top:6px">Note: ${d.errors[0]}</div>` : '';
    el.innerHTML = `<div class="muted">No liquidations found.</div>` + errInfo;
    return;
  }
  const hdr = `<div class="table-wrapper"><table class="table"><thead><tr>
    <th>Time</th>
    <th>Block</th>
    <th>User</th>
    <th>Collateral</th>
    <th>Amount</th>
    <th>Debt</th>
    <th>Amount</th>
    <th>Tx</th>
  </tr></thead><tbody>`;
  const rows = items.map((x,i)=>{
    // Zeit + Datum kompakt (2 Zeilen)
    const dateObj = x.time ? new Date(x.time*1000) : null;
    const dateStr = dateObj ? dateObj.toLocaleDateString('en-US') : '';
    const timeStr = dateObj ? dateObj.toLocaleTimeString('en-US', {hour:'2-digit', minute:'2-digit'}) : '';
    
    // Zeige Token-Symbole statt Adressen
    const collLabel = (x.collateralSymbol && !x.collateralSymbol.startsWith('0x')) ? x.collateralSymbol : shortAddr(x.collateralAsset||'');
    const debtLabel = (x.debtSymbol && !x.debtSymbol.startsWith('0x')) ? x.debtSymbol : shortAddr(x.debtAsset||'');
    
    // collateralOut und debtToCover kommen bereits als Token-Mengen
    const collOut = Number(x.collateralOut || 0);
    const debtCov = Number(x.debtToCover || 0);
    
    return `<tr class="liq-row-clickable" onclick="showLiqModal(window._liqData[${i}])">
      <td class="muted"><div>${dateStr}</div><div class="time-small">${timeStr}</div></td>
      <td class="mono">${x.block||''}</td>
      <td class="mono">${shortAddr(x.user||'')}</td>
      <td class="mono token-label">${collLabel}</td>
      <td class="mono num-col">${formatNumber(collOut, 4)}</td>
      <td class="mono token-label">${debtLabel}</td>
      <td class="mono num-col">${formatNumber(debtCov, 4)}</td>
      <td class="mono tx-link"><a href="https://etherscan.io/tx/${x.tx}" target="_blank" rel="noopener" onclick="event.stopPropagation()">${shortAddr(x.tx||'')}</a></td>
    </tr>`; }).join('');
  
	el.innerHTML = hdr + rows + '</tbody></table></div>';
  
  // Stelle Scroll-Position wieder her
  const newTableWrapper = el.querySelector('.table-wrapper');
  if(newTableWrapper && scrollTop > 0) {
    newTableWrapper.scrollTop = scrollTop;
  }
  
  window._liqData = items;
	// Update summary area with total count + last updated
	const countEl = document.getElementById('liqCount');
	if(countEl){
				// Show REAL total count from stats (not just filtered items)
			const totalStored = (d.stats && d.stats.total_count) ? d.stats.total_count : items.length;
			const displayedCount = items.length;
			const countText = (totalStored > displayedCount) 
				? `Showing: ${displayedCount} of ${totalStored} liquidations` 
				: `Total: ${totalStored} liquidations`;
			countEl.textContent = `${countText} · Last updated: ${formatDateTimeNow()}`;
	}
	
	// Scan-Status wurde entfernt - komplett silent
}// MetaMask Wallet Connection
async function connectWallet() {
	const btn = $('#walletConnectBtn');
	const btnText = $('#walletBtnText');
	if(!btn || !btnText) return;
	
	if(typeof window.ethereum === 'undefined') {
		showNotification('MetaMask nicht installiert. Bitte installieren Sie MetaMask.', 'error');
		window.open('https://metamask.io/download/', '_blank');
		return;
	}
	
	try {
		// Request account access
		const accounts = await ethereum.request({ method: 'eth_requestAccounts' });
		if(accounts && accounts.length > 0) {
			const address = accounts[0];
			// Auto-fill wallet input
			const input = $('#walletInput');
			if(input) input.value = address;
			// Update button state
			btn.classList.add('connected');
			btnText.textContent = `✓ ${shortAddr(address)}`;
			showNotification('Wallet successfully connected!', 'success');
			
			// Auto-analyze wallet and show DeFi positions
			await loadConnectedWalletPositions(address);
		}
	} catch(error) {
		console.error('MetaMask connection error:', error);
		if(error.code === 4001) {
			showNotification('Connection rejected', 'error');
		} else {
			showNotification('Error connecting: ' + error.message, 'error');
		}
	}
}

// Load and display connected wallet positions
async function loadConnectedWalletPositions(address) {
	const container = $('#connectedWalletPositions');
	if(!container) return;
	
	// Show loading state
	container.style.display = 'block';
	container.innerHTML = `
		<div class="card">
			<div class="card-header">
				<div class="card-title"><h2>💼 DeFi Positions</h2></div>
				<div class="subtitle">Connected Wallet: ${shortAddr(address)}</div>
			</div>
			<div class="loading"><div class="spinner"></div>Loading positions...</div>
		</div>
	`;
	
	try {
		const r = await fetch(`/api/wallet/positions?address=${encodeURIComponent(address)}`);
		const d = await r.json();
		if(d.error) throw new Error(d.error);
		
		// Render positions
		const positionsHtml = renderWalletPositions(d);
		container.innerHTML = `
			<div class="card">
				<div class="card-header">
					<div class="card-title"><h2>💼 DeFi Positions</h2></div>
					<div class="subtitle">Connected Wallet: ${shortAddr(address)}</div>
				</div>
				${positionsHtml}
			</div>
		`;
	} catch(e) {
		console.error('Failed to load positions:', e);
		container.innerHTML = `
			<div class="card">
				<div class="card-header">
					<div class="card-title"><h2>💼 DeFi Positions</h2></div>
					<div class="subtitle">Connected Wallet: ${shortAddr(address)}</div>
				</div>
				<div class="error">Error loading positions: ${e.message}</div>
			</div>
		`;
	}
}

// Listen for MetaMask account changes
if(typeof window.ethereum !== 'undefined') {
	ethereum.on('accountsChanged', (accounts) => {
		const btn = $('#walletConnectBtn');
		const btnText = $('#walletBtnText');
		const input = $('#walletInput');
		const container = $('#connectedWalletPositions');
		
		if(accounts.length === 0) {
			// Disconnected
			if(btn) btn.classList.remove('connected');
			if(btnText) btnText.textContent = '🦊 Connect Wallet';
			if(input) input.value = '';
			if(container) container.style.display = 'none';
			showNotification('Wallet getrennt', 'info');
		} else {
			// Account changed
			const address = accounts[0];
			if(input) input.value = address;
			if(btn) btn.classList.add('connected');
			if(btnText) btnText.textContent = `✓ ${shortAddr(address)}`;
			showNotification('Wallet address changed', 'info');
			// Reload positions for new account
			loadConnectedWalletPositions(address);
		}
	});
	
	// Listen for chain changes (optional - reload page for safety)
	ethereum.on('chainChanged', () => {
		window.location.reload();
	});
}

// Wallet Analyse
function isEthAddress(s){ return /^0x[a-fA-F0-9]{40}$/.test((s||'').trim()); }
function showWalletError(msg){ const e=$('#walletError'); if(!e) return; e.textContent = msg||''; e.style.display = msg? 'block':'none'; }
function setWalletResults(html){ const el=$('#walletResults'); if(el) el.innerHTML = html||''; }

function renderWalletPositions(data){
	if(!data || !Array.isArray(data.protocols) || data.protocols.length===0){
		return '<div class="muted">No positions found.</div>';
	}
	const parts = [];
	// Sortiere Protokolle: Aave V3 zuerst, dann Uniswap V2, dann Uniswap V3
	const sortedProtocols = [...data.protocols].sort((a, b) => {
		if(a.protocol === 'Aave V3') return -1;
		if(b.protocol === 'Aave V3') return 1;
		if(a.protocol === 'Uniswap V2') return -1;
		if(b.protocol === 'Uniswap V2') return 1;
		return 0;
	});
	for(const pr of sortedProtocols){
		const logoSvg = (pr.protocol === 'Uniswap V3' || pr.protocol === 'Uniswap V2') ? '<svg class="protocol-logo" style="width:20px;height:20px;margin-left:8px;" viewBox="0 0 400 434" xmlns="http://www.w3.org/2000/svg"><path d="M325.144 65.7698C325.727 55.5419 327.119 48.7957 329.918 42.6348C331.025 40.1962 332.063 38.2003 332.223 38.2003C332.383 38.2003 331.902 40.0003 331.153 42.1999C329.118 48.1791 328.784 56.3572 330.186 65.872C331.965 77.9447 332.976 79.6866 345.782 92.7278C351.788 98.8446 358.775 106.559 361.307 109.871L365.912 115.894L361.307 111.59C355.676 106.327 342.724 96.0618 339.863 94.5945C337.945 93.6104 337.66 93.6274 336.477 94.801C335.387 95.8823 335.157 97.5072 335.005 105.189C334.77 117.16 333.132 124.845 329.18 132.528C327.042 136.684 326.705 135.797 328.64 131.106C330.084 127.604 330.231 126.064 330.22 114.475C330.198 91.1888 327.424 85.5906 311.153 76.0005C307.032 73.5711 300.24 70.0674 296.062 68.2141C291.883 66.3608 288.564 64.7467 288.685 64.6261C289.146 64.1691 305.013 68.7839 311.399 71.2318C320.899 74.8731 322.467 75.3449 323.621 74.9057C324.394 74.6113 324.768 72.367 325.144 65.7698Z" fill="#F50DB4"/></svg>' : '';
		parts.push(`<div class="asset-item"><div class="asset-header"><div class="asset-name" style="display:flex;align-items:center;">${pr.protocol}${logoSvg}</div></div>`);
		if(pr.protocol==='Uniswap V2'){
			const p = pr.positions && pr.positions[0];
			if(p){
				const under = (p.underlying||[]).map(u=>`<div class="asset-stat"><div class="stat-label">${u.symbol}</div><div class="stat-value">${formatNumber(u.amount)} <span class="muted">(${formatUSD(u.value_usd)})</span></div></div>`).join('');
				parts.push(`<div class="asset-stats" style="grid-template-columns:repeat(2,1fr)">${under}</div>`);
				parts.push(`<div class="muted" style="margin-top:8px">Share: ${(p.pool_share*100).toFixed(4)}% &middot; Estimated: ${formatUSD(p.est_value_usd)}</div>`);
			}
		} else if(pr.protocol==='Uniswap V3'){
			const rows = (pr.positions||[]).slice(0,10).map(pos=>{
				// Placeholder message from backend
				if(pos.message) {
					return `<div class="asset-stat" style="grid-column: 1/-1;">
						<div class="stat-label">${pos.message}</div>
						<div class="muted" style="margin-top:4px;">${pos.note || ''}</div>
					</div>`;
				}
				
				if(pos.error) return `<div class="muted">Position #${pos.token_id}: Error loading</div>`;
				
				// Full position details
				return `
					<div class="asset-stat" style="grid-column: 1/-1; border-bottom: 1px solid rgba(255,255,255,0.1); padding-bottom: 12px; margin-bottom: 12px;">
						<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
							<div class="stat-label">NFT #${pos.token_id}</div>
							<div class="stat-value">${pos.token0_symbol || '?'}/${pos.token1_symbol || '?'} ${pos.fee_tier || ''}${pos.pool_price ? ` · ${pos.pool_price}` : ''}</div>
						</div>
						<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:0.9em;">
							<div class="muted">Liquidity:</div>
							<div>${pos.liquidity || '0'}</div>
							<div class="muted">Tick Range:</div>
							<div>${pos.tick_lower} → ${pos.tick_upper}</div>
							${pos.tokens_owed_0 && pos.tokens_owed_0 !== '0' ? `
								<div class="muted">Fees Owed:</div>
								<div>${pos.tokens_owed_0} / ${pos.tokens_owed_1}</div>
							` : ''}
						</div>
					</div>
				`;
			}).join('');
			parts.push(`<div class="asset-stats" style="grid-template-columns:1fr">${rows || '<div class="muted">Keine aktiven NFTs</div>'}</div>`);
			} else if(pr.protocol==='Aave V3'){
				// Gruppiere Supply und Borrow in zwei Spalten
				const supplyItems = [];
				const borrowItems = [];
				(pr.positions||[]).forEach(p=>{
					if(p.supplied > 0) {
						supplyItems.push(`<div class="asset-stat">
							<div class="stat-label">${p.asset}</div>
							<div class="stat-value">Supply: ${formatNumber(p.supplied)}</div>
							<div class="muted">USD: ${formatUSD(p.supplied * (p.price || 0))}</div>
						</div>`);
					}
					if(p.borrowed > 0) {
						borrowItems.push(`<div class="asset-stat">
							<div class="stat-label">${p.asset}</div>
							<div class="stat-value">Borrow: ${formatNumber(p.borrowed)}</div>
							<div class="muted">USD: ${formatUSD(p.borrowed * (p.price || 0))}</div>
						</div>`);
					}
				});
				const leftCol = supplyItems.join('');
				const rightCol = borrowItems.join('');
				const tableHtml = `<div style="display:grid;grid-template-columns:repeat(2,1fr);gap:16px;">
					<div>${leftCol || '<div class="muted">No supply positions</div>'}</div>
					<div>${rightCol || '<div class="muted">No borrow positions</div>'}</div>
				</div>`;
				const totals = pr.totals || {supplied_usd:0, borrowed_usd:0, net_usd:0};
				parts.push(tableHtml);
				parts.push(`<div class="muted" style="margin-top:8px">Summe · Supplied: ${formatUSD(totals.supplied_usd)} · Borrowed: ${formatUSD(totals.borrowed_usd)}</div>`);
		}
		parts.push('</div>');
	}
	return parts.join('');
}

async function analyzeWallet(){
	const input = $('#walletInput'); if(!input) return;
	const addr = (input.value||'').trim();
	if(!isEthAddress(addr)){ showWalletError('Please enter a valid Ethereum address (0x...)'); return; }
	showWalletError(''); setWalletResults('<div class="loading"><div class="spinner"></div>Analyzing…</div>');
	try{
		const r = await fetch(`/api/wallet/positions?address=${encodeURIComponent(addr)}`);
		const d = await r.json();
		if(d.error){ throw new Error(d.error); }
		setWalletResults(renderWalletPositions(d));
	}catch(e){
		showWalletError('Analysis failed: '+ (e.message||e));
		setWalletResults('');
	}
}

// Update the compare-chart header clock (shows current date/time)
function updateCompareHeaderTime(){
	const el = document.getElementById('compareTimestamp');
	if(!el) return;
	el.textContent = formatDateTimeNow();
}

async function pasteFromClipboard(){
	try{
		const txt = await navigator.clipboard.readText();
		const input = $('#walletInput'); if(input) input.value = txt || input.value;
	}catch(e){ /* ignore */ }
}

// ==================== UNIFIED DEFI POSITIONS ANALYSIS ====================

let currentProtocolFilter = 'all'; // all, uniswap, aave, position

function switchProtocol(protocol) {
	currentProtocolFilter = protocol;
	
	// Update active tab
	document.querySelectorAll('.protocol-tab').forEach(tab => {
		tab.classList.remove('active');
		if(tab.getAttribute('data-protocol') === protocol) {
			tab.classList.add('active');
		}
	});
	
	// Update input placeholder - Immer Wallet-Adresse verwenden
	const input = $('#walletInput');
	input.placeholder = '0x… Wallet-Adresse eingeben';
	
	// Clear previous results
	$('#walletResults').innerHTML = '';
	$('#walletError').style.display = 'none';
}

async function analyzeAllPositions() {
	console.log('🔍 analyzeAllPositions() called');
	const input = $('#walletInput').value.trim();
	const errorEl = $('#walletError');
	const loadingEl = $('#walletLoading');
	const resultsEl = $('#walletResults');
	
	console.log('📝 Input value:', input);
	console.log('📊 Current protocol filter:', currentProtocolFilter);
	
	// Reset
	errorEl.style.display = 'none';
	resultsEl.innerHTML = '';
	
	// Validate input
	if (!input) {
		errorEl.textContent = 'Please enter wallet address';
		errorEl.style.display = 'block';
		console.log('❌ No input provided');
		return;
	}
	
	// Validate wallet address
	if (!input.startsWith('0x') || input.length !== 42) {
		errorEl.textContent = 'Invalid Ethereum address. Must start with 0x and be 42 characters long.';
		errorEl.style.display = 'block';
		console.log('❌ Invalid address format');
		return;
	}
	
	console.log('✅ Valid address, fetching positions...');
	
	// Show loading
	loadingEl.style.display = 'block';
	
	try {
		// Fetch all positions based on filter
		const promises = [];
		
		console.log('🌐 Fetching data for filter:', currentProtocolFilter);
		
		if (currentProtocolFilter === 'all' || currentProtocolFilter === 'uniswap' || currentProtocolFilter === 'position') {
			const url = `/api/uniswap/wallet/${input}/positions`;
			console.log('📡 Fetching Uniswap positions:', url);
			promises.push(
				fetch(url)
					.then(r => {
						console.log('✅ Uniswap API response status:', r.status);
						return r.json();
					})
					.then(d => {
						console.log('📦 Uniswap data:', d);
						return { protocol: 'uniswap', data: d };
					})
					.catch(e => {
						console.error('❌ Uniswap fetch error:', e);
						return { protocol: 'uniswap', error: e.message };
					})
			);
		}
		
		if (currentProtocolFilter === 'all' || currentProtocolFilter === 'aave') {
			const aaveUrl = `/api/wallet/positions?address=${input}`;
			console.log('📡 Fetching Aave positions:', aaveUrl);
			promises.push(
				fetch(aaveUrl)
					.then(r => {
						console.log('✅ Aave API response status:', r.status);
						return r.json();
					})
					.then(d => {
						console.log('📦 Aave data:', d);
						return { protocol: 'aave', data: d };
					})
					.catch(e => {
						console.error('❌ Aave fetch error:', e);
						return { protocol: 'aave', error: e.message };
					})
			);
		}
		
		if (currentProtocolFilter === 'all') {
			const v2Url = `/api/wallet/positions?address=${input}`;
			console.log('📡 Fetching V2 positions:', v2Url);
			promises.push(
				fetch(v2Url)
					.then(r => r.json())
					.then(d => ({ protocol: 'legacy', data: d }))
					.catch(e => ({ protocol: 'legacy', error: e.message }))
			);
		}
		
		const results = await Promise.all(promises);
		loadingEl.style.display = 'none';
		
		console.log('📊 All results received:', results);
		
		// Display unified results
		displayUnifiedPositions(input, results);
		
	} catch (error) {
		console.error('❌ Error in analyzeAllPositions:', error);
		loadingEl.style.display = 'none';
		errorEl.textContent = error.message;
		errorEl.style.display = 'block';
	}
}

function displayUniswapPositionOnly(position) {
	const resultsEl = $('#walletResults');
	const { token0, token1, fee_tier, price_range, amounts, value_usd, unclaimed_fees, status, liquidity } = position;
	
	const statusBadge = status.in_range 
		? '<span class="badge badge-success">✓ In Range</span>'
		: '<span class="badge badge-warning">⚠ Out of Range</span>';
	
	const healthBadge = status.health === 'healthy'
		? '<span class="badge badge-success">Healthy</span>'
		: status.health === 'out_of_range'
		? '<span class="badge badge-warning">Out of Range</span>'
		: '<span class="badge badge-error">Inactive</span>';
	
	resultsEl.innerHTML = `
		<div class="protocol-section">
			<div class="protocol-header">
				<h3>🦄 Uniswap V3 Position</h3>
			</div>
			<div class="position-card">
				<div class="position-header">
					<h3>${token0.symbol} / ${token1.symbol}</h3>
					<div class="position-badges">
						${statusBadge}
						${healthBadge}
						<span class="badge">${fee_tier}% Fee</span>
					</div>
				</div>
				
				<div class="position-grid">
					<div class="position-stat">
						<div class="stat-label">Position ID</div>
						<div class="stat-value">#${position.position_id}</div>
					</div>
					<div class="position-stat">
						<div class="stat-label">Total Value</div>
						<div class="stat-value">${formatUSD(value_usd.total)}</div>
					</div>
					<div class="position-stat">
						<div class="stat-label">Unclaimed Fees</div>
						<div class="stat-value success">${formatUSD(unclaimed_fees.usd)}</div>
					</div>
					<div class="position-stat">
						<div class="stat-label">Pool Share</div>
						<div class="stat-value">${liquidity.share_of_pool.toFixed(4)}%</div>
					</div>
				</div>
				
				<div class="position-details">
					<div class="detail-row">
						<span class="detail-label">Price Range</span>
						<span class="detail-value">${price_range.lower.toFixed(6)} → ${price_range.upper.toFixed(6)}</span>
					</div>
					<div class="detail-row">
						<span class="detail-label">Current Price</span>
						<span class="detail-value ${status.in_range ? 'success' : 'warning'}">${price_range.current.toFixed(6)}</span>
					</div>
					<div class="detail-row">
						<span class="detail-label">${token0.symbol} Amount</span>
						<span class="detail-value">${amounts.token0.toFixed(6)} (${formatUSD(value_usd.token0)})</span>
					</div>
					<div class="detail-row">
						<span class="detail-label">${token1.symbol} Amount</span>
						<span class="detail-value">${amounts.token1.toFixed(6)} (${formatUSD(value_usd.token1)})</span>
					</div>
					<div class="detail-row">
						<span class="detail-label">Unclaimed ${token0.symbol}</span>
						<span class="detail-value success">${unclaimed_fees.token0.toFixed(6)}</span>
					</div>
					<div class="detail-row">
						<span class="detail-label">Unclaimed ${token1.symbol}</span>
						<span class="detail-value success">${unclaimed_fees.token1.toFixed(6)}</span>
					</div>
				</div>
				
				<div class="position-footer">
					<small class="muted">Pool: ${position.pool_address.slice(0,6)}...${position.pool_address.slice(-4)}</small>
				</div>
			</div>
		</div>
	`;
}

function displayUnifiedPositions(wallet, results) {
	const resultsEl = $('#walletResults');
	let html = '';
	
	let totalValue = 0;
	let hasAnyPositions = false;
	
	// Uniswap V3 Section
	const uniswapResult = results.find(r => r.protocol === 'uniswap');
	if (uniswapResult && uniswapResult.data && uniswapResult.data.success) {
		const { summary, positions } = uniswapResult.data;
		
		if (positions && positions.length > 0) {
			hasAnyPositions = true;
			totalValue += summary.total_value_usd || 0;
			
			html += `
				<div class="protocol-section">
					<div class="protocol-header">
						<h3>🦄 Uniswap V3 Positionen</h3>
						<div class="protocol-stats">
							<span class="stat-badge">${summary.total_positions} Positions</span>
							<span class="stat-badge success">${summary.in_range_positions} In Range</span>
							<span class="stat-badge">${formatUSD(summary.total_value_usd)}</span>
						</div>
					</div>
					
					<div class="wallet-summary">
						<div class="summary-grid">
							<div class="summary-stat">
								<div class="stat-label">Total Value</div>
								<div class="stat-value">${formatUSD(summary.total_value_usd)}</div>
							</div>
							<div class="summary-stat">
								<div class="stat-label">Active</div>
								<div class="stat-value">${summary.active_positions}/${summary.total_positions}</div>
							</div>
							<div class="summary-stat">
								<div class="stat-label">In Range</div>
								<div class="stat-value">${summary.in_range_positions}</div>
							</div>
							<div class="summary-stat">
								<div class="stat-label">Unclaimed Fees</div>
								<div class="stat-value success">${formatUSD(summary.total_unclaimed_fees_usd)}</div>
							</div>
						</div>
					</div>
					
					<div class="positions-list">
			`;
			
			positions.forEach(position => {
				const { token0, token1, fee_tier, price_range, value_usd, unclaimed_fees, status, position_id } = position;
				
				const statusBadge = status.in_range 
					? '<span class="badge badge-success">In Range</span>'
					: '<span class="badge badge-warning">Out of Range</span>';
				
				html += `
					<div class="position-card compact">
						<div class="position-header">
							<h4>${token0.symbol} / ${token1.symbol} <span class="muted small">#${position_id}</span></h4>
							<div class="position-badges">
								${statusBadge}
								<span class="badge">${fee_tier}% Fee</span>
							</div>
						</div>
						<div class="position-stats-row">
							<div class="stat-item">
								<span class="stat-label">Value</span>
								<span class="stat-value">${formatUSD(value_usd.total)}</span>
							</div>
							<div class="stat-item">
								<span class="stat-label">Fees</span>
								<span class="stat-value success">${formatUSD(unclaimed_fees.usd)}</span>
							</div>
							<div class="stat-item">
								<span class="stat-label">Price</span>
								<span class="stat-value">${price_range.current.toFixed(4)}</span>
							</div>
							<div class="stat-item">
								<span class="stat-label">Range</span>
								<span class="stat-value small">${price_range.lower.toFixed(2)}–${price_range.upper.toFixed(2)}</span>
							</div>
						</div>
					</div>
				`;
			});
			
			html += '</div></div>';
		}
	}
	
	// Aave positions
	const aaveResult = results.find(r => r.protocol === 'aave');
	if (aaveResult && aaveResult.data && !aaveResult.data.error) {
		const data = aaveResult.data;
		
		// Check if has Aave positions
		if (data.aave && data.aave.positions && data.aave.positions.length > 0) {
			hasAnyPositions = true;
			const aavePositions = data.aave.positions;
			
			html += `
				<div class="protocol-section">
					<div class="protocol-header">
						<h3>👻 Aave V3 Positions</h3>
						<div class="protocol-stats">
							<span class="stat-badge">${aavePositions.length} Markets</span>
						</div>
					</div>
					<div class="positions-list">
			`;
			
			aavePositions.forEach(pos => {
				const isSupply = pos.supplied > 0;
				const isBorrow = pos.borrowed > 0;
				const typeLabel = isSupply && isBorrow ? 'Supply & Borrow' : isSupply ? 'Supply' : 'Borrow';
				
				html += `
					<div class="position-card compact">
						<div class="position-header">
							<h4>${pos.asset}</h4>
							<div class="position-badges">
								<span class="badge ${isSupply ? 'badge-success' : 'badge-warning'}">${typeLabel}</span>
							</div>
						</div>
						<div class="position-stats-row">
							${isSupply ? `
								<div class="stat-item">
									<span class="stat-label">Supplied</span>
									<span class="stat-value">${formatNumber(pos.supplied)}</span>
								</div>
								<div class="stat-item">
									<span class="stat-label">Value</span>
									<span class="stat-value">${formatUSD(pos.supplied * (pos.price || 0))}</span>
								</div>
							` : ''}
							${isBorrow ? `
								<div class="stat-item">
									<span class="stat-label">Borrowed</span>
									<span class="stat-value">${formatNumber(pos.borrowed)}</span>
								</div>
								<div class="stat-item">
									<span class="stat-label">Debt</span>
									<span class="stat-value warning">${formatUSD(pos.borrowed * (pos.price || 0))}</span>
								</div>
							` : ''}
							<div class="stat-item">
								<span class="stat-label">APY</span>
								<span class="stat-value">${isSupply ? pos.supply_apy?.toFixed(2) : pos.borrow_apy?.toFixed(2)}%</span>
							</div>
						</div>
					</div>
				`;
			});
			
			html += '</div></div>';
		}
	}
	
	// Legacy wallet positions (V2, etc.)
	const legacyResult = results.find(r => r.protocol === 'legacy');
	if (legacyResult && legacyResult.data && !legacyResult.data.error) {
		const data = legacyResult.data;
		
		// Check if has Uniswap V2 positions
		if (data.uniswap_v2 && data.uniswap_v2.positions && data.uniswap_v2.positions.length > 0) {
			hasAnyPositions = true;
			const v2Positions = data.uniswap_v2.positions;
			
			html += `
				<div class="protocol-section">
					<div class="protocol-header">
						<h3>🦄 Uniswap V2 Liquidity</h3>
						<div class="protocol-stats">
							<span class="stat-badge">${v2Positions.length} Pools</span>
						</div>
					</div>
					<div class="positions-list">
			`;
			
			v2Positions.forEach(pos => {
				html += `
					<div class="position-card compact">
						<div class="position-header">
							<h4>${pos.token0_symbol} / ${pos.token1_symbol}</h4>
						</div>
						<div class="position-stats-row">
							<div class="stat-item">
								<span class="stat-label">${pos.token0_symbol}</span>
								<span class="stat-value">${formatNumber(pos.reserve0)}</span>
							</div>
							<div class="stat-item">
								<span class="stat-label">${pos.token1_symbol}</span>
								<span class="stat-value">${formatNumber(pos.reserve1)}</span>
							</div>
							<div class="stat-item">
								<span class="stat-label">Pool Share</span>
								<span class="stat-value">${(pos.pool_share * 100).toFixed(4)}%</span>
							</div>
						</div>
					</div>
				`;
			});
			
			html += '</div></div>';
		}
	}
	
	// No positions found
	if (!hasAnyPositions) {
		html = `
			<div class="empty-state">
				<div style="font-size:3em; margin-bottom:16px;">🔍</div>
				<div style="font-size:1.1em; margin-bottom:8px;">Keine DeFi-Positionen gefunden</div>
				<div class="muted">Diese Wallet hat keine aktiven Uniswap V2/V3 Positionen.</div>
			</div>
		`;
	}
	
	resultsEl.innerHTML = html;
}

function setupWallet(){
	// No event listener here - handled by onclick in HTML
}

// ==================== END UNIFIED POSITIONS ====================

// CSV status polling (updates badge in UI) - DISABLED (not needed)
async function pollCSVStatus(){
	// Status-Anzeige entfernt - nicht mehr benötigt
	return;
}

// Scan status polling (reads data/scan_status.json written by scanner)
async function fetchScanStatus(){
	const statusEl = document.getElementById('liquidations-content');
	const summaryEl = document.getElementById('liquidationsSummary');
	const countEl = document.getElementById('liqCount');
	try{
		const r = await fetch('/data/scan_status.json?t=' + Date.now());
		if(!r.ok) throw new Error('no status');
		const s = await r.json();
		updateScanStatusUI(s, statusEl, summaryEl, countEl);
	}catch(e){
		// silently ignore — keep existing UI
	}
}

function updateScanStatusUI(s, statusEl, summaryEl, countEl){
	if(!s) return;
	const status = s.status || 'idle';
	const events = s.events_found || 0;
	const msg = s.message || '';

	// Update count text
	if(countEl){
		countEl.textContent = events + ' liquidations found';
	}

	// Update summary with progress or idle/completed message
	if(summaryEl){
		let html = '';
		if(status === 'running'){
			const fromB = s.from_block || 0;
			// Prefer the live network latest block if available so 100% == network head
			const toB = (state.latestBlock && state.latestBlock > 0) ? state.latestBlock : (s.to_block || 0);
			const current = s.current_block || 0;
			let pct = 0;
			if(toB && fromB && toB > fromB){ pct = Math.round(((current - fromB) / (toB - fromB)) * 100); if(pct < 0) pct = 0; if(pct > 100) pct = 100; }
			
			// At 100% -> show only "completed" without Progress Bar
			if(pct >= 100){
				html += `<div class="muted">Scan complete. ${events} liquidations saved.</div>`;
			} else {
				html += `<div style="display:flex;align-items:center;gap:12px"><div class="scan-progress-bar"><div class="scan-progress-fill" style="width:${pct}%"></div></div><div style="min-width:60px;text-align:right">${pct}%</div></div>`;
				html += `<div class="muted" style="margin-top:6px">Scanning blockchain... (${events} liquidations saved)</div>`;
			}
		} else if(status === 'completed'){
			html += `<div class="muted">Scan complete. ${events} liquidations saved.</div>`;
		} else if(status === 'failed'){
			html += `<div class="error">Scan failed: ${msg}</div>`;
		} else {
			// idle
			if(events === 0){
				html += `<div class="muted">No liquidations available.</div>`;
				if(statusEl) statusEl.innerHTML = `<div class="muted" style="padding:20px;text-align:center">No liquidations found.</div>`;
			} else {
				html += `<div class="muted">Ready. ${events} liquidations saved.</div>`;
			}
		}
		summaryEl.innerHTML = html;
	}
}

window.addEventListener('load', ()=>{
        initTheme();
        toggleEthereumOnlySections();
        loadEthNetwork();
        // Loading historical data für Chart (Standard: 24h)
        loadHistoricalData(24);
        // Dashboard mit Batch-API laden (schneller!)
        loadDashboardBatch(false); // Erstes Laden mit Skeleton
	// Einzelne API-Calls für spezielle Features
	loadUniExtended();
	loadAaveRisk();
	loadLiquidations(false); // Erstes Laden mit Feedback
	setupWallet();

	// Init compare-chart header live clock and keep it updated every second
	updateCompareHeaderTime();
	setInterval(() => updateCompareHeaderTime(), 1000);
	
        // Initialize chain slider
        const savedChain = localStorage.getItem('activeChain') || 'ethereum';
        document.querySelectorAll('.chain-tab').forEach(tab => {
                if(tab.getAttribute('data-chain') === savedChain) {
                        tab.classList.add('active');
                }
        });
        updateChainSlider();
	window.addEventListener('resize', updateChainSlider);
	
	// Intervalle - SILENT UPDATES (kein visuelles Feedback)
	setInterval(() => loadEthNetwork(), 30000);
	setInterval(() => loadDashboardBatch(true), 30000);  // Silent update
	setInterval(() => loadUniExtended(), 60000);
	setInterval(() => loadAaveRisk(), 60000);
	setInterval(() => loadLiquidations(true), 60000); // Silent update
	setInterval(() => loadHistoricalData(currentHistoryRange), 300000); // Alle 5min

	// Start CSV status polling
	pollCSVStatus();
	setInterval(() => pollCSVStatus(), 5000);

	// Start scan status polling (shows progress for Aave V3 scanner)
	fetchScanStatus();
	setInterval(() => fetchScanStatus(), 3000);

	// No left live-clock — totals and last-updated are displayed on the right under the list
});


