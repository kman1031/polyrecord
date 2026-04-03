class PolyrecordDashboard {
    constructor() {
        this.charts = {};
        this.ws = null;
        this._chartsUpdating = false;
        this._rtdsUpdating = false;
        this.init();
    }

    async init() {
        await this.loadInitialData();
        this.connectWebSocket();
        // Higher refresh rate to make the dashboard feel "real-time".
        // Data is still pulled from the local SQLite (see /api endpoints),
        // so very small intervals may increase DB load.
        const refreshMs = 5000;
        setInterval(() => {
            if (this._chartsUpdating) return;
            this._chartsUpdating = true;
            this.updateCharts()
                .catch((e) => console.error("updateCharts error:", e))
                .finally(() => {
                    this._chartsUpdating = false;
                });
        }, refreshMs);
        setInterval(() => {
            if (this._rtdsUpdating) return;
            this._rtdsUpdating = true;
            this.updateRTDS()
                .catch((e) => console.error("updateRTDS error:", e))
                .finally(() => {
                    this._rtdsUpdating = false;
                });
        }, refreshMs);
    }

    async loadInitialData() {
        await Promise.all([
            this.updateStats(),
            this.updateMarkets(),
            this.updateRTDS(),
            this.initCharts()
        ]);
    }

    async updateStats() {
        try {
            const response = await fetch('/api/stats');
            const data = await response.json();
            
            document.getElementById('total-rows').textContent = data.total_rows.toLocaleString();
            document.getElementById('db-size').textContent = `${data.db_size_mb} MB`;
            
            const tables = data.tables;
            document.getElementById('bn-bbo-count').textContent = (tables.bn_bbo || 0).toLocaleString();
            document.getElementById('bn-kline-count').textContent = (tables.bn_kline_1m || 0).toLocaleString();
            document.getElementById('poly-bbo-count').textContent = (tables.poly_bbo || 0).toLocaleString();
            document.getElementById('poly-trades-count').textContent = (tables.poly_trades || 0).toLocaleString();
            document.getElementById('poly-orderbook-count').textContent = (tables.poly_orderbook || 0).toLocaleString();
            document.getElementById('rtds-count').textContent = (tables.poly_rtds_prices || 0).toLocaleString();
            
            document.getElementById('last-update').textContent = new Date().toLocaleTimeString();
        } catch (error) {
            console.error('Error updating stats:', error);
        }
    }

    async updateMarkets() {
        try {
            const response = await fetch('/api/markets');
            const markets = await response.json();
            
            const grid = document.getElementById('markets-grid');
            document.getElementById('active-markets').textContent = markets.length;
            
            if (markets.length === 0) {
                grid.innerHTML = '<div class="loading">No active markets</div>';
                return;
            }
            
            grid.innerHTML = markets.map(market => `
                <div class="market-card">
                    <div class="market-asset">${market.asset}</div>
                    <div class="market-time">
                        ${new Date(market.start_time).toLocaleTimeString()} - 
                        ${new Date(market.end_time).toLocaleTimeString()}
                    </div>
                    ${market.open_price ? `<div class="market-price">Open: $${market.open_price.toFixed(2)}</div>` : ''}
                    ${market.bn_open_price ? `<div class="market-price">BN Open: $${market.bn_open_price.toFixed(2)}</div>` : ''}
                    ${market.bn_close_price ? `<div class="market-price">BN Close: $${market.bn_close_price.toFixed(2)}</div>` : ''}
                    ${market.result ? `<div class="market-result ${market.result.toLowerCase()}">${market.result}</div>` : ''}
                </div>
            `).join('');
        } catch (error) {
            console.error('Error updating markets:', error);
        }
    }

    async updateRTDS() {
        try {
            const response = await fetch('/api/rtds/prices?minutes=5');
            const prices = await response.json();
            
            const grid = document.getElementById('rtds-grid');
            
            if (prices.length === 0) {
                grid.innerHTML = '<div class="loading">No RTDS data available</div>';
                return;
            }
            
            const latestPrices = {};
            prices.reverse().forEach(p => {
                if (!latestPrices[p.symbol]) {
                    latestPrices[p.symbol] = p;
                }
            });
            
            grid.innerHTML = Object.values(latestPrices).map(p => `
                <div class="rtds-card">
                    <div class="rtds-symbol">${p.symbol}</div>
                    <div class="rtds-price">$${p.price.toFixed(2)}</div>
                </div>
            `).join('');
        } catch (error) {
            console.error('Error updating RTDS:', error);
        }
    }

    async initCharts() {
        const symbols = ['btcusdt', 'ethusdt', 'solusdt', 'xrpusdt'];
        const chartIds = ['btc-chart', 'eth-chart', 'sol-chart', 'xrp-chart'];
        const labels = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT'];
        
        for (let i = 0; i < symbols.length; i++) {
            await this.createChart(symbols[i], chartIds[i], labels[i]);
        }
    }

    async createChart(symbol, chartId, label) {
        try {
            const response = await fetch(`/api/prices/binance/${symbol}?minutes=60`);
            const data = await response.json();
            
            const ctx = document.getElementById(chartId).getContext('2d');
            
            this.charts[chartId] = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: data.map(d => new Date(d.time).toLocaleTimeString()),
                    datasets: [{
                        label: label,
                        data: data.map(d => d.mid),
                        borderColor: '#3b82f6',
                        backgroundColor: 'rgba(59, 130, 246, 0.1)',
                        borderWidth: 2,
                        fill: true,
                        tension: 0.4,
                        pointRadius: 0,
                        pointHoverRadius: 4,
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true,
                    plugins: {
                        legend: {
                            display: true,
                            labels: {
                                color: '#e8edf5',
                                font: { size: 14 }
                            }
                        },
                        tooltip: {
                            mode: 'index',
                            intersect: false,
                            backgroundColor: '#1a2030',
                            titleColor: '#e8edf5',
                            bodyColor: '#8492a6',
                            borderColor: '#2d3748',
                            borderWidth: 1
                        }
                    },
                    scales: {
                        x: {
                            display: true,
                            ticks: {
                                color: '#8492a6',
                                maxTicksLimit: 8
                            },
                            grid: {
                                color: '#2d3748',
                                drawBorder: false
                            }
                        },
                        y: {
                            display: true,
                            ticks: {
                                color: '#8492a6',
                                callback: (value) => '$' + value.toLocaleString()
                            },
                            grid: {
                                color: '#2d3748',
                                drawBorder: false
                            }
                        }
                    },
                    interaction: {
                        intersect: false,
                        mode: 'index'
                    }
                }
            });
        } catch (error) {
            console.error(`Error creating chart for ${symbol}:`, error);
        }
    }

    async updateCharts() {
        const symbols = ['btcusdt', 'ethusdt', 'solusdt', 'xrpusdt'];
        const chartIds = ['btc-chart', 'eth-chart', 'sol-chart', 'xrp-chart'];
        const tasks = [];
        for (let i = 0; i < symbols.length; i++) {
            tasks.push(this.updateChart(symbols[i], chartIds[i]));
        }
        await Promise.all(tasks);
    }

    async updateChart(symbol, chartId) {
        try {
            const response = await fetch(`/api/prices/binance/${symbol}?minutes=60`);
            const data = await response.json();
            
            const chart = this.charts[chartId];
            if (chart) {
                chart.data.labels = data.map(d => new Date(d.time).toLocaleTimeString());
                chart.data.datasets[0].data = data.map(d => d.mid);
                chart.update('none');
            }
        } catch (error) {
            console.error(`Error updating chart for ${symbol}:`, error);
        }
    }

    connectWebSocket() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/ws`;
        
        this.ws = new WebSocket(wsUrl);
        
        this.ws.onopen = () => {
            console.log('WebSocket connected');
            const indicator = document.getElementById('connection-status');
            indicator.classList.add('connected');
            indicator.classList.remove('disconnected');
            indicator.querySelector('.status-text').textContent = 'Connected';
        };
        
        this.ws.onmessage = async (event) => {
            const message = JSON.parse(event.data);
            
            if (message.type === 'update') {
                await this.updateStats();
                await this.updateMarkets();
            }
        };
        
        this.ws.onclose = () => {
            console.log('WebSocket disconnected');
            const indicator = document.getElementById('connection-status');
            indicator.classList.remove('connected');
            indicator.classList.add('disconnected');
            indicator.querySelector('.status-text').textContent = 'Disconnected';
            
            setTimeout(() => this.connectWebSocket(), 5000);
        };
        
        this.ws.onerror = (error) => {
            console.error('WebSocket error:', error);
        };
    }
}

document.addEventListener('DOMContentLoaded', () => {
    new PolyrecordDashboard();
});
