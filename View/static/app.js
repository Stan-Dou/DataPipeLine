const state = {
  meta: [],
  companiesBySymbol: new Map(),
  trackedGroups: [],
  priceSeriesBySymbol: new Map(),
  range: 'all',
  colorIndex: 0,
  groupIndex: 0,
};

const palette = [
  '#0f6d73', '#c65d2e', '#6a4c93', '#1e847f', '#c08a00', '#9b2226', '#486581', '#6930c3'
];

const elements = {
  companySelect: document.getElementById('company-select'),
  expirySelect: document.getElementById('expiry-select'),
  typeSelect: document.getElementById('type-select'),
  strikeStartSelect: document.getElementById('strike-start-select'),
  strikeEndSelect: document.getElementById('strike-end-select'),
  rangeSelect: document.getElementById('range-select'),
  addGroupButton: document.getElementById('add-group-button'),
  syncButton: document.getElementById('sync-button'),
  statusText: document.getElementById('status-text'),
  lineList: document.getElementById('line-list'),
  charts: {
    price: document.getElementById('price-chart'),
    oi: document.getElementById('oi-chart'),
    settlement: document.getElementById('settlement-chart'),
  },
};

function setStatus(message) {
  elements.statusText.textContent = message;
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.error || `Request failed: ${response.status}`);
  }
  return response.json();
}

async function loadMetadata() {
  const payload = await fetchJson('/api/meta');
  state.meta = payload.companies;
  state.companiesBySymbol = new Map(state.meta.map((company) => [company.hkats_code, company]));
  populateCompanyOptions();
  updateDependentOptions();
}

function populateCompanyOptions() {
  const previous = elements.companySelect.value;
  elements.companySelect.innerHTML = state.meta
    .map((company) => `<option value="${company.hkats_code}">${company.hkats_code} · ${company.stock_name}</option>`)
    .join('');
  if (previous && state.companiesBySymbol.has(previous)) {
    elements.companySelect.value = previous;
  }
}

function updateDependentOptions() {
  const company = state.companiesBySymbol.get(elements.companySelect.value);
  if (!company) {
    elements.expirySelect.innerHTML = '';
    elements.typeSelect.innerHTML = '';
    elements.strikeStartSelect.innerHTML = '';
    elements.strikeEndSelect.innerHTML = '';
    return;
  }

  const selectedType = company.types.includes(elements.typeSelect.value) ? elements.typeSelect.value : company.types[0];
  elements.typeSelect.innerHTML = company.types
    .map((type) => `<option value="${type}">${type === 'C' ? 'Call' : 'Put'}</option>`)
    .join('');
  elements.typeSelect.value = selectedType;

  const expiryChoices = company.expiries.filter((expiry) => (company.contract_index?.[expiry]?.[selectedType] || []).length > 0);
  const selectedExpiry = expiryChoices.includes(elements.expirySelect.value) ? elements.expirySelect.value : expiryChoices[0];
  elements.expirySelect.innerHTML = expiryChoices
    .map((expiry) => `<option value="${expiry}">${expiry}</option>`)
    .join('');
  elements.expirySelect.value = selectedExpiry;

  const strikes = (company.contract_index?.[selectedExpiry]?.[selectedType] || []).slice().sort((left, right) => left - right);
  const strikeOptions = strikes.map((strike) => `<option value="${strike}">${strike}</option>`).join('');
  const previousStart = elements.strikeStartSelect.value;
  const previousEnd = elements.strikeEndSelect.value;
  elements.strikeStartSelect.innerHTML = strikeOptions;
  elements.strikeEndSelect.innerHTML = strikeOptions;
  if (strikes.length > 0) {
    const strikeValues = strikes.map(String);
    const fallbackStrike = getNearestStrike(strikes, company.latest_closing_price);
    elements.strikeStartSelect.value = strikeValues.includes(previousStart) ? previousStart : String(fallbackStrike);
    elements.strikeEndSelect.value = strikeValues.includes(previousEnd) ? previousEnd : elements.strikeStartSelect.value;
  }
}

function getNearestStrike(strikes, closingPrice) {
  if (!strikes.length) {
    return '';
  }
  if (closingPrice === null || closingPrice === undefined || Number.isNaN(Number(closingPrice))) {
    return strikes[0];
  }

  let nearestStrike = strikes[0];
  let nearestDistance = Math.abs(strikes[0] - Number(closingPrice));
  for (const strike of strikes.slice(1)) {
    const distance = Math.abs(strike - Number(closingPrice));
    if (distance < nearestDistance) {
      nearestStrike = strike;
      nearestDistance = distance;
    }
  }
  return nearestStrike;
}

async function ensurePriceSeries(symbol) {
  if (state.priceSeriesBySymbol.has(symbol)) {
    return;
  }
  const payload = await fetchJson(`/api/stock-series?symbol=${encodeURIComponent(symbol)}`);
  state.priceSeriesBySymbol.set(symbol, payload);
}

function getLineKey(symbol, expiry, type, strike) {
  return `${symbol}|${expiry}|${type}|${strike}`;
}

function buildGroupName(symbol, expiry, type, startStrike, endStrike) {
  return `${symbol}-${expiry}-${type === 'C' ? 'Call' : 'Put'}-${startStrike}~${endStrike}`;
}

function getAllLines() {
  return state.trackedGroups.flatMap((group) => group.lines);
}

function getVisibleLines() {
  return state.trackedGroups.flatMap((group) => (
    group.visible ? group.lines.filter((line) => line.visible) : []
  ));
}

async function addTrackedGroup() {
  const symbol = elements.companySelect.value;
  const expiry = elements.expirySelect.value;
  const type = elements.typeSelect.value;
  const startStrike = Number(elements.strikeStartSelect.value);
  const endStrike = Number(elements.strikeEndSelect.value);
  const company = state.companiesBySymbol.get(symbol);
  const availableStrikes = (company?.contract_index?.[expiry]?.[type] || []).filter((strike) => {
    const floor = Math.min(startStrike, endStrike);
    const ceiling = Math.max(startStrike, endStrike);
    return strike >= floor && strike <= ceiling;
  });

  if (!availableStrikes.length) {
    setStatus('No strikes exist in that range for the selected company, expiry, and type.');
    return;
  }

  const existingKeys = new Set(getAllLines().map((line) => line.key));
  const newStrikes = availableStrikes.filter((strike) => !existingKeys.has(getLineKey(symbol, expiry, type, strike)));
  if (!newStrikes.length) {
    setStatus('All lines in that strike range are already tracked.');
    return;
  }

  setStatus(`Loading ${newStrikes.length} line(s)...`);
  await ensurePriceSeries(symbol);

  const linePayloads = await Promise.all(newStrikes.map(async (strike) => {
    const payload = await fetchJson(
      `/api/contract-series?symbol=${encodeURIComponent(symbol)}&expiry=${encodeURIComponent(expiry)}&type=${encodeURIComponent(type)}&strike=${encodeURIComponent(strike)}`
    );
    const line = {
      key: getLineKey(symbol, expiry, type, strike),
      symbol,
      visible: true,
      color: palette[state.colorIndex % palette.length],
      data: payload,
    };
    state.colorIndex += 1;
    return line;
  }));

  const sortedStrikes = newStrikes.slice().sort((left, right) => left - right);
  const floor = sortedStrikes[0];
  const ceiling = sortedStrikes[sortedStrikes.length - 1];
  const isSingle = floor === ceiling;
  state.trackedGroups.push({
    id: `group-${++state.groupIndex}`,
    symbol,
    expiry,
    type,
    startStrike: floor,
    endStrike: ceiling,
    name: isSingle ? linePayloads[0].data.line_name : buildGroupName(symbol, expiry, type, floor, ceiling),
    visible: true,
    isSingle,
    expanded: false,
    lines: linePayloads,
  });

  renderLineList();
  renderAllCharts();
  setStatus(isSingle ? `Loaded ${linePayloads[0].data.line_name}` : `Loaded group ${buildGroupName(symbol, expiry, type, floor, ceiling)}`);
}

function renderLineList() {
  if (!state.trackedGroups.length) {
    elements.lineList.className = 'line-list empty-state';
    elements.lineList.textContent = 'No contract lines added yet.';
    return;
  }

  elements.lineList.className = 'line-list';
  elements.lineList.innerHTML = state.trackedGroups.map((group) => {
    const visibleCount = group.lines.filter((line) => line.visible).length;
    const typeLabel = group.type === 'C' ? 'Call' : 'Put';
    if (group.isSingle) {
      const line = group.lines[0];
      const pointCount = line.data.points.length;
      return `
        <div class="line-item">
          <div class="line-item-header">
            <span class="line-name" style="color:${line.color}">${line.data.line_name}</span>
          </div>
          <div class="line-meta">${typeLabel} · ${line.data.company.stock_name} · ${pointCount} points</div>
          <div class="line-actions">
            <button class="secondary" data-action="toggle-single" data-group-id="${group.id}">${group.visible && line.visible ? 'Hide' : 'Show'}</button>
            <button class="secondary danger" data-action="remove-single" data-group-id="${group.id}">Delete</button>
          </div>
        </div>
      `;
    }
    return `
      <div class="group-item">
        <div class="group-header">
          <div class="group-summary">
            <div class="group-name">${group.name}</div>
            <div class="group-meta">${typeLabel} · ${group.lines[0]?.data.company.stock_name || group.symbol} · ${visibleCount}/${group.lines.length} visible lines</div>
          </div>
          <button class="secondary group-toggle" data-action="toggle-expand" data-group-id="${group.id}">${group.expanded ? 'Collapse' : 'Expand'}</button>
        </div>
        <div class="group-actions">
          <button class="secondary" data-action="toggle-group" data-group-id="${group.id}">${group.visible ? 'Hide Group' : 'Show Group'}</button>
          <button class="secondary danger" data-action="remove-group" data-group-id="${group.id}">Delete Group</button>
        </div>
        <div class="group-lines ${group.expanded ? '' : 'collapsed'}">
          ${group.lines.map((line) => {
            const pointCount = line.data.points.length;
            return `
              <div class="line-row">
                <div class="line-row-main">
                  <div class="line-name" style="color:${line.color}">${line.data.line_name}</div>
                  <div class="line-meta">${pointCount} points</div>
                </div>
                <div class="line-row-actions">
                  <button class="secondary" data-action="toggle-line" data-group-id="${group.id}" data-key="${line.key}">${line.visible ? 'Hide' : 'Show'}</button>
                  <button class="secondary danger" data-action="remove-line" data-group-id="${group.id}" data-key="${line.key}">Delete</button>
                </div>
              </div>
            `;
          }).join('')}
        </div>
      </div>
    `;
  }).join('');
}

function getFilteredPoints(points) {
  if (state.range === 'all' || !points.length) {
    return points;
  }
  const days = Number(state.range);
  const cutoff = new Date(points[points.length - 1].date);
  cutoff.setDate(cutoff.getDate() - days + 1);
  return points.filter((point) => new Date(point.date) >= cutoff);
}

function collectDates(seriesList) {
  const dates = new Set();
  for (const series of seriesList) {
    for (const point of series.points) {
      dates.add(point.date);
    }
  }
  return Array.from(dates).sort();
}

function formatNumber(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '-';
  }
  return Number(value).toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function formatMetricValue(metricLabel, value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '-';
  }
  if (metricLabel.includes('(HKD)')) {
    return `${formatNumber(value)} HKD`;
  }
  if (metricLabel.includes('(Contracts)')) {
    return `${Number(value).toLocaleString()} contracts`;
  }
  return formatNumber(value);
}

function renderAllCharts() {
  const visibleLines = getVisibleLines();
  const priceSeries = Array.from(
    new Set(visibleLines.map((line) => line.symbol))
  ).map((symbol) => {
    const payload = state.priceSeriesBySymbol.get(symbol);
    return payload
      ? {
          id: symbol,
          label: symbol,
          color: '#1f3c88',
          points: getFilteredPoints(payload.points).map((point) => ({ date: point.date, value: point.closing_price })),
        }
      : null;
  }).filter(Boolean);

  const oiSeries = visibleLines.map((line) => ({
    id: line.key,
    label: line.data.line_name,
    color: line.color,
    points: getFilteredPoints(line.data.points).map((point) => ({ date: point.date, value: point.oi, raw: point })),
  }));

  const settlementSeries = visibleLines.map((line) => ({
    id: line.key,
    label: line.data.line_name,
    color: line.color,
    points: getFilteredPoints(line.data.points).map((point) => ({ date: point.date, value: point.settlement, raw: point })),
  }));

  drawChart(elements.charts.price, priceSeries, 'Closing Price (HKD)');
  drawChart(elements.charts.oi, oiSeries, 'OI (Contracts)');
  drawChart(elements.charts.settlement, settlementSeries, 'Settlement (HKD)');
  syncCrosshair(elements.charts.price, elements.charts.oi, elements.charts.settlement);
}

function drawChart(container, seriesList, metricLabel) {
  const width = container.clientWidth || 900;
  const height = 220;
  const margin = { top: 16, right: 18, bottom: 28, left: 52 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;

  const dates = collectDates(seriesList);
  const values = seriesList.flatMap((series) => series.points.map((point) => point.value)).filter((value) => value !== null && value !== undefined);

  if (!dates.length || !values.length) {
    container.innerHTML = `<div class="empty-state">No ${metricLabel.toLowerCase()} data to display.</div>`;
    return;
  }

  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  const paddedMin = minValue === maxValue ? minValue * 0.95 : minValue - (maxValue - minValue) * 0.08;
  const paddedMax = minValue === maxValue ? maxValue * 1.05 : maxValue + (maxValue - minValue) * 0.08;

  const xForIndex = (index) => margin.left + (dates.length === 1 ? plotWidth / 2 : (index / (dates.length - 1)) * plotWidth);
  const yForValue = (value) => margin.top + plotHeight - ((value - paddedMin) / (paddedMax - paddedMin || 1)) * plotHeight;

  const dateIndex = new Map(dates.map((date, index) => [date, index]));
  const yTicks = [0, 0.25, 0.5, 0.75, 1].map((ratio) => paddedMin + (paddedMax - paddedMin) * ratio);
  const xTickDates = dates.filter((_, index) => index === 0 || index === dates.length - 1 || index === Math.floor((dates.length - 1) / 2));

  const paths = seriesList.map((series) => {
    const commands = series.points.map((point, index) => {
      const x = xForIndex(dateIndex.get(point.date));
      const y = yForValue(point.value);
      return `${index === 0 ? 'M' : 'L'} ${x.toFixed(2)} ${y.toFixed(2)}`;
    }).join(' ');
    return `<path d="${commands}" fill="none" stroke="${series.color}" stroke-width="2.4" stroke-linejoin="round" stroke-linecap="round"></path>`;
  }).join('');

  const yGrid = yTicks.map((tick) => {
    const y = yForValue(tick);
    return `
      <line class="grid-line" x1="${margin.left}" x2="${width - margin.right}" y1="${y}" y2="${y}"></line>
      <text class="tick-label" x="${margin.left - 8}" y="${y + 4}" text-anchor="end">${formatAxisTick(metricLabel, tick)}</text>
    `;
  }).join('');

  const xGrid = xTickDates.map((date) => {
    const x = xForIndex(dateIndex.get(date));
    return `
      <line class="grid-line" x1="${x}" x2="${x}" y1="${margin.top}" y2="${height - margin.bottom}"></line>
      <text class="tick-label" x="${x}" y="${height - 8}" text-anchor="middle">${date.slice(5)}</text>
    `;
  }).join('');

  container.innerHTML = `
    <svg class="chart-svg" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">
      <line class="axis-line" x1="${margin.left}" x2="${margin.left}" y1="${margin.top}" y2="${height - margin.bottom}"></line>
      <line class="axis-line" x1="${margin.left}" x2="${width - margin.right}" y1="${height - margin.bottom}" y2="${height - margin.bottom}"></line>
      ${yGrid}
      ${xGrid}
      ${paths}
      <line class="crosshair-line" x1="0" x2="0" y1="${margin.top}" y2="${height - margin.bottom}" visibility="hidden"></line>
      <text class="axis-label" x="${margin.left}" y="12">${metricLabel}</text>
    </svg>
    <div class="chart-tooltip tooltip-hidden"></div>
  `;

  container.dataset.dates = JSON.stringify(dates);
  container.dataset.metric = metricLabel;
  container.__seriesList = seriesList;
  container.__dimensions = { width, height, margin, plotWidth, plotHeight, dateIndex, xForIndex, yForValue };
}

function syncCrosshair(...containers) {
  containers.forEach((container) => {
    const svg = container.querySelector('.chart-svg');
    const tooltip = container.querySelector('.chart-tooltip');
    const crosshair = container.querySelector('.crosshair-line');
    if (!svg || !tooltip || !crosshair) {
      return;
    }

    const leave = () => {
      containers.forEach((target) => {
        const targetCrosshair = target.querySelector('.crosshair-line');
        const targetTooltip = target.querySelector('.chart-tooltip');
        if (targetCrosshair) {
          targetCrosshair.setAttribute('visibility', 'hidden');
        }
        if (targetTooltip) {
          targetTooltip.classList.add('tooltip-hidden');
        }
      });
    };

    svg.onmouseleave = leave;
    svg.onmousemove = (event) => {
      const rect = svg.getBoundingClientRect();
      const dates = JSON.parse(container.dataset.dates || '[]');
      if (!dates.length) {
        return;
      }

      const { margin, plotWidth, xForIndex } = container.__dimensions;
      const relativeX = Math.max(0, Math.min(plotWidth, event.clientX - rect.left - margin.left));
      const ratio = plotWidth === 0 ? 0 : relativeX / plotWidth;
      const index = Math.round(ratio * (dates.length - 1));
      const x = xForIndex(index);
      const activeDate = dates[index];

      containers.forEach((target) => updateCrosshairForContainer(target, x, activeDate, event.clientX, event.clientY));
    };
  });
}

function updateCrosshairForContainer(container, x, activeDate, clientX, clientY) {
  const crosshair = container.querySelector('.crosshair-line');
  const tooltip = container.querySelector('.chart-tooltip');
  if (!crosshair || !tooltip || !container.__seriesList) {
    return;
  }
  const metricLabel = container.dataset.metric || '';

  crosshair.setAttribute('x1', x);
  crosshair.setAttribute('x2', x);
  crosshair.setAttribute('visibility', 'visible');

  const lines = container.__seriesList
    .map((series) => ({
      series,
      point: series.points.find((entry) => entry.date === activeDate),
    }))
    .sort((left, right) => {
      const leftValue = left.point?.value ?? Number.NEGATIVE_INFINITY;
      const rightValue = right.point?.value ?? Number.NEGATIVE_INFINITY;
      return rightValue - leftValue;
    })
    .map(({ series, point }) => `<div><strong style="color:${series.color}">${series.label}</strong>: ${formatMetricValue(metricLabel, point?.value)}</div>`)
    .join('');

  tooltip.innerHTML = `<div><strong>${activeDate}</strong></div>${lines}`;
  tooltip.classList.remove('tooltip-hidden');

  const rect = container.getBoundingClientRect();
  tooltip.style.left = `${Math.min(rect.width - 210, clientX - rect.left + 14)}px`;
  tooltip.style.top = `${Math.max(10, clientY - rect.top - 20)}px`;
}

function formatAxisTick(metricLabel, value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '-';
  }
  if (metricLabel.includes('(HKD)')) {
    return formatNumber(value);
  }
  if (metricLabel.includes('(Contracts)')) {
    return Number(value).toLocaleString();
  }
  return formatNumber(value);
}

async function syncAll() {
  setStatus('Syncing from database...');
  const payload = await fetchJson('/api/sync', { method: 'POST' });
  state.meta = payload.meta.companies;
  state.companiesBySymbol = new Map(state.meta.map((company) => [company.hkats_code, company]));
  populateCompanyOptions();
  updateDependentOptions();

  const existingGroups = state.trackedGroups.map((group) => ({
    ...group,
    lines: group.lines.map((line) => ({ ...line })),
  }));
  state.trackedGroups = [];
  state.priceSeriesBySymbol.clear();

  for (const group of existingGroups) {
    await ensurePriceSeries(group.symbol);
    const refreshedLines = [];
    for (const line of group.lines) {
      const refreshed = await fetchJson(
        `/api/contract-series?symbol=${encodeURIComponent(line.symbol)}&expiry=${encodeURIComponent(line.data.expiry)}&type=${encodeURIComponent(line.data.type)}&strike=${encodeURIComponent(line.data.strike)}`
      );
      refreshedLines.push({
        ...line,
        data: refreshed,
      });
    }
    state.trackedGroups.push({
      ...group,
      lines: refreshedLines,
    });
  }

  renderLineList();
  renderAllCharts();
  setStatus('Sync complete.');
}

function handleLineListClick(event) {
  const button = event.target.closest('button[data-action]');
  if (!button) {
    return;
  }
  const action = button.dataset.action;
  const groupId = button.dataset.groupId;
  const key = button.dataset.key;
  const group = state.trackedGroups.find((entry) => entry.id === groupId);
  if (!group) {
    return;
  }

  if (action === 'toggle-group') {
    group.visible = !group.visible;
  }
  if (action === 'toggle-expand') {
    group.expanded = !group.expanded;
  }
  if (action === 'remove-group') {
    state.trackedGroups = state.trackedGroups.filter((entry) => entry.id !== groupId);
  }
  if (action === 'toggle-single') {
    group.visible = !group.visible;
    group.lines[0].visible = group.visible;
  }
  if (action === 'remove-single') {
    state.trackedGroups = state.trackedGroups.filter((entry) => entry.id !== groupId);
  }
  if (action === 'toggle-line') {
    const line = group.lines.find((entry) => entry.key === key);
    if (!line) {
      return;
    }
    line.visible = !line.visible;
  }
  if (action === 'remove-line') {
    group.lines = group.lines.filter((entry) => entry.key !== key);
    if (!group.lines.length) {
      state.trackedGroups = state.trackedGroups.filter((entry) => entry.id !== groupId);
    }
  }
  renderLineList();
  renderAllCharts();
}

async function initialize() {
  try {
    await loadMetadata();
    renderAllCharts();
    setStatus('Ready.');
  } catch (error) {
    console.error(error);
    setStatus(error.message);
  }
}

elements.companySelect.addEventListener('change', updateDependentOptions);
elements.typeSelect.addEventListener('change', updateDependentOptions);
elements.expirySelect.addEventListener('change', updateDependentOptions);
elements.strikeStartSelect.addEventListener('change', () => {
  elements.strikeEndSelect.value = elements.strikeStartSelect.value;
});
elements.rangeSelect.addEventListener('change', () => {
  state.range = elements.rangeSelect.value;
  renderAllCharts();
});
elements.addGroupButton.addEventListener('click', () => addTrackedGroup().catch((error) => setStatus(error.message)));
elements.syncButton.addEventListener('click', () => syncAll().catch((error) => setStatus(error.message)));
elements.lineList.addEventListener('click', handleLineListClick);
window.addEventListener('resize', renderAllCharts);

initialize();