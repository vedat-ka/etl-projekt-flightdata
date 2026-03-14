import { useEffect, useMemo, useState } from 'react';
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';

const chartPalette = ['#c24d2c', '#2b6d4f', '#b26d08', '#7d2614', '#537e8b', '#8a6d3b', '#9b4f96', '#d37a35'];
const timeframeOptions = [
  { value: 'all', label: 'Alle Zeiten' },
  { value: 'night', label: 'Nacht' },
  { value: 'morning', label: 'Morgen' },
  { value: 'afternoon', label: 'Nachmittag' },
  { value: 'evening', label: 'Abend' },
];

const previewColumnLabels = {
  destination_airport_iata: 'Zielflughafen',
  destination_airport_name: 'Flughafenname',
  origin_airport_iata: 'Abflug',
  origin_country: 'Land',
  flight_number: 'Flug',
  airline_name: 'Airline',
  arrival_time: 'Ankunft',
  status: 'Status',
};

const csvPreviewPresets = {
  destination_airport_iata: ['destination_airport_iata', 'destination_airport_name', 'flight_number', 'airline_name', 'arrival_time', 'status'],
  airline_name: ['airline_name', 'flight_number', 'origin_airport_iata', 'destination_airport_iata', 'arrival_time', 'status'],
  origin_country: ['origin_country', 'origin_airport_iata', 'destination_airport_iata', 'airline_name', 'flight_number', 'arrival_time'],
  status: ['status', 'flight_number', 'airline_name', 'destination_airport_iata', 'origin_airport_iata', 'arrival_time'],
};

function formatDate(value) {
  if (!value) {
    return 'n/a';
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString('de-DE');
}

function numberLabel(value) {
  return new Intl.NumberFormat('de-DE').format(value ?? 0);
}

function formatCellValue(value, key) {
  if (value === null || value === undefined || value === '') {
    return 'n/a';
  }
  if (key === 'arrival_time' || key.endsWith('_at')) {
    return formatDate(value);
  }
  if (typeof value === 'number') {
    return numberLabel(value);
  }
  return String(value);
}

function StatusPill({ value }) {
  const tone = String(value || 'unknown').toLowerCase();
  return <span className={`status-pill status-pill--${tone}`}>{value || 'unknown'}</span>;
}

function MetricCard({ label, value, detail }) {
  return (
    <article className="metric-card">
      <div className="metric-card__content">
        <p>{label}</p>
        <span>{detail}</span>
      </div>
      <strong>{value}</strong>
    </article>
  );
}

function FilePreviewCard({ file, chartPalette }) {
  const [activeChartIndex, setActiveChartIndex] = useState(0);
  const [selectedChartValue, setSelectedChartValue] = useState(null);
  const [currentPage, setCurrentPage] = useState(1);
  const defaultPreviewColumnOrder = [
    'destination_airport_iata',
    'destination_airport_name',
    'origin_airport_iata',
    'origin_country',
    'flight_number',
    'airline_name',
    'arrival_time',
    'status',
  ];
  const chartGroups = file.analytics?.chart_groups || [];
  const activeChart = chartGroups[activeChartIndex] || chartGroups[0] || null;
  const selectedColumnOrder = activeChart?.column
    ? csvPreviewPresets[activeChart.column] || defaultPreviewColumnOrder
    : defaultPreviewColumnOrder;
  const previewColumns = selectedColumnOrder.filter((column) => file.columns.includes(column));
  const fallbackColumns = file.columns.slice(0, 8);
  const visibleColumns = (previewColumns.length ? previewColumns : fallbackColumns).map((column) => ({
    key: column,
    label: previewColumnLabels[column] || column,
    render: (value) => formatCellValue(value, column),
  }));
  const previewRows = useMemo(() => {
    const rows = [...(file.preview_rows || [])];
    if (!activeChart?.column) {
      return rows;
    }

    const filteredRows = selectedChartValue
      ? rows.filter((row) => String(row[activeChart.column] ?? '') === String(selectedChartValue))
      : rows;

    return filteredRows.sort((left, right) => {
      const leftValue = String(left[activeChart.column] ?? '');
      const rightValue = String(right[activeChart.column] ?? '');

      if (activeChart.column === 'arrival_time') {
        return leftValue.localeCompare(rightValue);
      }
      return leftValue.localeCompare(rightValue, 'de', { sensitivity: 'base' });
    });
  }, [activeChart, file.preview_rows, selectedChartValue]);

  useEffect(() => {
    setCurrentPage(1);
  }, [activeChartIndex, selectedChartValue, file.relative_path]);

  return (
    <div className="file-card file-card--stacked">
      <div className="file-card__summary">
        <div className="file-card__header file-card__header--wrap">
          <div>
            <strong>{file.name}</strong>
            <span>{file.relative_path}</span>
          </div>
          <small>{formatDate(file.updated_at * 1000)} | {numberLabel(file.row_count)} Zeilen</small>
        </div>
      </div>

      {chartGroups.length ? (
        <div className="file-card__charts file-card__charts--stacked">
          <div className="filter-chip-row filter-chip-row--compact">
            {chartGroups.map((group, index) => (
              <button
                key={`${file.relative_path}-${group.column}-chip`}
                type="button"
                className={`filter-chip ${index === activeChartIndex ? 'filter-chip--active' : ''}`}
                onClick={() => {
                  setActiveChartIndex(index);
                  setSelectedChartValue(null);
                }}
              >
                {group.label}
              </button>
            ))}
          </div>
          <div className="csv-chart-stack">
            {activeChart ? (
              <BarChartCard
                key={`${file.relative_path}-${activeChart.column}-bar`}
                title={activeChart.label}
                subtitle={file.name}
                data={activeChart.items}
                color={chartPalette[activeChartIndex % chartPalette.length]}
                height={310}
                activeName={selectedChartValue}
                onBarClick={(entry) => {
                  const nextName = entry?.name ?? null;
                  setSelectedChartValue((current) => (current === nextName ? null : nextName));
                }}
              />
            ) : null}
          </div>
        </div>
      ) : null}

      <div className="file-card__table">
        <div className="file-card__table-header">
          <strong>{activeChart ? `CSV-Vorschau: ${activeChart.label}` : 'CSV-Vorschau'}</strong>
          <span>
            {selectedChartValue
              ? `Gefiltert auf ${selectedChartValue}`
              : 'Klicke auf einen Balken, um die Tabelle zu filtern.'}
          </span>
        </div>
        <DataTable
          columns={visibleColumns}
          rows={previewRows}
          emptyText="Keine Vorschau verfuegbar."
          paginated
          rowsPerPage={8}
          fixedHeight={420}
          currentPage={currentPage}
          onPageChange={setCurrentPage}
        />
      </div>
    </div>
  );
}

function DataTable({
  columns,
  rows,
  emptyText,
  compact = false,
  paginated = false,
  rowsPerPage = 10,
  fixedHeight = null,
  currentPage = 1,
  onPageChange,
}) {
  if (!rows?.length) {
    return <div className="empty-panel">{emptyText}</div>;
  }

  const totalPages = paginated ? Math.max(1, Math.ceil(rows.length / rowsPerPage)) : 1;
  const safePage = Math.min(Math.max(1, currentPage), totalPages);
  const visibleRows = paginated
    ? rows.slice((safePage - 1) * rowsPerPage, safePage * rowsPerPage)
    : rows;

  return (
    <div className="table-block">
      <div
        className={`table-wrap ${compact ? 'table-wrap--dense' : ''} ${fixedHeight ? 'table-wrap--fixed-height' : ''}`}
        style={fixedHeight ? { maxHeight: `${fixedHeight}px` } : undefined}
      >
        <table className={compact ? 'table--dense' : ''}>
          <thead>
            <tr>
              {columns.map((column) => (
                <th key={column.key}>{column.label}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {visibleRows.map((row, index) => (
              <tr key={`${row.id || row.relative_path || row.flight_number || row.airline_name || index}`}>
                {columns.map((column) => (
                  <td key={column.key}>{column.render ? column.render(row[column.key], row) : row[column.key]}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {paginated ? (
        <div className="table-pagination">
          <span>{`Seite ${safePage} von ${totalPages}`}</span>
          <div className="table-pagination__actions">
            <button type="button" className="button button--ghost button--compact" onClick={() => onPageChange?.(safePage - 1)} disabled={safePage <= 1}>
              Zurueck
            </button>
            <button type="button" className="button button--ghost button--compact" onClick={() => onPageChange?.(safePage + 1)} disabled={safePage >= totalPages}>
              Weiter
            </button>
          </div>
        </div>
      ) : null}
    </div>
  );
}

function ChartCard({ title, subtitle, children }) {
  return (
    <div className="chart-card">
      <div className="chart-card__header">
        <strong>{title}</strong>
        {subtitle ? <span>{subtitle}</span> : null}
      </div>
      <div className="chart-surface">{children}</div>
    </div>
  );
}

function EmptyChart({ text }) {
  return <div className="empty-panel empty-panel--chart">{text}</div>;
}

function BarChartCard({ title, subtitle, data, xKey = 'name', dataKey = 'count', color = '#c24d2c', height = 260, activeName = null, onBarClick }) {
  return (
    <ChartCard title={title} subtitle={subtitle}>
      {data?.length ? (
        <ResponsiveContainer width="100%" height={height}>
          <BarChart data={data} margin={{ top: 8, right: 12, left: -18, bottom: 24 }}>
            <CartesianGrid stroke="rgba(55, 37, 18, 0.08)" strokeDasharray="4 4" />
            <XAxis dataKey={xKey} tick={{ fill: '#715945', fontSize: 12 }} angle={-18} textAnchor="end" interval={0} height={56} />
            <YAxis tick={{ fill: '#715945', fontSize: 12 }} allowDecimals={false} />
            <Tooltip />
            <Bar dataKey={dataKey} fill={color} radius={[10, 10, 0, 0]} onClick={onBarClick} cursor={onBarClick ? 'pointer' : 'default'}>
              {data.map((entry, index) => (
                <Cell
                  key={`${entry[xKey]}-${index}`}
                  fill={activeName && activeName !== entry[xKey] ? 'rgba(194, 77, 44, 0.35)' : color}
                  stroke={activeName === entry[xKey] ? '#7d2614' : 'none'}
                  strokeWidth={activeName === entry[xKey] ? 2 : 0}
                />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      ) : (
        <EmptyChart text="Keine Diagrammdaten vorhanden." />
      )}
    </ChartCard>
  );
}

function PieChartCard({ title, subtitle, data }) {
  return (
    <ChartCard title={title} subtitle={subtitle}>
      {data?.length ? (
        <ResponsiveContainer width="100%" height={260}>
          <PieChart>
            <Pie data={data} dataKey="count" nameKey="name" innerRadius={54} outerRadius={88} paddingAngle={2}>
              {data.map((entry, index) => (
                <Cell key={`${entry.name}-${index}`} fill={chartPalette[index % chartPalette.length]} />
              ))}
            </Pie>
            <Tooltip />
            <Legend wrapperStyle={{ fontSize: '12px' }} />
          </PieChart>
        </ResponsiveContainer>
      ) : (
        <EmptyChart text="Keine Diagrammdaten vorhanden." />
      )}
    </ChartCard>
  );
}

function LineChartCard({ title, subtitle, data, xKey = 'bucket', dataKey = 'count' }) {
  return (
    <ChartCard title={title} subtitle={subtitle}>
      {data?.length ? (
        <ResponsiveContainer width="100%" height={260}>
          <LineChart data={data} margin={{ top: 8, right: 12, left: -18, bottom: 8 }}>
            <CartesianGrid stroke="rgba(55, 37, 18, 0.08)" strokeDasharray="4 4" />
            <XAxis dataKey={xKey} tick={{ fill: '#715945', fontSize: 12 }} />
            <YAxis tick={{ fill: '#715945', fontSize: 12 }} allowDecimals={false} />
            <Tooltip />
            <Line type="monotone" dataKey={dataKey} stroke="#2b6d4f" strokeWidth={3} dot={{ r: 4 }} />
          </LineChart>
        </ResponsiveContainer>
      ) : (
        <EmptyChart text="Keine Diagrammdaten vorhanden." />
      )}
    </ChartCard>
  );
}

function FilterChips({ title, options, value, onChange }) {
  return (
    <div className="filter-group">
      <span>{title}</span>
      <div className="filter-chip-row">
        {options.map((option) => (
          <button
            key={option.value}
            type="button"
            className={`filter-chip ${value === option.value ? 'filter-chip--active' : ''}`}
            onClick={() => onChange(option.value)}
          >
            {option.label}
          </button>
        ))}
      </div>
    </div>
  );
}

function aggregateTimeframeAirlines(source, timeframe) {
  const counts = new Map();
  Object.values(source || {}).forEach((airportGroup) => {
    const rows = airportGroup?.[timeframe] || [];
    rows.forEach((row) => {
      const current = counts.get(row.airline_name) || 0;
      counts.set(row.airline_name, current + row.count);
    });
  });
  return Array.from(counts.entries())
    .map(([airline_name, count]) => ({ airline_name, count }))
    .sort((left, right) => right.count - left.count || left.airline_name.localeCompare(right.airline_name))
    .slice(0, 10);
}

function getAirportAirlines(database, airportCode, timeframe) {
  if (timeframe === 'all') {
    return database.top_airlines_by_airport?.[airportCode] || [];
  }

  return database.airlines_by_airport_and_timeframe?.[airportCode]?.[timeframe] || [];
}

export default function OverviewPage({
  snapshot,
  form,
  submitting,
  saving,
  onUpdateForm,
  onToggleAirport,
  onAddCustomAirport,
  onRun,
  onSaveSettings,
  onRefresh,
}) {
  const [selectedAirportFilter, setSelectedAirportFilter] = useState('all');
  const [selectedTimeframe, setSelectedTimeframe] = useState('all');

  const selectedAirportSet = useMemo(() => new Set(form.selectedAirports), [form.selectedAirports]);
  const health = snapshot?.health || {};
  const job = snapshot?.job || {};
  const database = snapshot?.database || {};
  const files = snapshot?.files || { csv_files: [], raw_payloads: [], reports: [], logs: [] };
  const logAnalytics = snapshot?.log_analytics || { level_counts: [], hourly_activity: [], recent_events: [], line_count: 0 };

  const displayedAirports = useMemo(() => {
    const knownAirports = new Map((snapshot?.airports || []).map((airport) => [airport.iata, airport]));
    form.selectedAirports.forEach((airportCode) => {
      if (!knownAirports.has(airportCode)) {
        knownAirports.set(airportCode, {
          iata: airportCode,
          name: `Custom ${airportCode}`,
          timezone: 'UTC',
          api_code: airportCode,
          api_code_type: 'iata',
        });
      }
    });
    return Array.from(knownAirports.values());
  }, [form.selectedAirports, snapshot]);

  const airportOptions = useMemo(
    () => [
      { value: 'all', label: 'Alle Airports' },
      ...(displayedAirports.map((airport) => ({ value: airport.iata, label: airport.iata }))),
    ],
    [displayedAirports],
  );

  const selectedTopAirlines = useMemo(() => {
    if (selectedAirportFilter === 'all' && selectedTimeframe === 'all') {
      return database.top_airlines || [];
    }
    if (selectedAirportFilter === 'all') {
      return aggregateTimeframeAirlines(database.airlines_by_airport_and_timeframe, selectedTimeframe);
    }
    if (selectedTimeframe === 'all') {
      return database.top_airlines_by_airport?.[selectedAirportFilter] || [];
    }
    return database.airlines_by_airport_and_timeframe?.[selectedAirportFilter]?.[selectedTimeframe] || [];
  }, [database, selectedAirportFilter, selectedTimeframe]);

  const airportTopAirlineGroups = useMemo(() => {
    const availableAirports = displayedAirports.map((airport) => airport.iata);
    const airportCodes = selectedAirportFilter === 'all' ? availableAirports : [selectedAirportFilter];

    return airportCodes
      .map((airportCode) => ({
        airportCode,
        rows: getAirportAirlines(database, airportCode, selectedTimeframe),
      }))
      .filter((group) => group.rows.length > 0);
  }, [database, displayedAirports, selectedAirportFilter, selectedTimeframe]);

  const selectedArrivalActivity = useMemo(() => {
    const rows = database.arrival_activity || [];
    if (selectedAirportFilter === 'all') {
      const buckets = new Map();
      rows.forEach((row) => {
        buckets.set(row.bucket, (buckets.get(row.bucket) || 0) + row.count);
      });
      return Array.from(buckets.entries()).map(([bucket, count]) => ({ bucket, count }));
    }
    return rows.filter((row) => row.destination_airport_iata === selectedAirportFilter).map((row) => ({ bucket: row.bucket, count: row.count }));
  }, [database, selectedAirportFilter]);

  const selectedTopAirlineChart = selectedTopAirlines.map((row) => ({ name: row.airline_name, count: row.count }));
  const statusChartData = (database.status_distribution || []).map((item) => ({ name: item.status, count: item.count }));

  return (
    <div className="page-stack">
      <section className="hero-panel">
        <div>
          <p className="eyebrow">Flight ETL Control Room</p>
          <h2 className="hero-title">Dashboard fuer Steuerung, Airport-Filter und SQLite-Analyse</h2>
          <p className="hero-copy">
            Filtere Top-Airlines nach Airport und Zeitraum, starte neue Loads und nutze die gesamte Bildschirmbreite fuer Diagramme, Logs und Datenvorschauen.
          </p>
        </div>
        <div className="hero-status hero-status--row hero-status--stretch">
          <div>
            <span>Backend</span>
            <StatusPill value={health.backend_status} />
          </div>
          <div>
            <span>Job</span>
            <StatusPill value={job.status} />
          </div>
          <div>
            <span>API-Credentials</span>
            <StatusPill value={health.api_credentials_configured ? 'configured' : 'missing'} />
          </div>
        </div>
      </section>

      <section className="metrics-grid metrics-grid--wide">
        <MetricCard label="Flights in SQLite" value={numberLabel(database.flight_count)} detail="aktuelle Snapshot-Zeilen" />
        <MetricCard label="Airlines" value={numberLabel(database.airline_count)} detail="Mastertabelle" />
        <MetricCard label="CSV-Dateien" value={numberLabel(files.csv_files?.length)} detail="mit Vorschau" />
        <MetricCard label="Raw Payloads" value={numberLabel(files.raw_payloads?.length)} detail="Cache-Dateien" />
        <MetricCard label="Reports" value={numberLabel(files.reports?.length)} detail="Markdown-Berichte" />
        <MetricCard label="Log-Zeilen" value={numberLabel(logAnalytics.line_count)} detail="etl.log gesamt" />
      </section>

      <section className="page-stack">
        <article className="panel panel--wide panel--run-control">
          <div className="panel-header panel-header--wrap">
            <div>
              <p className="panel-kicker">Run Control</p>
              <h2>ETL konfigurieren</h2>
            </div>
            <div className="panel-meta">
              <span>Start: {formatDate(job.started_at)}</span>
              <span>Ende: {formatDate(job.finished_at)}</span>
            </div>
          </div>

          <form className="control-form control-form--stacked" onSubmit={onRun}>
            <section className="control-section control-section--wide">
              <div className="control-section__header">
                <strong>Airports</strong>
                <span>Auswahl waechst automatisch mit weiteren Flughäfen.</span>
              </div>
              <div className="chip-grid chip-grid--airports">
                {displayedAirports.map((airport) => (
                  <button type="button" key={airport.iata} className={`chip ${selectedAirportSet.has(airport.iata) ? 'chip--active' : ''}`} onClick={() => onToggleAirport(airport.iata, 'selectedAirports')}>
                    <span>{airport.iata}</span>
                    <small>{airport.name}</small>
                  </button>
                ))}
              </div>
            </section>

            <div className="control-grid control-grid--meta">
              <section className="control-section">
                <div className="control-section__header">
                  <strong>Flughafen ergänzen</strong>
                </div>
                <div className="inline-input-row">
                  <input
                    type="text"
                    maxLength="3"
                    placeholder="z. B. FRA"
                    value={form.customAirportCode}
                    onChange={(event) => onUpdateForm('customAirportCode', event.target.value.toUpperCase())}
                  />
                  <button className="button button--ghost" type="button" onClick={onAddCustomAirport}>
                    Hinzufügen
                  </button>
                </div>
              </section>

              <section className="control-section">
                <div className="control-section__header">
                  <strong>Zeitfenster</strong>
                </div>
                <div className="control-grid control-grid--time">
                  <label>
                    Lookback (Stunden)
                    <input type="number" min="0" max="12" value={form.lookbackHours} onChange={(event) => onUpdateForm('lookbackHours', event.target.value)} />
                  </label>
                  <label>
                    Lookahead (Stunden)
                    <input type="number" min="0" max="12" value={form.lookaheadHours} onChange={(event) => onUpdateForm('lookaheadHours', event.target.value)} />
                  </label>
                  <label>
                    Von (optional)
                    <input type="datetime-local" value={form.fromDatetime} onChange={(event) => onUpdateForm('fromDatetime', event.target.value)} />
                  </label>
                  <label>
                    Bis (optional)
                    <input type="datetime-local" value={form.toDatetime} onChange={(event) => onUpdateForm('toDatetime', event.target.value)} />
                  </label>
                </div>
              </section>
            </div>

            <div className="control-grid control-grid--bottom">
              <section className="control-section control-section--wide">
                <div className="control-section__header">
                  <strong>Force Refresh</strong>
                  <span>Nur aktiv für ausgewählte Airports.</span>
                </div>
                <div className="checkbox-grid checkbox-grid--airports">
                  {displayedAirports.map((airport) => (
                    <label key={airport.iata} className="checkbox-card">
                      <input type="checkbox" checked={form.forceRefreshAirports.includes(airport.iata)} disabled={!selectedAirportSet.has(airport.iata)} onChange={() => onToggleAirport(airport.iata, 'forceRefreshAirports')} />
                      <span>{airport.iata}</span>
                    </label>
                  ))}
                </div>
              </section>

              <section className="control-section control-section--actions">
                <div className="control-section__header">
                  <strong>Aktionen</strong>
                </div>
                <div className="button-row button-row--actions-stacked">
                  <button className="button button--primary" type="submit" disabled={submitting || !form.selectedAirports.length}>
                    {submitting ? 'Starte ...' : 'ETL neu laden'}
                  </button>
                  <button className="button button--ghost" type="button" onClick={onSaveSettings} disabled={saving || !form.selectedAirports.length}>
                    {saving ? 'Speichere ...' : 'Einstellungen sichern'}
                  </button>
                  <button className="button button--ghost" type="button" onClick={onRefresh}>
                    Jetzt aktualisieren
                  </button>
                </div>
              </section>
            </div>
          </form>
        </article>

        <div className="page-stack">
          <article className="panel panel--wide">
            <div className="panel-header panel-header--wrap">
              <div>
                <p className="panel-kicker">Database Analytics</p>
                <h2>Flights und Top Airlines nach Airport</h2>
              </div>
              <div className="panel-meta panel-meta--inline">
                <span>{database.db_exists ? 'SQLite aktiv' : 'SQLite fehlt'}</span>
                <span>{`${numberLabel(database.db_size_bytes)} B`}</span>
              </div>
            </div>

            <div className="filter-toolbar">
              <FilterChips title="Airport" options={airportOptions} value={selectedAirportFilter} onChange={setSelectedAirportFilter} />
              <FilterChips title="Zeitraum" options={timeframeOptions} value={selectedTimeframe} onChange={setSelectedTimeframe} />
            </div>

            <div className="chart-grid chart-grid--triple-wide">
              <BarChartCard title="Flights pro Airport" subtitle="SQLite Snapshot" data={(database.flights_by_destination || []).map((item) => ({ name: item.destination_airport_iata, count: item.count }))} />
              <BarChartCard title={`Top Airlines ${selectedAirportFilter === 'all' ? '' : `- ${selectedAirportFilter}`}`.trim()} subtitle={selectedTimeframe === 'all' ? 'Alle Zeiten' : timeframeOptions.find((option) => option.value === selectedTimeframe)?.label} data={selectedTopAirlineChart} color="#2b6d4f" />
              <PieChartCard title="Statusverteilung" subtitle="SQLite Snapshot" data={statusChartData} />
            </div>

            <div className="chart-grid chart-grid--double-wide">
              <LineChartCard title="Arrival-Aktivitaet" subtitle={selectedAirportFilter === 'all' ? 'alle Airports' : selectedAirportFilter} data={selectedArrivalActivity} />
              <BarChartCard title="Top Herkunftslaender" subtitle="SQLite Snapshot" data={(database.top_origin_countries || []).map((item) => ({ name: item.origin_country, count: item.count }))} color="#b26d08" />
            </div>

            <div className="table-split">
              <div>
                <h3>Flights pro Airport</h3>
                <DataTable columns={[{ key: 'destination_airport_iata', label: 'Airport' }, { key: 'count', label: 'Flights' }]} rows={database.flights_by_destination} emptyText="Noch keine SQLite-Daten vorhanden." />
              </div>
              <div>
                <h3>Top Airlines gesamt</h3>
                <DataTable columns={[{ key: 'airline_name', label: 'Airline' }, { key: 'count', label: 'Flights' }]} rows={selectedTopAirlines} emptyText="Keine Airline-Statistik verfuegbar." />
              </div>
            </div>

            <div>
              <h3>Top Airlines pro Airport</h3>
              <div className="airport-airline-grid">
                {airportTopAirlineGroups.length ? (
                  airportTopAirlineGroups.map((group) => (
                    <article key={group.airportCode} className="airport-airline-card">
                      <div className="airport-airline-card__header">
                        <div>
                          <strong>{group.airportCode}</strong>
                          <span>
                            {selectedTimeframe === 'all'
                              ? 'Alle Zeiten'
                              : timeframeOptions.find((option) => option.value === selectedTimeframe)?.label}
                          </span>
                        </div>
                      </div>
                      <DataTable
                        columns={[
                          { key: 'airline_name', label: 'Airline' },
                          { key: 'count', label: 'Flights' },
                        ]}
                        rows={group.rows}
                        emptyText="Keine Airline-Statistik verfuegbar."
                        compact
                      />
                    </article>
                  ))
                ) : (
                  <div className="empty-panel">Keine Airline-Statistik pro Airport verfuegbar.</div>
                )}
              </div>
            </div>
          </article>

          <section className="page-stack page-stack--sections">
            <article className="panel panel--wide">
              <div className="panel-header">
                <div>
                  <p className="panel-kicker">CSV Overview</p>
                  <h2>Dateivorschau</h2>
                </div>
              </div>
              {files.csv_files?.length ? (
                files.csv_files.map((file) => <FilePreviewCard key={file.relative_path} file={file} chartPalette={chartPalette} />)
              ) : (
                <div className="empty-panel">Keine CSV-Dateien gefunden.</div>
              )}
            </article>

            <article className="panel panel--wide">
              <div className="panel-header">
                <div>
                  <p className="panel-kicker">Observability</p>
                  <h2>Logs visualisiert</h2>
                </div>
              </div>
              <div className="observability-grid">
                <BarChartCard title="Log-Level" subtitle="INFO / WARNING / ERROR" data={logAnalytics.level_counts} color="#b26d08" />
                <LineChartCard title="Log-Aktivitaet" subtitle="letzte Stunden" data={logAnalytics.hourly_activity} />
              </div>
              <div className="observability-grid observability-grid--bottom">
                <div>
                  <h3>Letzte Log-Events</h3>
                  <DataTable columns={[{ key: 'timestamp', label: 'Zeit' }, { key: 'level', label: 'Level' }, { key: 'logger', label: 'Logger' }, { key: 'message', label: 'Nachricht' }]} rows={logAnalytics.recent_events} emptyText="Noch keine Log-Events vorhanden." />
                </div>
                <div className="code-panel">
                  <h3>etl.log</h3>
                  <pre>{snapshot?.log_tail || 'Noch keine Logs vorhanden.'}</pre>
                </div>
              </div>
            </article>

            <article className="panel panel--wide panel--stacked">
              <div className="panel-header">
                <div>
                  <p className="panel-kicker">Explorer</p>
                  <h2>Schema, Raw und letzte Flights</h2>
                </div>
              </div>
              <div className="explorer-grid">
                <div>
                  <h3>Raw Payloads</h3>
                  <DataTable columns={[{ key: 'airport_iata', label: 'Airport' }, { key: 'arrival_count', label: 'Arrivals' }, { key: 'fetch_source', label: 'Quelle' }]} rows={files.raw_payloads} emptyText="Noch keine Raw-Payloads vorhanden." />
                </div>
                <div>
                  <h3>Flights Tabelle</h3>
                  <DataTable columns={[{ key: 'name', label: 'Spalte' }, { key: 'type', label: 'Typ' }, { key: 'notnull', label: 'Not Null', render: (value) => (value ? 'ja' : 'nein') }, { key: 'primary_key', label: 'PK', render: (value) => (value ? 'ja' : 'nein') }]} rows={database.tables?.flights} emptyText="Schema nicht verfuegbar." />
                </div>
                <div>
                  <h3>Letzte Flights</h3>
                  <DataTable columns={[{ key: 'flight_number', label: 'Flug' }, { key: 'airline_name', label: 'Airline' }, { key: 'destination_airport_iata', label: 'Airport' }, { key: 'arrival_time', label: 'Arrival' }]} rows={database.recent_flights} emptyText="Keine Flugdaten vorhanden." />
                </div>
              </div>
            </article>
          </section>
        </div>
      </section>
    </div>
  );
}