import { useEffect, useMemo, useState } from 'react';

function humanizeColumnName(column) {
  const explicitLabels = {
    destination_airport_iata: 'Zielflughafen',
    destination_airport_name: 'Flughafenname',
    origin_airport_iata: 'Abflug',
    origin_country: 'Land',
    flight_number: 'Flug',
    airline_name: 'Airline',
    arrival_time: 'Ankunft',
    row_count: 'Zeilen',
    execution_ms: 'Dauer (ms)',
  };

  if (explicitLabels[column]) {
    return explicitLabels[column];
  }

  return column
    .split('_')
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function formatCellValue(value, column) {
  if (value === null || value === undefined || value === '') {
    return 'n/a';
  }

  if (column.endsWith('_time') || column.endsWith('_at')) {
    const parsed = new Date(value);
    if (!Number.isNaN(parsed.getTime())) {
      return parsed.toLocaleString('de-DE');
    }
  }

  return String(value);
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
  paginationPosition = 'bottom',
}) {
  if (!rows?.length || !columns?.length) {
    return <div className="empty-panel">{emptyText}</div>;
  }

  const totalPages = paginated ? Math.max(1, Math.ceil(rows.length / rowsPerPage)) : 1;
  const safePage = Math.min(Math.max(1, currentPage), totalPages);
  const visibleRows = paginated
    ? rows.slice((safePage - 1) * rowsPerPage, safePage * rowsPerPage)
    : rows;

  const pagination = paginated ? (
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
  ) : null;

  return (
    <div className="table-block">
      {paginationPosition === 'top' ? pagination : null}

      <div
        className={`table-wrap ${compact ? 'table-wrap--dense' : ''} ${fixedHeight ? 'table-wrap--fixed-height' : ''}`}
        style={fixedHeight ? { maxHeight: `${fixedHeight}px` } : undefined}
      >
        <table className={compact ? 'table--dense' : ''}>
          <thead>
            <tr>
              {columns.map((column) => (
                <th key={column}>{humanizeColumnName(column)}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {visibleRows.map((row, index) => (
              <tr key={`${row.id || row.flight_number || row.airline_name || index}`}>
                {columns.map((column) => (
                  <td key={column}>{formatCellValue(row[column], column)}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {paginationPosition !== 'top' ? pagination : null}
    </div>
  );
}

const sqlTemplates = [
  {
    label: 'Flights je Airport',
    sql: 'SELECT destination_airport_iata, COUNT(*) AS flights FROM flights GROUP BY destination_airport_iata ORDER BY flights DESC;',
  },
  {
    label: 'Top Airlines',
    sql: 'SELECT a.name AS airline_name, COUNT(*) AS flights FROM flights f JOIN airlines a ON a.airline_id = f.airline_id GROUP BY a.name ORDER BY flights DESC LIMIT 10;',
  },
  {
    label: 'Letzte Flights',
    sql: 'SELECT flight_number, destination_airport_iata, arrival_time, status FROM flights ORDER BY arrival_time DESC LIMIT 25;',
  },
];

export default function SqlExplorerPage({ snapshot, fetchJson }) {
  const [selectedTable, setSelectedTable] = useState('flights');
  const [explorerData, setExplorerData] = useState(null);
  const [explorerPage, setExplorerPage] = useState(1);
  const [sql, setSql] = useState(sqlTemplates[0].sql);
  const [queryResult, setQueryResult] = useState(null);
  const [loadingExplorer, setLoadingExplorer] = useState(false);
  const [runningQuery, setRunningQuery] = useState(false);
  const [error, setError] = useState('');

  const fallbackTables = useMemo(() => snapshot?.database?.tables ? Object.keys(snapshot.database.tables) : ['flights', 'airlines'], [snapshot]);

  useEffect(() => {
    async function loadExplorer() {
      setLoadingExplorer(true);
      setExplorerPage(1);
      try {
        const data = await fetchJson('/api/database/explorer', {
          method: 'POST',
          body: JSON.stringify({ table_name: selectedTable, limit: 50 }),
        });
        setExplorerData(data);
        setError('');
      } catch (loadError) {
        setError(loadError.message);
      } finally {
        setLoadingExplorer(false);
      }
    }

    loadExplorer();
  }, [fetchJson, selectedTable]);

  async function runQuery(event) {
    event.preventDefault();
    setRunningQuery(true);
    try {
      const data = await fetchJson('/api/sql/query', {
        method: 'POST',
        body: JSON.stringify({ sql }),
      });
      setQueryResult(data);
      setError('');
    } catch (queryError) {
      setError(queryError.message);
    } finally {
      setRunningQuery(false);
    }
  }

  return (
    <div className="page-stack">
      {error ? <div className="banner banner--error">{error}</div> : null}

      <section className="page-stack">
        <article className="panel panel--stacked">
          <div className="panel-header panel-header--wrap">
            <div>
              <p className="panel-kicker">SQL Explorer</p>
              <h2>Read-only SQL direkt im Dashboard</h2>
            </div>
          </div>

          <form className="sql-form" onSubmit={runQuery}>
            <label>
              SQL Query
              <textarea value={sql} onChange={(event) => setSql(event.target.value)} rows={8} className="sql-editor" />
            </label>
            <div className="filter-chip-row">
              {sqlTemplates.map((template) => (
                <button key={template.label} type="button" className="filter-chip" onClick={() => setSql(template.sql)}>
                  {template.label}
                </button>
              ))}
            </div>
            <div className="button-row button-row--wide">
              <button className="button button--primary" type="submit" disabled={runningQuery}>
                {runningQuery ? 'Fuehre aus ...' : 'Query ausfuehren'}
              </button>
            </div>
          </form>

          <div className="result-meta">
            <span>Nur SELECT, WITH und PRAGMA sind erlaubt.</span>
            {queryResult ? <span>{`${queryResult.row_count} Zeilen in ${queryResult.execution_ms} ms`}</span> : null}
          </div>

          <DataTable columns={queryResult?.columns || []} rows={queryResult?.rows || []} emptyText="Noch kein SQL-Ergebnis vorhanden." compact />
        </article>

        <article className="panel panel--stacked">
          <div className="panel-header panel-header--wrap">
            <div>
              <p className="panel-kicker">DB Explorer</p>
              <h2>Tabellen und Datenvorschau</h2>
            </div>
          </div>

          <div className="filter-chip-row">
            {fallbackTables.map((tableName) => (
              <button key={tableName} type="button" className={`filter-chip ${selectedTable === tableName ? 'filter-chip--active' : ''}`} onClick={() => setSelectedTable(tableName)}>
                {tableName}
              </button>
            ))}
          </div>

          {loadingExplorer ? <div className="empty-panel">Tabellenvorschau wird geladen ...</div> : null}

          {explorerData ? (
            <>
              <div className="metrics-grid metrics-grid--compact">
                {explorerData.tables.map((table) => (
                  <article key={table.name} className="metric-card metric-card--compact">
                    <div className="metric-card__content">
                      <p>{table.name}</p>
                      <span>{table.columns.length} Spalten</span>
                    </div>
                    <strong>{table.row_count}</strong>
                  </article>
                ))}
              </div>
              <DataTable
                columns={(explorerData.preview_rows?.[0] && Object.keys(explorerData.preview_rows[0])) || []}
                rows={explorerData.preview_rows || []}
                emptyText="Keine Preview-Daten vorhanden."
                compact
                paginated
                rowsPerPage={16}
                currentPage={explorerPage}
                onPageChange={setExplorerPage}
                paginationPosition="top"
              />
            </>
          ) : null}
        </article>
      </section>
    </div>
  );
}