import { Suspense, lazy, startTransition, useCallback, useEffect, useMemo, useRef, useState } from 'react';

const OverviewPage = lazy(() => import('./pages/OverviewPage.jsx'));
const SqlExplorerPage = lazy(() => import('./pages/SqlExplorerPage.jsx'));

const initialForm = {
  selectedAirports: [],
  lookbackHours: 3,
  lookaheadHours: 9,
  fromDatetime: '',
  toDatetime: '',
  forceRefreshAirports: [],
  customAirportCode: '',
};

export default function App() {
  const [snapshot, setSnapshot] = useState(null);
  const [form, setForm] = useState(initialForm);
  const [error, setError] = useState('');
  const [infoMessage, setInfoMessage] = useState('');
  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [saving, setSaving] = useState(false);
  const [formDirty, setFormDirty] = useState(false);
  const [activePage, setActivePage] = useState('overview');
  const hasHydratedForm = useRef(false);

  const fetchJson = useCallback(async (path, options) => {
    const response = await fetch(path, {
      headers: {
        'Content-Type': 'application/json',
      },
      ...options,
    });
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      throw new Error(body.detail || `Request failed with status ${response.status}`);
    }
    return response.json();
  }, []);

  const loadDashboard = useCallback(async (showSpinner = false) => {
    if (showSpinner) {
      setLoading(true);
    }
    try {
      const data = await fetchJson('/api/dashboard');
      startTransition(() => {
        setSnapshot(data);
      });
      setError('');
    } catch (loadError) {
      setError(loadError.message);
    } finally {
      if (showSpinner) {
        setLoading(false);
      }
    }
  }, [fetchJson]);

  useEffect(() => {
    loadDashboard(true);
  }, [loadDashboard]);

  useEffect(() => {
    if (activePage !== 'overview') {
      return undefined;
    }

    const interval = window.setInterval(() => {
      loadDashboard(false);
    }, 8000);

    return () => window.clearInterval(interval);
  }, [activePage, loadDashboard]);

  useEffect(() => {
    if (!snapshot?.settings) {
      return;
    }

    if (!hasHydratedForm.current || !formDirty) {
      setForm({
        selectedAirports: snapshot.settings.selected_airports,
        lookbackHours: snapshot.settings.lookback_hours,
        lookaheadHours: snapshot.settings.lookahead_hours,
        fromDatetime: '',
        toDatetime: '',
        forceRefreshAirports: snapshot.settings.selected_airports,
        customAirportCode: '',
      });
      hasHydratedForm.current = true;
    }
  }, [snapshot, formDirty]);

  const pageTitle = useMemo(() => {
    if (activePage === 'sql') {
      return 'SQL Explorer und DB-Preview';
    }
    return 'Flight ETL Dashboard';
  }, [activePage]);

  function updateForm(key, value) {
    setFormDirty(true);
    setForm((current) => ({ ...current, [key]: value }));
  }

  function toggleAirport(code, field) {
    setFormDirty(true);
    setForm((current) => {
      const values = new Set(current[field]);
      if (values.has(code)) {
        values.delete(code);
      } else {
        values.add(code);
      }

      const nextState = {
        ...current,
        [field]: Array.from(values),
      };

      if (field === 'selectedAirports') {
        nextState.forceRefreshAirports = nextState.forceRefreshAirports.filter((airportCode) =>
          nextState.selectedAirports.includes(airportCode),
        );
      }
      return nextState;
    });
  }

  function addCustomAirport() {
    const normalizedCode = form.customAirportCode.trim().toUpperCase();
    if (!/^[A-Z]{3}$/.test(normalizedCode)) {
      setError('Zusätzliche Airports müssen als 3-stelliger IATA-Code eingegeben werden.');
      return;
    }

    setError('');
    setFormDirty(true);
    setForm((current) => ({
      ...current,
      selectedAirports: current.selectedAirports.includes(normalizedCode)
        ? current.selectedAirports
        : [...current.selectedAirports, normalizedCode],
      customAirportCode: '',
    }));
  }

  async function handleSaveSettings(event) {
    event.preventDefault();
    setSaving(true);
    setInfoMessage('');
    try {
      await fetchJson('/api/settings', {
        method: 'PUT',
        body: JSON.stringify({
          selected_airports: form.selectedAirports,
          lookback_hours: Number(form.lookbackHours),
          lookahead_hours: Number(form.lookaheadHours),
        }),
      });
      setFormDirty(false);
      setInfoMessage('Dashboard-Einstellungen wurden gespeichert.');
      await loadDashboard(false);
    } catch (saveError) {
      setError(saveError.message);
    } finally {
      setSaving(false);
    }
  }

  async function handleRun(event) {
    event.preventDefault();
    setSubmitting(true);
    setInfoMessage('');
    try {
      await fetchJson('/api/etl/run', {
        method: 'POST',
        body: JSON.stringify({
          selected_airports: form.selectedAirports,
          lookback_hours: Number(form.lookbackHours),
          lookahead_hours: Number(form.lookaheadHours),
          from_datetime: form.fromDatetime || null,
          to_datetime: form.toDatetime || null,
          force_refresh_airports: form.forceRefreshAirports,
        }),
      });
      setInfoMessage('ETL-Lauf wurde gestartet.');
      await loadDashboard(false);
    } catch (runError) {
      setError(runError.message);
    } finally {
      setSubmitting(false);
    }
  }

  if (loading && !snapshot) {
    return <main className="app-shell"><div className="loading-screen">Dashboard wird geladen ...</div></main>;
  }

  return (
    <main className="app-shell app-shell--wide">
      <header className="topbar">
        <div>
          <p className="eyebrow">Flight ETL Admin</p>
          <h1 className="page-title">{pageTitle}</h1>
        </div>
        <nav className="page-switcher" aria-label="Dashboard pages">
          <button
            type="button"
            className={`page-switcher__button ${activePage === 'overview' ? 'page-switcher__button--active' : ''}`}
            onClick={() => setActivePage('overview')}
          >
            Overview
          </button>
          <button
            type="button"
            className={`page-switcher__button ${activePage === 'sql' ? 'page-switcher__button--active' : ''}`}
            onClick={() => setActivePage('sql')}
          >
            SQL Explorer
          </button>
        </nav>
      </header>

      <section className="content-stage">
        {error ? <div className="banner banner--error">{error}</div> : null}
        {infoMessage ? <div className="banner banner--info">{infoMessage}</div> : null}

        <Suspense fallback={<div className="loading-panel">Ansicht wird geladen ...</div>}>
          {activePage === 'overview' ? (
            <OverviewPage
              snapshot={snapshot}
              form={form}
              submitting={submitting}
              saving={saving}
              onUpdateForm={updateForm}
              onToggleAirport={toggleAirport}
              onAddCustomAirport={addCustomAirport}
              onRun={handleRun}
              onSaveSettings={handleSaveSettings}
              onRefresh={() => loadDashboard(false)}
            />
          ) : (
            <SqlExplorerPage snapshot={snapshot} fetchJson={fetchJson} />
          )}
        </Suspense>
      </section>
    </main>
  );
}