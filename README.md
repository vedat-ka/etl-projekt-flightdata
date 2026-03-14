# Flight ETL mit SQLite

Dieses Projekt ruft aktuelle Ankunftsdaten mehrerer europaeischer Flughaefen ab, transformiert sie in einer ETL-Pipeline und laedt den letzten Lauf in eine lokale SQLite-Datenbank.

Die Implementierung ist objektorientiert aufgebaut und verwendet Logging für alle Schritte der Pipeline.

Verarbeitete Flughäfen:
- `BER` – Berlin Brandenburg
- `STR` – Stuttgart
- `CDG` – Paris Charles de Gaulle

Verwendete API:
- AeroDataBox API über RapidAPI
- Verwendeter Datentyp: aktuelle Arrival-Flüge pro Flughafen über den IATA-Code
- Im Code genutzt in [src/api_client.py](src/api_client.py)

## Ziel des Projekts

Die Pipeline deckt die drei klassischen ETL-Schritte ab:

1. Extract
Abruf von Arrival-Daten für `BER`, `STR` und `CDG`.

2. Transform
Bereinigung der Rohdaten, Extraktion der Pflichtfelder, Zerlegung der Arrival-Zeit und Normalisierung der Airline-Daten.

3. Load
Speicherung der transformierten Daten in einer lokalen SQLite-Datenbank mit Foreign Key zwischen Flights und Airlines.

Zusätzlich umgesetzt:
- Airline-Mastertabelle
- Integer-Primärschlüssel für SQLite
- fachlicher Unique-Key pro Flugdatensatz
- CSV-Datei zur Sichtkontrolle
- Raw-JSON-Datei pro Flughafen als Cache
- OOP-Struktur mit klar getrennten Verantwortlichkeiten
- zentrales Logging in Konsole und Datei

## Pflichtfelder der Aufgabe

Für jeden Flug werden diese Felder verarbeitet und gespeichert:
- `origin_country`
- `flight_number`
- `airline_name`
- `arrival_time`

Die Arrival-Zeit wird zusätzlich in diese sechs Spalten zerlegt:
- `arrival_year`
- `arrival_month`
- `arrival_day`
- `arrival_hour`
- `arrival_minute`
- `arrival_second`

## SQLite-Datenmodell

Die Datenbank wird lokal gespeichert in [data/flights.sqlite](data/flights.sqlite).

### Tabelle `airlines`

- `airline_id INTEGER PRIMARY KEY AUTOINCREMENT`
- `iata_code TEXT NOT NULL DEFAULT ''`
- `icao_code TEXT NOT NULL DEFAULT ''`
- `name TEXT NOT NULL`
- `UNIQUE(iata_code, icao_code, name)`

### Tabelle `flights`

- `flight_id INTEGER PRIMARY KEY AUTOINCREMENT`
- `destination_airport_iata TEXT NOT NULL`
- `destination_airport_name TEXT NOT NULL`
- `origin_airport_iata TEXT`
- `origin_country TEXT NOT NULL`
- `flight_number TEXT NOT NULL`
- `airline_id INTEGER NOT NULL`
- `arrival_time TEXT NOT NULL`
- `arrival_year INTEGER NOT NULL`
- `arrival_month INTEGER NOT NULL`
- `arrival_day INTEGER NOT NULL`
- `arrival_hour INTEGER NOT NULL`
- `arrival_minute INTEGER NOT NULL`
- `arrival_second INTEGER NOT NULL`
- `status TEXT`
- `source_record_key TEXT NOT NULL UNIQUE`

Wichtig:
- Die Primärschlüssel in SQLite sind reine Integer-Werte.
- Alphanumerische Werte wie `DL 5378` bleiben normale Fachattribute und sind keine Primärschlüssel.
- `flights.airline_id` referenziert `airlines.airline_id`.

## Projektstruktur

```text
flight-etl-sqlite/
├── .env
├── .env.example
├── .gitignore
├── backend/
│   ├── app.py
│   ├── schemas.py
│   └── services.py
├── frontend/
│   ├── package.json
│   ├── vite.config.js
│   └── src/
│       ├── App.jsx
│       ├── main.jsx
│       └── styles.css
├── requirements.txt
├── main.py
├── README.md
├── data/
│   ├── flights.sqlite
│   ├── processed/
│   │   └── flights_processed.csv
│   └── raw/
│       ├── ber_arrivals_raw.json
│       ├── str_arrivals_raw.json
│       └── cdg_arrivals_raw.json
├── logs/
│   └── etl.log
├── reports/
│   └── latest_run_report.md
├── scripts/
│   └── run_sql_checks.py
└── src/
    ├── api_client.py
    ├── config.py
    ├── database.py
    ├── etl.py
    ├── logger_config.py
    └── __init__.py
```

## Voraussetzungen

- Python 3.11 oder neuer
- Node.js 20 oder neuer fuer das React-Dashboard
- RapidAPI-Account mit Zugriff auf die AeroDataBox API
- Windows PowerShell oder ein anderes Terminal

Hinweis:
- Das Projekt wurde in dieser Umgebung mit Python 3.11 unter Windows ausgeführt.
- SQLite ist bereits in Python enthalten. Es muss kein zusätzliches Paket installiert werden.
- API-Keys gehoeren ausschliesslich in `.env` oder in CI-Variablen.

## `.venv` einrichten

### Windows PowerShell

1. In den Projektordner wechseln:

```powershell
Set-Location .\flight-etl-sqlite
```

2. Virtuelles Environment anlegen:

```powershell
py -3.11 -m venv .venv
```

3. Environment aktivieren:

```powershell
.\.venv\Scripts\Activate.ps1
```

4. Abhängigkeiten installieren:

```powershell
python -m pip install -r requirements.txt
```

5. Frontend-Abhaengigkeiten installieren:

```powershell
Set-Location .\frontend
npm install
Set-Location ..
```

### Linux / macOS

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cd frontend && npm install
cd ..
```

## API konfigurieren

Beispieldatei kopieren:

### Windows PowerShell

```powershell
Copy-Item .env.example .env
```

### Linux / macOS

```bash
cp .env.example .env
```

Danach `.env` mit den RapidAPI-Zugangsdaten fuellen:

```env
RAPIDAPI_KEY=dein_schluessel_hier
RAPIDAPI_HOST=aerodatabox.p.rapidapi.com
RAPIDAPI_BASE_URL=https://aerodatabox.p.rapidapi.com
```

`RAPIDAPI_BASE_URL` ist optional konfigurierbar und wird standardmaessig auf AeroDataBox gesetzt.

Sicherheitshinweis:
- `.env` ist eine lokale Datei und darf nicht eingecheckt oder weitergegeben werden.
- Wenn ein echter API-Key versehentlich lokal gespeichert wurde, sollte er im RapidAPI-Konto rotiert werden.

## Pipeline starten

### Mit aktivierter `.venv`

```powershell
python main.py
```

### Ohne Aktivierung der `.venv` unter Windows

```powershell
.\.venv\Scripts\python.exe main.py
```

Beim Start passiert automatisch:
- API-Abruf nur dann, wenn für einen Flughafen noch kein Raw-JSON in `data/raw/` vorhanden ist
- vorhandene Raw-JSON-Dateien werden als Cache wiederverwendet
- neue Rohdaten werden nur bei fehlendem Cache in `data/raw/` gespeichert
- Transformation in ein bereinigtes Tabellenformat
- CSV-Export nach `data/processed/flights_processed.csv`
- Laden in `data/flights.sqlite`
- Erzeugung eines Markdown-Reports in `reports/latest_run_report.md`
- Logging aller Schritte in Konsole und `logs/etl.log`

## Dashboard starten

Das Projekt enthaelt zusaetzlich:

- ein FastAPI-Backend fuer ETL-Steuerung, Status und Dateiansichten
- ein React-Dashboard fuer Verwaltung, Reloads und SQLite-Uebersichten

### FastAPI-Backend starten

Mit aktiver `.venv`:

```powershell
python -m uvicorn backend.app:app --reload
```

Ohne Aktivierung der `.venv` unter Windows:

```powershell
.\.venv\Scripts\python.exe -m uvicorn backend.app:app --reload
```

Das Backend ist danach unter `http://127.0.0.1:8000` erreichbar.

Wichtige API-Endpunkte:

- `GET /api/dashboard`: kombinierter Snapshot fuer das Dashboard
- `POST /api/etl/run`: neuer ETL-Lauf mit Airport- und Zeitfenster-Auswahl
- `PUT /api/settings`: gespeicherte Dashboard-Standardwerte
- `GET /api/database`: SQLite-Kennzahlen, Schema und letzte Flights
- `POST /api/database/explorer`: Tabellenvorschau fuer den DB-Explorer
- `POST /api/sql/query`: read-only SQL-Abfragen (`SELECT`, `WITH`, `PRAGMA`)
- `GET /api/files`: CSV-, Raw-, Log- und Report-Uebersichten
- `GET /api/logs`: Log-Tail aus `logs/etl.log`
- `GET /api/report`: aktueller Markdown-Report

### React-Frontend starten

In einem zweiten Terminal:

```powershell
Set-Location .\frontend
npm run dev
```

Das Dashboard laeuft dann unter `http://127.0.0.1:5173` und spricht per Vite-Proxy mit dem FastAPI-Backend.

### Funktionen des Dashboards

- Airports fuer den naechsten Lauf ein- und ausschalten
- Lookback- und Lookahead-Stunden konfigurieren und speichern
- optional ein eigenes `from`-/`to`-Zeitfenster fuer einen Reload setzen
- einzelne Airports per Force-Refresh neu gegen die API laden
- CSV-Dateien inklusive Vorschaudaten anzeigen
- Balken- und Tortendiagramme aus SQLite-, CSV- und Log-Daten anzeigen
- Top-Airlines interaktiv nach Airport und Tageszeit filtern
- eine eigene SQL-Explorer-Seite fuer Tabellenvorschau und Ad-hoc-Abfragen nutzen
- Raw-JSON-Cache, Reports und Logs ueberblicken
- SQLite-Kennzahlen, Tabellen-Schema und letzte Flights ansehen
- sehen, ob Backend laeuft und ob gerade ein ETL-Job aktiv ist

## Typischer Ablauf

```text
INFO  Flight ETL Pipeline gestartet
INFO  Arrivals fuer konfigurierte Airports abrufen oder Cache verwenden
INFO  Rohdaten validieren, bereinigen und normalisieren
INFO  CSV-Kontrollausgabe erzeugen
INFO  SQLite-Snapshot vollstaendig neu laden
INFO  Report und Logs aktualisieren
INFO  Flight ETL Pipeline erfolgreich beendet
```

## Welche Dateien entstehen?

- [data/raw](data/raw): Raw-JSON-Dateien pro Flughafen als Cache
- [data/processed/flights_processed.csv](data/processed/flights_processed.csv): transformierte Kontrollausgabe
- [data/flights.sqlite](data/flights.sqlite): finale SQLite-Datenbank
- [sql/control_queries.sql](sql/control_queries.sql): Kontrollabfragen für SQLite
- [reports/latest_run_report.md](reports/latest_run_report.md): Ergebnisbericht mit Fetch-Übersicht, Zeitfenstern, Statusverteilung und Top-Listen

Die Raw-JSON-Dateien in [data/raw](data/raw) werden mitgeliefert, damit direkt zum Start Beispieldaten vorhanden sind. Andere Laufartefakte wie CSV, SQLite, Logs und Reports bleiben ueber `.gitignore` ausgeschlossen.

## Wichtige Implementierungsdetails

- Einstiegspunkt: [main.py](main.py)
- Konfiguration und Flughäfen: [src/config.py](src/config.py)
- API-Client-Klasse: [src/api_client.py](src/api_client.py)
- ETL-Pipeline-Klassen: [src/etl.py](src/etl.py)
- SQLite-Manager: [src/database.py](src/database.py)
- Logging-Konfiguration: [src/logger_config.py](src/logger_config.py)

Die Pipeline verwendet `source_record_key` als fachlichen Unique-Key, damit Duplikate beim Laden in SQLite nicht mehrfach angelegt werden.

Die SQLite-Ladestrategie ist ein Full Refresh pro Lauf:
- Vor jedem Load werden `flights` und `airlines` vollständig geleert.
- Danach wird nur der aktuelle ETL-Snapshot geladen.
- Die Datenbank enthält damit immer den Stand des letzten erfolgreichen Laufs und keine historisch aufsummierten Altbestände.

Die Extract-Strategie ist cache-first:
- Existiert für einen Flughafen bereits eine Raw-JSON-Datei, wird kein neuer API-Request gesendet.

## API-Parameter

Die API-Aufrufe sind jetzt über [src/config.py](src/config.py) steuerbar statt fest im Client verdrahtet.

Aktuelle Standardwerte, optimiert fuer dieses ETL-Szenario:
- `codeType=iata`
- `direction=Arrival`
- `withLeg=true`, damit Abflughafen-Informationen fuer Ankuenfte sicher verfuegbar sind
- `withCancelled=false`, damit ausgefallene oder umgeleitete Fluege nicht unnoetig ins ETL laufen
- `withCodeshared=false`, damit Codeshare-Dubletten reduziert werden
- `withCargo=false`
- `withPrivate=false`
- `withLocation=false`

Das Zeitfenster wird beim Request validiert:
- `fromLocal` und `toLocal` muessen im Format `YYYY-MM-DDTHH:mm` vorliegen
- `toLocal` muss spaeter als `fromLocal` sein
- das Fenster darf hoechstens 12 Stunden betragen

Falls ein Flughafen ueber ICAO statt IATA abgefragt werden soll, kann das pro Airport in [src/config.py](src/config.py) gesetzt werden:

```python
AirportConfig(
    iata="BER",
    name="Berlin Brandenburg",
    timezone="Europe/Berlin",
    api_code="EDDB",
    api_code_type="icao",
)
```
- Existiert noch kein Raw-JSON, wird genau ein API-Fetch durchgeführt und das Ergebnis lokal gespeichert.
- Dadurch bleiben unnötige API-Aufrufe und Rate-Limits reduziert.

Model-Training ist bewusst nicht Bestandteil dieses Projekts. Die Implementierung hält sich an die ETL-Beschreibung und konzentriert sich auf Extract, Transform und Load.

## SQL-Kontrollabfragen

Für die Prüfung der Datenbank liegt ein fertiges Set an SQL-Abfragen in [sql/control_queries.sql](sql/control_queries.sql).

Die Datei enthält unter anderem:
- Gesamtanzahl von Airlines und Flights
- Flights pro Zielflughafen
- Top-Airlines
- Top-Origin-Countries
- Duplikatprüfung über `source_record_key`
- Join-Abfrage zwischen `flights` und `airlines`
- `PRAGMA foreign_key_check`

### SQL-Checks direkt ausführen

```powershell
.\.venv\Scripts\python.exe scripts\run_sql_checks.py
```

Das Skript liest [sql/control_queries.sql](sql/control_queries.sql) und führt alle Kontrollabfragen gegen [data/flights.sqlite](data/flights.sqlite) aus.

## Tests ausführen

Die Tests verwenden nur `unittest` aus der Python-Standardbibliothek.

### Alle Tests starten

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -p "test_*.py" -v
```

Abgedeckt werden:
- ETL-Transformation und Zerlegung der Arrival-Zeit
- Deduplication über `source_record_key`
- Foreign-Key-Beziehung in SQLite
- Unique-Key-Verhalten
- Full-Refresh-Ladestrategie pro Lauf

## Report pro Lauf

Nach jedem erfolgreichen ETL-Lauf wird zusätzlich ein kompakter Bericht erzeugt in [reports/latest_run_report.md](reports/latest_run_report.md).

Der Report enthält:
- Anzahl der verarbeiteten Zeilen
- Anzahl der Airlines und Flights in SQLite
- Flights pro Zielflughafen
- Top-Origin-Countries
- Top-Airlines

## Hinweise

- Wenn die API ein Rate Limit liefert, versucht das Projekt den Abruf erneut und nutzt, falls vorhanden, zwischengespeicherte Raw-Dateien als Fallback.
- Nicht jeder API-Response enthält alle optionalen Felder vollständig. In SQLite werden nur valide Datensätze mit den für die Aufgabe benötigten Pflichtfeldern geladen.
- `.env`, CSV und SQLite-Datei sind in `.gitignore` ausgeschlossen. Raw-JSON-Dateien unter [data/raw](data/raw) koennen als Startdaten im Projekt enthalten sein.
- Laufzeit-Logs werden in `logs/etl.log` gespeichert.
- Git-Metadaten wie `.git/HEAD` oder `.git/logs/HEAD` sind nicht Teil des eigentlichen Projektinhalts und werden von `.gitignore` nicht gesteuert.

