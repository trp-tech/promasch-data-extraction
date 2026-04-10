# Constructionary Extractor (Playwright)

Extracts parts data from the Constructionary module at `https://gw.promasch.in`.

## Extraction flow

```
Login → Open Constructionary → Walk folder tree (left panel)
  → For each leaf folder: scroll through all part cards
    → Per card: extract fields + click Vendors link → extract vendor popup
  → Save structured JSONL
```

## Setup

```bash
uv sync
uv run playwright install chromium
```

## Step 1 — Run snapshot (first time)

Since the app is GWT-based, first save a DOM snapshot to find the real CSS selectors:

```bash
export CONSTRUCTIONARY_USER="your_user"
export CONSTRUCTIONARY_PASSWORD="your_password"

uv run python main.py --headful --snapshot
```

This saves to `data/constructionary/<timestamp>/`:
- `snapshot.html` — full page HTML
- `css_classes.json` — all CSS class names on the page
- `selector_probe.json` — which built-in selectors matched (count > 0 = working)
- `frames.json` — iframe breakdown

Open `selector_probe.json` — find selectors with a count > 0 and note them.

## Step 2 — Full extraction

```bash
uv run python main.py --headful
```

If auto-detection fails, override selectors manually:

```bash
uv run python main.py --headful \
  --sel-tree ".gwt-TreeItem" \
  --sel-card "[class*='partCard']" \
  --sel-popup ".gwt-DialogBox"
```

## All options

| Flag | Default | Description |
|------|---------|-------------|
| `--headful` | false | Show browser window |
| `--snapshot` | false | Dump DOM for selector discovery, then exit |
| `--max-folders` | 200 | Safety cap on folder clicks |
| `--scroll-rounds` | 30 | Max scroll iterations per folder to load all cards |
| `--sel-tree` | auto | Override folder tree selector |
| `--sel-card` | auto | Override part card selector |
| `--sel-popup` | auto | Override vendor popup selector |
| `--output-dir` | `data/constructionary` | Output base directory |

## Output

Each run creates a timestamped folder: `data/constructionary/<timestamp>/`

### `parts.jsonl`

One JSON line per part:

```json
{
  "part_name": "Daikin(RXQ6ARY6).151",
  "folder_path": "AC Outdoor Unit (VRV/VRF) (Nos)",
  "uom": "Nos",
  "my_purchase_history_qty": "1 Nos",
  "my_purchase_history_worth": "Worth ₹1.42 L",
  "last_purchase_price": "₹ 110588",
  "last_purchase_date": "22 Oct 2019 12:35:53",
  "marketplace_price": "₹ 124776.61",
  "marketplace_price_date": "10 Nov 2024 12:39:40",
  "total_demand": "0 Nos",
  "immediate_demand": "0 Nos",
  "created_by": "Other | Other",
  "vendor_count": 5,
  "vendors": [
    { "type": "my_vendor", "name": "Acme HVAC", "location": "Delhi" },
    { "type": "marketplace", "name": "BLUE STAR LIMITED", "location": "Gujarat" }
  ],
  "extracted_at": 1774600000000
}
```

### `summary.json`

Totals + selector info used for the run.

### `folder_log.json`

Per-folder click log (leaf vs category, parts count).

## API extraction (GWT `getPartDetails`)

Python pipeline under [`api-extraction/`](api-extraction/): capture RPC payloads with Playwright, replay with `requests`, decode responses with [`api-extraction/gwt_parser.py`](api-extraction/gwt_parser.py) (named to avoid clashing with the stdlib `parser` module).

```bash
export CONSTRUCTIONARY_USER="..."
export CONSTRUCTIONARY_PASSWORD="..."

# Full pipeline (creates api-extraction/data/<timestamp>/)
uv run python api-extraction/main.py --all --headful

# Or step by step
uv run python api-extraction/main.py --collect --headful
uv run python api-extraction/main.py --replay --data-dir api-extraction/data/<timestamp>
uv run python api-extraction/main.py --parse --data-dir api-extraction/data/<timestamp>
```

Outputs per run directory: `payloads/`, `dumps/`, `parsed/`, `payload_catalog.json`, `auth_state.json`, `output.json`, `logs/`.
