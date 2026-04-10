# Constructionary Data Extraction — Approaches

Target: `https://gw.promasch.in` → Constructionary module  
Goal: Extract all parts (name, prices, vendors, specs) across all folder categories for migration.

---

## Approach 1 — GWT-RPC Response Capture (API Interception)

**File:** `main.py` (initial version)  
**Status:** Attempted, partially worked — payloads captured but unreadable

### What it does

- Login with Playwright
- Attach `page.on("response", ...)` listener
- Click folders in the left panel to trigger backend calls
- Save raw response bodies to JSONL

### Problem

The backend uses **GWT-RPC** — all responses are in the format:

```
//OK[0,0,-245,...,'E9',4,0,-101,...,[" java.util.ArrayList/...", "com.l3.depth..."],0,7]
```

This is a binary-like serialized format, not JSON. Numbers are back-references into a string table; strings are Java class names. Raw capture is unreadable without a proper decoder.

### Lesson

GWT-RPC responses cannot be used directly — require a custom parser to decode the string table and map stream indices to fields.

---

## Approach 2 — UI Scraping with DOM Selectors (Playwright)

**File:** `main.py` (current)  
**Status:** Active — selector discovery complete, extraction working

### What it does

1. Login and navigate to Constructionary
2. Walk the left panel folder tree (`.folderTileNew`, `.categoryTileNew`)
3. Detect leaf folders by name pattern (`N Specifications` = leaf)
4. For each leaf folder: scroll through part cards (`.partsSectionBorderDark`)
5. Extract fields from each card using exact CSS class selectors
6. Click `Vendors(N)` link per card → scrape vendor popup (`.gwt-PopupPanel`)
7. Save one JSON line per part to `parts.jsonl`

### Selector discovery process

The GWT app uses obfuscated class names — not standard ARIA roles. To discover them:

1. **Home page snapshot** (`--snapshot`):
   - Saved `css_classes.json` → revealed class names like `folderTileNew`, `categoryTileNew`, `rightFolderTile`, `partsSectionBorderDark`
   - All tree/card selectors returned count 0 (snapshot taken before navigating into a folder)

2. **Leaf page snapshot** (`--snapshot-leaf`):
   - Navigated to first real leaf folder, saved snapshot there
   - `selector_probe_leaf.json` confirmed:
     - `.partsSectionBorderDark` → **2** (part cards ✅)
     - `.ConstructionaryDetailsPanel` → 1 (full right panel, too broad)
     - `.gwt-PopupPanel` → 2 (vendor popup ✅)
   - `css_classes_leaf.json` revealed field-level classes:
     - `.lppValue` / `.lppValueGreen` → Last Purchase Price
     - `.lppDate` → Last Purchase Price date
     - `.mpMyPurchaseHistoryQty` → Purchase qty
     - `.mpMyPurchaseHistoryValueBig` → Purchase ₹ value
     - `.mpTotalDemandText` / `.mpTotalDemandValue` → Total demand
     - `.marketPlaceBoxBorder` → Marketplace price section
     - `.partUsed` → Used In count
     - `.divPerson` → Created By

### Problems encountered and fixes

| Problem | Cause | Fix |
|---|---|---|
| Folder tree matched 0 elements | GWT doesn't use `role="treeitem"` — uses custom classes | Used `.folderTileNew, .categoryTileNew` from DOM snapshot |
| Every folder classified as leaf | `is_leaf_folder()` checked for `"text=Parts"` — folder names contain "Parts" so always True | Replaced with `is_leaf_by_name()` — regex on `N Categories` vs `N Specifications` in name |
| 0 parts extracted from all folders | `.rightFolderTile` matched sub-category tiles (right panel), not part cards | Added `.partsSectionBorderDark` as primary; confirmed via leaf snapshot |
| Field extraction unreliable | Text-line parsing fragile — GWT renders content in unexpected order | Replaced with class-based locators: `.lppValue`, `.mpMyPurchaseHistoryQty`, etc. |
| Vendor popup not found | Popup selector list tried `.gwt-DialogBox` first — not present in this app | `.gwt-PopupPanel` confirmed working (count 2 in snapshot) |

### Output schema (`parts.jsonl`)

```json
{
  "part_name": "Daikin(RXQ6ARY6).151",
  "folder_path": "AC & Hot Units > AC Outdoor Unit (VRV/VRF) (Nos)",
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
    { "type": "marketplace", "name": "BLUE STAR LIMITED", "location": "Gujarat" }
  ],
  "extracted_at": 1774600000000
}
```

### Run

```bash
uv run python main.py --headful --sel-card ".partsSectionBorderDark"
```

---

## Approach 3 — GWT-RPC Payload Capture + Custom Parser

**Files:** `api-extraction/collector.py`, `api-extraction/gwt_parser.py`  
**Status:** Working — successfully extracting structured part data

### What it does

Rather than reading the DOM, this approach:

1. **Intercepts only `getPartDetails` RPC calls** (filtered by request `post_data` containing `"getPartDetails"`)
2. Saves raw request payload + response body per call
3. **Parses GWT-RPC format** with a custom decoder:
   - Strips `//OK` envelope
   - Collapses `.concat([...])` chunked arrays (large responses are split)
   - Splits primitive stream and string table
   - Locates display name positions (pattern: `Brand(Model).ID`)
   - Extracts per-part segment between display name positions
   - Maps string table refs → vendor names, locations, images, person names
   - Extracts price candidates from floats outside GWT sentinel range

### Parser logic (key steps)

```
Raw text  →  normalize_gwt_response()  →  Python list
         →  split_primitive_stream_and_table()  →  (primitives, string_table, tail)
         →  find_display_name_positions()  →  [(index, "Brand(Model).ID"), ...]
         →  per-part: extract_part_data_from_segment()
                →  vendor names (heuristic: pvt/ltd/llp keywords)
                →  locations (India state names lookup)
                →  images (http URLs)
                →  prices (floats > 100, outside GWT sentinels [-30, 0])
```

### Sample parsed output

```json
{
  "brand": "Daikin",
  "model": "FXMQ125PBV36",
  "id": "FXMQ125PBV36.19",
  "display_name": "Daikin(FXMQ125PBV36).19",
  "price_last_purchase": 11966.955,
  "price_market": 42739.125,
  "purchase_qty": null,
  "vendor_count": 9,
  "vendors": [
    "DAIKIN AIRCONDITIONING INDIA PVT. LTD. (UP)",
    "DAIKIN AIRCONDITIONING INDIA PVT. LTD. (G)",
    "Bliss Refrigeration Pvt. Ltd. (Godown)"
  ],
  "location": "Uttar Pradesh",
  "images": ["https://promaschprodn.s3.ap-south-1.amazonaws.com/..."],
  "created_by": "Rohit Kumar"
}
```

### Run

```bash
uv run python api-extraction/collector.py --headful
```

### Known limitations

- `price_market` can be inflated (GWT stream includes cumulative purchase amounts, hard to isolate exact market price)
- `purchase_qty` often null (stream layout varies per part)
- `category_path` not populated yet (needs folder context from tree walker)
- Vendor location is city/state only — not full address
- Some `created_by` values are company names mis-detected as person names

---

## Approach Comparison

| | Approach 2 (UI Scraping) | Approach 3 (API + Parser) |
|---|---|---|
| Speed | Slow (clicks + scrolls + vendor popups) | Fast (intercepts backend directly) |
| Field accuracy | High (reads rendered text) | Medium (price parsing heuristic) |
| Vendor detail | Name + location (from popup) | Name only (from string table) |
| Images | No | Yes (S3 URLs) |
| Category path | Yes (from tree walk) | Partial (needs tree context) |
| Robustness | Fragile if UI changes | Fragile if GWT stream layout changes |
| Requires headful | Optional (headless works) | Optional |

### Recommended strategy

Use **both in parallel**:
- Approach 3 for bulk speed and image URLs
- Approach 2 for vendor location detail and marketplace price accuracy
- Cross-reference on `display_name` (`Brand(Model).ID`) as the join key
