
# 📄 Technical Design: Promasch GWT Extraction Pipeline

## 1. Objective

Extract **complete, structured part data** from Promasch’s GWT RPC (`ERPService.getPartDetails`) at scale (10k–60k items), with:

* High fidelity (no UI loss)
* Repeatable pipeline (replayable requests)
* Maintainable parsing (schema-aware, not brittle DOM scraping)

---

## 2. System Architecture

```text
[Playwright Collector] ──► [Raw Dumps (GWT)]
         │
         └──► [Payload Catalog]

[Python Replay Client] ──► [Raw Dumps (GWT)]

[Python Parser]
   ├─ Normalize GWT
   ├─ Extract String Table
   ├─ Detect Block Structure
   ├─ Field Mapping (PartTO)
   └─ Build Structured JSON

[Storage]
   ├─ dumps/
   ├─ parsed/
   └─ logs/
```

---

## 3. GWT Payload & Response Model

### 3.1 Request (Captured)

```
7|0|7|...|com.l3.depth.client.proxy.ERPService|getPartDetails|...|<args>|
```

### 3.2 Response Shape

```text
//OK [
  <primitive stream ...>,
  <object graph ...>,
  <string_table[]>,   ← IMPORTANT
  <flags>
]
```

### 3.3 Key Observations

* **String table (ST)** contains:

  * Class names (`PartTO`, `VendorTO`, …)
  * Human-readable values (names, vendors, URLs)
* **Primitive stream** is a **repeating block per PartTO**
* Structure is **positional + index-referenced**

---

## 4. Data Model (Target Output)

```json
{
  "id": "1",
  "brand": "Daikin",
  "model": "FRWF35TV162.15",
  "display_name": "Daikin(FRWF35TV162).15",
  "price_last_purchase": 22265.625,
  "price_market": 23828.125,
  "purchase_qty": 1,
  "vendor_count": 5,
  "images": ["https://...jpg"],
  "specifications": {},
  "vendors": ["ABC Pvt Ltd", "XYZ Cooling"],
  "location": "Delhi",
   "category_path": [
    "AC Indoor Unit",
    "Cassette AC Unit"
  ],
  "created_by": "Rohit Kumar"
}
```

---

## 5. Phase Plan

## Phase 1 — Playwright Collector

### Goal

Capture:

* GWT request payloads
* GWT responses

### Implementation

```ts
page.on('request', req => {
  if (req.url().includes('ERPService')) {
    save('payloads/', req.postData());
  }
});

page.on('response', async res => {
  if (res.url().includes('ERPService')) {
    const txt = await res.text();
    save('dumps/', txt);
  }
});
```

### Output

```
/payloads/*.txt
/dumps/*.txt
```

---

## Phase 2 — Replay Client (Python)

### Goal

Bypass UI completely

```python
import requests

def replay(payload):
    return requests.post(
        URL,
        data=payload,
        headers={"Content-Type": "text/x-gwt-rpc"}
    ).text
```

---

## Phase 3 — Normalization

### Goal

Make response parseable

```python
def normalize(text):
    text = text.replace("//OK", "")
    text = text.replace("'", '"')
    return json.loads(text)
```

---

## Phase 4 — String Table Extraction

```python
def get_string_table(data):
    return data[-2]
```

---

## Phase 5 — Block Detection (CRITICAL)

### Goal

Find **block size (B)** and **start index (S)**

---

### 5.1 Marker Strategy

Use **stable numeric anchors**:

* price-like values (2000–100000)
* repeated across items

```python
def find_positions(data, predicate):
    return [i for i, v in enumerate(data) if predicate(v)]
```

Example predicate:

```python
lambda v: isinstance(v, float) and 1000 < v < 100000
```

---

### 5.2 Distance Clustering

```python
def detect_block_size(indices):
    diffs = [j - i for i, j in zip(indices, indices[1:])]
    return mode(diffs)
```

👉 Expect stable value (e.g., 110–140)

---

### 5.3 Start Index

Pick first valid block:

```python
START = indices[0] - OFFSET
```

OFFSET determined via inspection (~10–20)

---

## Phase 6 — Block Extraction

```python
def slice_blocks(data, start, size):
    return [
        data[i:i+size]
        for i in range(start, len(data), size)
        if len(data[i:i+size]) == size
    ]
```

---

## Phase 7 — Field Mapping (PartTO)

### 7.1 Known Field Anchors (from your payload)

From your sample:

```text
..., 6234.375, 0, 'XYZ', 20, 0.0, 22265.625, ...
```

### Derived Mapping (example — verify once)

| Offset | Field               |
| ------ | ------------------- |
| +0     | purchase_rate       |
| +2     | internal_code       |
| +3     | uom enum            |
| +5     | market_price        |
| +?     | last_purchase_price |

---

### 7.2 Extraction Functions

```python
def extract_name(block):
    for v in block:
        if isinstance(v, str) and "(" in v and ")." in v:
            return v
    return None
```

```python
def parse_name(name):
    import re
    m = re.match(r"(.*?)\((.*?)\)\.(\d+)", name)
    if not m: return {}
    return {
        "brand": m.group(1),
        "model": m.group(2),
        "id": f"{m.group(2)}.{m.group(3)}"
    }
```

---

### 7.3 Final Block Parser

```python
def parse_block(block, st):
    name = extract_name(block)

    prices = [v for v in block if isinstance(v, float)]

    return {
        **parse_name(name),
        "display_name": name,
        "price_last_purchase": prices[-1] if prices else None,
        "price_market": prices[-2] if len(prices) > 1 else None,
        "purchase_qty": next((v for v in block if isinstance(v, int) and v < 20), None)
    }
```

---

## Phase 8 — String Table Enrichment

```python
def extract_images(st):
    return [s for s in st if isinstance(s, str) and s.startswith("http")]
```

```python
def extract_vendors(st):
    return [
        s for s in st
        if isinstance(s, str)
        and s.isupper() is False
        and "Pvt" in s or "ENTERPRISE" in s
    ]
```

---

## Phase 9 — Aggregation

```python
def build(parts):
    return {
        "total": len(parts),
        "parts": parts
    }
```

---

## Phase 10 — Scaling Strategy

### Parallel Replay

```python
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=5) as ex:
    results = list(ex.map(replay, payloads))
```

---

## 6. Validation Strategy

### Cross-check with UI

For 5–10 samples:

* name ✔
* price ✔
* vendor ✔

---

## 7. Failure Handling

| Issue          | Solution                      |
| -------------- | ----------------------------- |
| Block drift    | dynamic detection per payload |
| Missing fields | nullable mapping              |
| Token expiry   | refresh via Playwright        |

---

## 8. Deliverables

* `collector.ts` → Playwright capture
* `replay.py` → API fetch
* `parser.py` → decode + structure
* `output.json` → final dataset

---

# 🧨 Final Engineering Notes

* Don’t build generic GWT decoder → waste
* Build **PartTO-specific decoder**
* Block slicing = 80% of success
* String table = 100% of semantic value

