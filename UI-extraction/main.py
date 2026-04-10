import argparse
import json
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from playwright.sync_api import Page, sync_playwright

# ── Candidate selectors (tried in order, first match wins) ────────────────────

_TREE_SELECTORS = [
    # Real class names from Constructionary DOM (css_classes.json)
    ".folderTileNew, .categoryTileNew",   # both levels in one pass
    ".folderTileNew",
    ".categoryTileNew",
    ".folderName",
    # Generic fallbacks
    ".gwt-TreeItem",
    "td.gwt-TreeItem",
    "[class*='TreeItem']",
    "[class*='treeItem']",
    "[class*='tree-node']",
    "[class*='folderItem']",
    "[class*='folder-row']",
    "[class*='folder-item']",
]

_CARD_SELECTORS = [
    # Real class names found in Constructionary DOM (from css_classes.json)
    ".partsSectionBorderDark",
    ".rightFolderTile",           # also used for category tiles; tried as fallback
    ".ConstructionaryDetailsPanel",
    # Generic GWT / common patterns
    "[class*='partCard']",
    "[class*='PartCard']",
    "[class*='part-card']",
    "[class*='partListItem']",
    "[class*='PartListItem']",
    "[class*='partItem']",
    "[class*='PartItem']",
    "[class*='partRow']",
    "[class*='PartRow']",
    "[class*='listItem']",
    "[class*='ListItem']",
]

_POPUP_SELECTORS = [
    ".gwt-DialogBox",
    "[class*='DialogBox']",
    "[class*='dialogBox']",
    "[class*='gwt-PopupPanel']",
    "[class*='PopupPanel']",
    "[class*='popupPanel']",
    "[class*='dialog']",
    "[class*='Dialog']",
    "[class*='popup']",
]

# ── Helpers ───────────────────────────────────────────────────────────────────


def clean(text: str) -> str:
    return " ".join(text.split()).strip()


def sanitize(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", value).strip("_") or "item"


def append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def first_match(ctx, selectors: List[str]) -> Tuple[Optional[Any], Optional[str]]:
    """Return (locator, selector_string) for first selector with count > 0."""
    for sel in selectors:
        try:
            loc = ctx.locator(sel)
            if loc.count() > 0:
                return loc, sel
        except Exception:
            continue
    return None, None


# ── Args ──────────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Constructionary Playwright extractor")
    p.add_argument("--base-url", default="https://gw.promasch.in")
    p.add_argument("--user", default=os.getenv("CONSTRUCTIONARY_USER", "Vikram@greenwave.ws"))
    p.add_argument("--password", default=os.getenv("CONSTRUCTIONARY_PASSWORD", "Infosys@9009"))
    p.add_argument("--headful", action="store_true", help="Show browser window")
    p.add_argument(
        "--snapshot",
        action="store_true",
        help="Save DOM snapshot at Constructionary home, then exit",
    )
    p.add_argument(
        "--snapshot-leaf",
        action="store_true",
        help="Navigate to first leaf folder, save snapshot there (finds part card classes), then exit",
    )
    p.add_argument("--output-dir", default="data/constructionary")
    p.add_argument(
        "--max-folders",
        type=int,
        default=200,
        help="Max folder nodes to click (safety cap)",
    )
    p.add_argument(
        "--scroll-rounds",
        type=int,
        default=30,
        help="Max scroll rounds per folder to load all cards",
    )
    # Override selectors from CLI once snapshot reveals real class names
    p.add_argument("--sel-tree", default=None, help="Override tree item selector")
    p.add_argument("--sel-card", default=None, help="Override part card selector")
    p.add_argument("--sel-popup", default=None, help="Override vendor popup selector")
    return p.parse_args()


def ensure_credentials(args: argparse.Namespace) -> None:
    if not args.user or not args.password:
        raise ValueError(
            "Missing credentials. Pass --user/--password or set "
            "CONSTRUCTIONARY_USER / CONSTRUCTIONARY_PASSWORD."
        )


# ── Login ─────────────────────────────────────────────────────────────────────


def login_and_open_constructionary(page: Page, base_url: str, user: str, password: str) -> None:
    page.goto(base_url, wait_until="domcontentloaded", timeout=120_000)
    page.fill('input[type="text"]', user)
    page.fill('input[type="password"]', password)
    page.click('button[type="submit"], button:has-text("Login"), input[type="submit"]')
    page.wait_for_load_state("networkidle", timeout=120_000)
    page.locator("text=Constructionary").first.click()
    page.wait_for_timeout(4000)


# ── Frame / context detection ─────────────────────────────────────────────────


def get_context(page: Page):
    """Return the frame that actually contains the Constructionary UI."""
    # Check main page first
    for marker in ["FOLDER EXPLORER", "Constructionary", "FILTER BY"]:
        if page.locator(f"text={marker}").count() > 0:
            return page

    for frame in page.frames:
        if not frame.url:
            continue
        try:
            for marker in ["FOLDER EXPLORER", "Constructionary", "FILTER BY"]:
                if frame.locator(f"text={marker}").count() > 0:
                    return frame
        except Exception:
            continue

    return page  # fallback


# ── Snapshot mode ─────────────────────────────────────────────────────────────


def save_snapshot(page: Page, run_dir: Path) -> None:
    """Save HTML + class inventory + frame info to help identify correct selectors."""
    run_dir.mkdir(parents=True, exist_ok=True)
    ctx = get_context(page)

    # Full HTML
    html = ctx.content()
    (run_dir / "snapshot.html").write_text(html, encoding="utf-8")

    # All unique CSS class tokens on the page
    classes: List[str] = ctx.evaluate("""() => {
        const all = document.querySelectorAll('[class]');
        const seen = new Set();
        all.forEach(el => {
            el.className.split(' ').forEach(c => { if (c) seen.add(c); });
        });
        return [...seen].sort();
    }""")
    (run_dir / "css_classes.json").write_text(
        json.dumps(classes, indent=2), encoding="utf-8"
    )

    # Probe each candidate selector
    probe: Dict[str, int] = {}
    for sel in _TREE_SELECTORS + _CARD_SELECTORS + _POPUP_SELECTORS:
        try:
            probe[sel] = ctx.locator(sel).count()
        except Exception:
            probe[sel] = -1
    (run_dir / "selector_probe.json").write_text(
        json.dumps(probe, indent=2), encoding="utf-8"
    )

    # Frame inventory
    frames = []
    for f in page.frames:
        try:
            frames.append({
                "url": f.url,
                "name": f.name,
                "gwt_tree_items": f.locator(".gwt-TreeItem").count(),
                "text_preview": f.inner_text("body", timeout=2000)[:300],
            })
        except Exception as e:
            frames.append({"url": getattr(f, "url", "?"), "error": str(e)})
    (run_dir / "frames.json").write_text(json.dumps(frames, indent=2), encoding="utf-8")

    print(f"\n[snapshot] Saved to: {run_dir}")
    print(f"  snapshot.html      — full page DOM")
    print(f"  css_classes.json   — all CSS classes found on page")
    print(f"  selector_probe.json — which selectors matched (count > 0 = working)")
    print(f"  frames.json        — iframe inventory")
    print(
        "\nNext: open selector_probe.json and css_classes.json to find the right selectors,"
        "\nthen re-run with --sel-tree / --sel-card / --sel-popup to override defaults."
    )


# ── Vendor popup extraction ───────────────────────────────────────────────────


def extract_vendor_popup(
    ctx, card, popup_sel: str
) -> Dict[str, Any]:
    """
    Click the 'Vendors(N)' link inside a card, scrape the popup, close it.
    Returns: {"vendor_count": int, "vendors": [{"type", "name", "location"}]}
    """
    result: Dict[str, Any] = {"vendor_count": 0, "vendors": []}

    # Find vendor link inside this card
    vendor_link = card.locator("text=/Vendors?\\(\\d+\\)/i").first
    if vendor_link.count() == 0:
        # Try plain text fallback
        vendor_link = card.locator("text=Vendor").first

    if vendor_link.count() == 0:
        return result

    # Parse count from link text before clicking
    try:
        link_text = vendor_link.inner_text(timeout=2000)
        m = re.search(r"\((\d+)\)", link_text)
        if m:
            result["vendor_count"] = int(m.group(1))
    except Exception:
        pass

    # Click vendor link
    try:
        vendor_link.click(timeout=5000)
        ctx.wait_for_timeout(1500)
    except Exception:
        return result

    # Locate popup
    popup_locator = ctx.locator(popup_sel).first
    if not popup_locator.is_visible(timeout=3000):
        ctx.keyboard.press("Escape")
        return result

    vendors: List[Dict[str, str]] = []

    try:
        popup_text = popup_locator.inner_text(timeout=3000)
        lines = [clean(l) for l in popup_text.split("\n") if clean(l)]

        # Find section boundaries
        my_start = next((i for i, l in enumerate(lines) if "MY VENDORS" in l.upper()), None)
        mkt_start = next((i for i, l in enumerate(lines) if "MARKETPLACE" in l.upper()), None)

        def parse_vendor_block(block_lines: List[str], vtype: str) -> List[Dict[str, str]]:
            """
            Vendor blocks look like:
              1.
              VENDOR NAME
              Location

            OR just:
              VENDOR NAME
              Location
            """
            items: List[Dict[str, str]] = []
            i = 0
            while i < len(block_lines):
                line = block_lines[i]
                # Skip numbered list markers like "1.", "2." etc.
                if re.match(r"^\d+\.$", line):
                    i += 1
                    continue
                # Skip section headers
                if any(h in line.upper() for h in ["MY VENDORS", "MARKETPLACE", "VENDORS"]):
                    i += 1
                    continue
                # Skip empty / dividers
                if not line or line in ["|", "-", "—"]:
                    i += 1
                    continue
                # Assume: name line followed by location line
                name = line
                location = block_lines[i + 1] if (i + 1) < len(block_lines) else ""
                # Skip if next line looks like another name (all caps) or is a number marker
                if re.match(r"^\d+\.$", location):
                    location = ""
                else:
                    i += 1
                items.append({"type": vtype, "name": name, "location": location})
                i += 1
            return items

        if my_start is not None and mkt_start is not None:
            my_lines = lines[my_start + 1: mkt_start]
            mkt_lines = lines[mkt_start + 1:]
            vendors += parse_vendor_block(my_lines, "my_vendor")
            vendors += parse_vendor_block(mkt_lines, "marketplace")
        elif mkt_start is not None:
            mkt_lines = lines[mkt_start + 1:]
            vendors += parse_vendor_block(mkt_lines, "marketplace")
        else:
            # No section headers found; dump all as marketplace
            vendors += parse_vendor_block(lines[1:], "marketplace")

    except Exception as e:
        print(f"    [warn] Vendor popup parse error: {e}")

    result["vendors"] = vendors
    if vendors and result["vendor_count"] == 0:
        result["vendor_count"] = len(vendors)

    # Close popup
    closed = False
    for close_sel in [
        "button:has-text('×')",
        "button:has-text('Close')",
        "[title='Close']",
        ".gwt-DialogBox .Caption button",
        "[class*='close']",
        "[class*='Close']",
    ]:
        try:
            btn = popup_locator.locator(close_sel).first
            if btn.is_visible(timeout=500):
                btn.click(timeout=2000)
                closed = True
                break
        except Exception:
            continue

    if not closed:
        ctx.keyboard.press("Escape")

    ctx.wait_for_timeout(400)
    return result


# ── Part card extraction ──────────────────────────────────────────────────────

def _loc_text(card, selector: str, timeout: int = 2000) -> str:
    """Return inner text of first match inside card, or empty string."""
    try:
        loc = card.locator(selector).first
        if loc.count() > 0:
            return clean(loc.inner_text(timeout=timeout))
    except Exception:
        pass
    return ""


def extract_card_fields(card) -> Dict[str, Any]:
    """
    Extract all fields from a part card using precise CSS class selectors
    discovered from the leaf DOM snapshot (css_classes_leaf.json).

    Card CSS classes mapped to fields:
      Part name / UOM          — first .gwt-Label in card header area
      .lppValue / .lppValueGreen   — Last Purchase Price value
      .lppDate                     — Last Purchase Price date
      .mpMyPurchaseHistoryQty      — Purchase history qty (e.g. "1 Nos")
      .mpMyPurchaseHistoryValueBig — Purchase history ₹ amount
      .mpMyPurchaseHistoryWorthTextBig — "Worth ₹1.42 L"
      .marketPlaceBoxBorder        — Marketplace price section
      .mpTotalDemandText / .mpTotalDemandValue — Total demand
      .mpDemandRightBorder         — Immediate demand section
      .partUsed                    — "Used In (N)"
      .divPerson                   — "Created By" person info
      .demandAggregation           — Demand aggregation section
    """
    part: Dict[str, Any] = {}

    # ── Part name: first non-empty gwt-Label at the top of the card ──────────
    # Fall back to first line of full text if class-based pick fails
    try:
        raw_text = card.inner_text(timeout=4000)
    except Exception:
        return part

    lines = [clean(l) for l in raw_text.split("\n") if clean(l)]
    if not lines:
        return part
    part["part_name"] = lines[0]

    # ── UOM: look for "UOM :" or "UOM:" pattern in card text ─────────────────
    for line in lines:
        low = line.lower()
        if low.startswith("uom") and ":" in line:
            part["uom"] = line.split(":", 1)[-1].strip()
            break

    # ── Last Purchase Price ───────────────────────────────────────────────────
    lpp = _loc_text(card, ".lppValue, .lppValueGreen")
    if lpp:
        part["last_purchase_price"] = lpp
    lpp_date = _loc_text(card, ".lppDate")
    if lpp_date:
        part["last_purchase_date"] = lpp_date

    # ── My Purchase History ───────────────────────────────────────────────────
    ph_qty = _loc_text(card, ".mpMyPurchaseHistoryQty")
    if ph_qty:
        part["my_purchase_history_qty"] = ph_qty
    ph_val = _loc_text(card, ".mpMyPurchaseHistoryValueBig")
    if ph_val:
        part["my_purchase_history_worth"] = ph_val
    ph_worth = _loc_text(card, ".mpMyPurchaseHistoryWorthTextBig")
    if ph_worth and ph_worth != ph_val:
        part.setdefault("my_purchase_history_worth", ph_worth)

    # ── Marketplace price (full section text — extract ₹ value + date) ───────
    mp_section = _loc_text(card, ".marketPlaceBoxBorder")
    if mp_section:
        # Pull out ₹ value
        m_price = re.search(r"₹\s*[\d,]+(?:\.\d+)?", mp_section)
        if m_price:
            part["marketplace_price"] = m_price.group(0).strip()
        # Pull out date
        m_date = re.search(r"\d{2}\s+\w+\s+\d{4}(?:\s+\d{2}:\d{2}:\d{2})?", mp_section)
        if m_date:
            part["marketplace_price_date"] = m_date.group(0).strip()

    # ── Total demand ──────────────────────────────────────────────────────────
    td_text = _loc_text(card, ".mpTotalDemandText")
    td_val = _loc_text(card, ".mpTotalDemandValue")
    if td_val:
        part["total_demand"] = td_val
    elif td_text:
        # Sometimes value is inline: "TOTAL DEMAND: 0 Nos"
        m = re.search(r":\s*(.+)", td_text)
        part["total_demand"] = m.group(1).strip() if m else td_text

    # ── Immediate demand (right border section) ───────────────────────────────
    imm_section = _loc_text(card, ".mpDemandRightBorder")
    if imm_section:
        m = re.search(r"immediate demand[:\s]*([\d\s\w]+)", imm_section, re.IGNORECASE)
        if m:
            part["immediate_demand"] = m.group(1).strip()

    # ── Used In ───────────────────────────────────────────────────────────────
    used_in = _loc_text(card, ".partUsed")
    if used_in:
        part["used_in"] = used_in

    # ── Created By ────────────────────────────────────────────────────────────
    created_by = _loc_text(card, ".divPerson")
    if created_by:
        part["created_by"] = created_by

    # ── Fallback: text-parse remaining fields not yet filled ──────────────────
    # (catches edge cases where class-based extraction missed something)
    for i, line in enumerate(lines):
        low = line.lower()
        if "created by" in low and "created_by" not in part:
            part["created_by"] = line.replace("Created By", "").replace(":", "").strip()
        elif "total demand" in low and "total_demand" not in part:
            part["total_demand"] = line.split(":", 1)[-1].strip() if ":" in line else (lines[i + 1] if i + 1 < len(lines) else "")
        elif "immediate demand" in low and "immediate_demand" not in part:
            part["immediate_demand"] = line.split(":", 1)[-1].strip() if ":" in line else (lines[i + 1] if i + 1 < len(lines) else "")
        elif re.match(r"used in\s*\(", low) and "used_in" not in part:
            part["used_in"] = line
        elif ("marketplace price" in low or "last purchase price" in low) and "last_purchase_price" not in part:
            for j in range(i + 1, min(i + 4, len(lines))):
                if "₹" in lines[j]:
                    if "marketplace" in low:
                        part.setdefault("marketplace_price", lines[j])
                    else:
                        part.setdefault("last_purchase_price", lines[j])
                    break

    return part


# ── Parts scroll loop ─────────────────────────────────────────────────────────


def extract_parts_in_folder(
    ctx,
    folder_path: str,
    run_dir: Path,
    card_sel: str,
    popup_sel: str,
    scroll_rounds: int,
) -> int:
    """
    Scroll through all parts in current folder view, extract each card + vendor popup.
    Returns count of parts written.
    """
    parts_file = run_dir / "parts.jsonl"
    extracted = 0
    seen_names: set = set()
    prev_count = 0
    stable_rounds = 0

    for _ in range(scroll_rounds):
        ctx.wait_for_timeout(600)
        cards = ctx.locator(card_sel)
        count = cards.count()

        for i in range(prev_count, count):
            try:
                card = cards.nth(i)
                fields = extract_card_fields(card)
                if not fields or not fields.get("part_name"):
                    continue
                if fields["part_name"] in seen_names:
                    continue
                seen_names.add(fields["part_name"])

                # Vendor popup
                vendor_data = extract_vendor_popup(ctx, card, popup_sel)
                fields.update(vendor_data)

                fields["folder_path"] = folder_path
                fields["extracted_at"] = int(time.time() * 1000)

                append_jsonl(parts_file, fields)
                extracted += 1

            except Exception as e:
                print(f"    [warn] Card error at index {i}: {e}")

        if count == prev_count:
            stable_rounds += 1
            if stable_rounds >= 3:
                break
        else:
            stable_rounds = 0

        prev_count = count
        # Scroll down in the parts panel
        ctx.mouse.wheel(0, 1800)

    return extracted


# ── Folder tree walking ───────────────────────────────────────────────────────


def is_leaf_by_name(name: str) -> bool:
    """
    Determine if a folder tile is a leaf (has actual parts) or a parent category.

    Folder name patterns observed:
      Parent:  "AC Indoor Unit (VRV/VRF) (Nos) 4 Categories 320 Parts"
      Parent:  "AC Outdoor Unit (VRV/VRF) (Nos) 1 Categories 200 Parts"
      Leaf:    "Cassette AC Unit (VRV/VRF) (Nos) 174 Parts 25 Specifications"
      Leaf:    "AC Outdoor Unit (VRV/VRF) (Nos) 200 Parts 19 Specifications"

    Rule:
      - Contains "N Categories" → NOT a leaf (is a parent category)
      - Contains "N Specifications" with no "N Categories" → leaf
      - Contains only "N Parts" (no Categories, no Specifications) → treat as leaf
    """
    low = name.lower()
    has_categories = bool(re.search(r"\d+\s+categor", low))
    has_specs = bool(re.search(r"\d+\s+specification", low))
    has_parts = bool(re.search(r"\d+\s+parts?", low))

    if has_categories:
        return False  # parent category — must expand further
    if has_specs or has_parts:
        return True   # leaf — has actual parts
    return False


def probe_card_selector(ctx) -> Optional[str]:
    """
    After navigating to a leaf folder, probe all candidate card selectors
    and return the first one with count > 0. Returns None if none match.
    """
    for sel in _CARD_SELECTORS:
        try:
            if ctx.locator(sel).count() > 0:
                return sel
        except Exception:
            continue
    return None


def walk_and_extract(
    ctx,
    run_dir: Path,
    tree_sel: str,
    card_sel: Optional[str],
    popup_sel: str,
    max_folders: int,
    scroll_rounds: int,
    snapshot_leaf: bool = False,
) -> Dict[str, int]:
    """
    Walk the flat list of all folder tiles (both .folderTileNew and .categoryTileNew
    are in the same selector), click each one, detect leaf vs category by NAME,
    and extract parts from leaf folders only.

    Folder path is built by tracking parent context:
      clicking a folderTileNew resets the path root
      clicking a categoryTileNew appends to the current path
    """
    summary: Dict[str, int] = {}
    folder_log: List[Dict[str, Any]] = []

    # Read ALL tile counts before starting (GWT pre-renders hidden items)
    total_nodes = min(ctx.locator(tree_sel).count(), max_folders)
    print(f"\nFound {total_nodes} folder nodes with selector: '{tree_sel}'")

    # Stack-based path tracking: [(name, depth), ...]
    # depth 0 = folderTileNew, depth 1+ = categoryTileNew
    # We detect depth by checking which class the element has
    current_path: List[str] = []

    for i in range(total_nodes):
        folder = ctx.locator(tree_sel).nth(i)
        try:
            name = clean(folder.inner_text(timeout=3000))
        except Exception:
            name = f"folder_{i + 1}"

        if not name:
            name = f"folder_{i + 1}"

        # Determine tile type (top-level folder vs sub-category)
        try:
            cls = folder.get_attribute("class") or ""
        except Exception:
            cls = ""

        is_top_level = "folderTileNew" in cls and "categoryTileNew" not in cls

        # Update path
        if is_top_level:
            current_path = [name]
        else:
            # Sub-category: pop until we find the correct nesting (simplify: just track last 2 levels)
            if current_path:
                current_path = [current_path[0], name]
            else:
                current_path = [name]

        folder_path = " > ".join(current_path)
        is_leaf = is_leaf_by_name(name)

        label = "LEAF" if is_leaf else "CATEGORY"
        print(f"\n[{i + 1}/{total_nodes}] [{label}] {name}")

        try:
            folder.click(timeout=8000)
        except Exception as e:
            print(f"  [skip] Click failed: {e}")
            continue

        try:
            ctx.wait_for_load_state("networkidle", timeout=15_000)
        except Exception:
            pass
        ctx.wait_for_timeout(1200)

        if not is_leaf:
            folder_log.append({"folder": name, "path": folder_path, "parts": 0, "type": "category"})
            continue

        # ── Snapshot-leaf mode: take snapshot at first leaf, then exit ──────
        if snapshot_leaf:
            print(f"\n  [snapshot-leaf] At leaf: {folder_path}")
            print(f"  Taking snapshot ...")
            save_snapshot_at_leaf(ctx, run_dir, folder_path)
            print(f"\nLeaf snapshot saved. Check selector_probe_leaf.json for part card selectors.")
            return summary  # exit early — caller checks snapshot_leaf flag

        # ── Resolve card selector dynamically at leaf ───────────────────────
        active_card_sel = card_sel
        if not active_card_sel:
            active_card_sel = probe_card_selector(ctx)
            if active_card_sel:
                print(f"  [auto-detected card selector] {active_card_sel}")

        if not active_card_sel:
            print(f"  [skip] No card selector matched at leaf — run with --snapshot-leaf")
            folder_log.append({"folder": name, "path": folder_path, "parts": 0, "type": "leaf-no-selector"})
            continue

        # Re-probe if the pre-detected selector yields 0 at this specific leaf
        if ctx.locator(active_card_sel).count() == 0:
            fallback = probe_card_selector(ctx)
            if fallback and fallback != active_card_sel:
                print(f"  [selector fallback] {active_card_sel} → {fallback}")
                active_card_sel = fallback

        print(f"  [extract] path: {folder_path}")
        count = extract_parts_in_folder(
            ctx, folder_path, run_dir, active_card_sel, popup_sel, scroll_rounds
        )
        summary[folder_path] = count
        print(f"  → {count} parts saved")
        folder_log.append({"folder": name, "path": folder_path, "parts": count, "type": "leaf"})

    (run_dir / "folder_log.json").write_text(
        json.dumps(folder_log, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return summary


def save_snapshot_at_leaf(ctx, run_dir: Path, folder_path: str) -> None:
    """Take a focused snapshot inside a leaf folder to discover part card selectors."""
    run_dir.mkdir(parents=True, exist_ok=True)

    # CSS classes present at leaf
    classes: List[str] = ctx.evaluate("""() => {
        const all = document.querySelectorAll('[class]');
        const seen = new Set();
        all.forEach(el => {
            el.className.split(' ').forEach(c => { if (c) seen.add(c); });
        });
        return [...seen].sort();
    }""")
    (run_dir / "css_classes_leaf.json").write_text(
        json.dumps(classes, indent=2), encoding="utf-8"
    )

    # Probe all card selectors at leaf
    probe: Dict[str, int] = {}
    for sel in _CARD_SELECTORS:
        try:
            probe[sel] = ctx.locator(sel).count()
        except Exception:
            probe[sel] = -1
    (run_dir / "selector_probe_leaf.json").write_text(
        json.dumps(probe, indent=2), encoding="utf-8"
    )

    # Save HTML
    html = ctx.content()
    (run_dir / "snapshot_leaf.html").write_text(html, encoding="utf-8")

    print(f"  Leaf: {folder_path}")
    print(f"  Files: css_classes_leaf.json, selector_probe_leaf.json, snapshot_leaf.html")
    # Print selectors that matched
    matched = {k: v for k, v in probe.items() if v > 0}
    print(f"  Matched selectors at leaf: {json.dumps(matched, indent=2)}")


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    args = parse_args()
    ensure_credentials(args)

    run_dir = Path(args.output_dir) / str(int(time.time()))
    run_dir.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not args.headful)
        page = browser.new_page(viewport={"width": 1600, "height": 900})

        login_and_open_constructionary(page, args.base_url, args.user, args.password)
        ctx = get_context(page)

        # ── Snapshot mode (home page) ──────────────────────────────────────
        if args.snapshot:
            save_snapshot(page, run_dir)
            browser.close()
            return

        # ── Selector resolution ────────────────────────────────────────────
        tree_sel = args.sel_tree
        if not tree_sel:
            _, tree_sel = first_match(ctx, _TREE_SELECTORS)

        # card_sel may be None here — walk_and_extract re-probes per leaf
        card_sel = args.sel_card
        if not card_sel:
            _, card_sel = first_match(ctx, _CARD_SELECTORS)
            # Don't abort if no card selector yet — leaf snapshot will find it

        popup_sel = args.sel_popup
        if not popup_sel:
            _, popup_sel = first_match(ctx, _POPUP_SELECTORS)
            if not popup_sel:
                popup_sel = ".gwt-PopupPanel"

        if not tree_sel:
            print(
                "[error] No folder tree selector matched. "
                "Run with --snapshot to inspect DOM, then use --sel-tree to override."
            )
            save_snapshot(page, run_dir)
            browser.close()
            return

        print(f"Using selectors:")
        print(f"  tree  : {tree_sel}")
        print(f"  card  : {card_sel or '(auto-detect per leaf)'}")
        print(f"  popup : {popup_sel}")

        # ── Snapshot-leaf mode ─────────────────────────────────────────────
        if args.snapshot_leaf:
            print("\n[snapshot-leaf] Navigating to first leaf folder for selector discovery ...")
            walk_and_extract(
                ctx,
                run_dir,
                tree_sel=tree_sel,
                card_sel=None,    # don't need card for snapshot
                popup_sel=popup_sel,
                max_folders=args.max_folders,
                scroll_rounds=args.scroll_rounds,
                snapshot_leaf=True,
            )
            browser.close()
            return

        # ── Full extraction ────────────────────────────────────────────────
        summary = walk_and_extract(
            ctx,
            run_dir,
            tree_sel=tree_sel,
            card_sel=card_sel,
            popup_sel=popup_sel,
            max_folders=args.max_folders,
            scroll_rounds=args.scroll_rounds,
            snapshot_leaf=False,
        )

        total_parts = sum(summary.values())

        (run_dir / "summary.json").write_text(
            json.dumps(
                {
                    "total_folders": len(summary),
                    "total_parts": total_parts,
                    "selectors_used": {
                        "tree": tree_sel,
                        "card": card_sel,
                        "popup": popup_sel,
                    },
                    "parts_per_folder": summary,
                },
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        browser.close()

    print(f"\n{'='*50}")
    print(f"Done. {len(summary)} folders, {total_parts} total parts")
    print(f"Output: {run_dir}")


if __name__ == "__main__":
    main()
