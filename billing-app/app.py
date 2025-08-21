# single-file web app: Challan + Invoice (Flask) with Google Sheets + PDF
# - Deployable on Railway (or any host)
# - Secrets via ENV: SPREADSHEET_ID, GOOGLE_SA_JSON, SESSION_SECRET
# - Optional ENV: SAVE_DIR (local folder to save server-side copies; e.g. C:\Invoice_challan)
# - Login reads ID/PASS from Google Sheet tab "PASS" (A2=id, B2=pass)
# - Challan: 2 copies per page, 5 rows, no GST/discount, logs to "Challan"
# - Invoice: GST @ env GST_TOTAL (default 5%), global SAC, logs to "Invoice"
# - Firms from "ID" tab, Suppliers from "Supplier" tab
#
# pip install: flask gspread google-auth reportlab gunicorn

import os, re, io, json, base64
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from functools import wraps

from flask import (
    Flask, render_template, request, redirect, url_for,
    session, send_file, flash
)

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib import colors
from reportlab.pdfbase import pdfmetrics
from reportlab.lib.utils import ImageReader

import gspread
from google.oauth2.service_account import Credentials as SA_Credentials
from jinja2 import DictLoader

# ==============================
# Config from ENV
# ==============================
SPREADSHEET_ID   = os.getenv("SPREADSHEET_ID")        # required
GOOGLE_SA_JSON   = os.getenv("GOOGLE_SA_JSON")        # service account JSON (single env var)
SESSION_SECRET   = os.getenv("SESSION_SECRET", "change-me")

# Optional: where to save a server-side copy (works if path exists & writable)
SAVE_DIR         = os.getenv("SAVE_DIR", "").strip()  # e.g. C:\Invoice_challan

# Sheet names
ID_TAB_NAME        = os.getenv("ID_TAB_NAME", "ID")
SUPPLIER_TAB_NAME  = os.getenv("SUPPLIER_TAB_NAME", "Supplier")
CHALLAN_TAB_NAME   = os.getenv("CHALLAN_TAB_NAME", "Challan")
INVOICE_TAB_NAME   = os.getenv("INVOICE_TAB_NAME", "Invoice")
PASS_TAB_NAME      = os.getenv("PASS_TAB_NAME", "PASS")

SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
GST_TOTAL   = float(os.getenv("GST_TOTAL", "5.0"))
CGST_RATE   = SGST_RATE = GST_TOTAL/2.0
SAC_DEFAULT = os.getenv("SAC_DEFAULT", "123456")

INV_MAX_ROWS = 10
CH_MAX_ROWS  = 5

IST = ZoneInfo("Asia/Kolkata")

app = Flask(__name__)
app.secret_key = SESSION_SECRET
app.permanent_session_lifetime = timedelta(days=30)  # "remember me"

# ==============================
# Google Sheets helpers
# ==============================
def _gc():
    if not GOOGLE_SA_JSON or not SPREADSHEET_ID:
        raise RuntimeError("Missing env vars: GOOGLE_SA_JSON and/or SPREADSHEET_ID.")
    info = json.loads(GOOGLE_SA_JSON)
    creds = SA_Credentials.from_service_account_info(info, scopes=SHEETS_SCOPES)
    return gspread.authorize(creds)

def _ws(sheet_name):
    gc = _gc()
    return gc.open_by_key(SPREADSHEET_ID).worksheet(sheet_name)

def load_firms():
    """Return dict: key -> profile dict (includes 'logo' from 'logolink')."""
    try:
        ws = _ws(ID_TAB_NAME)
        rows = ws.get_all_values()
        if not rows: return {}
        header = [h.strip().lower() for h in rows[0]]
        idx = {h:i for i,h in enumerate(header)}
        def val(r, name):
            i = idx.get(name.lower());  return (r[i].strip() if i is not None and i < len(r) else "")
        out = {}
        for r in rows[1:]:
            if not r or not any(r): continue
            firm = val(r, "firm")
            if not firm: continue
            out[firm.upper()] = {
                "title_name": firm.upper(),
                "company_name": firm,
                "addr": val(r,"address"),
                "mobile": val(r,"number"),
                "gst": val(r,"gst"),
                "logo": val(r,"logolink"),  # <-- logo URL/path/data-URI
                "bank_lines": [
                    f"Bank: {val(r,'bank') or '—'}",
                    f"A/C Name: {val(r,'account_name') or '—'}",
                    f"A/C No.: {val(r,'account_number') or '—'}",
                    f"IFSC: {val(r,'ifsc') or '—'}",
                    f"Branch: {val(r,'branch') or '—'}",
                ],
            }
        return out
    except Exception as e:
        print("Firms load error:", e);  return {}

def load_suppliers():
    try:
        ws = _ws(SUPPLIER_TAB_NAME)
        rows = ws.get_all_records()
        out = {}
        for r in rows:
            code = str(r.get("Supplier Code","")).strip()
            if not code: continue
            out[code] = {
                "name":    str(r.get("Supplier Name","")).strip(),
                "gstin":   str(r.get("Supplier GSTIN","")).strip(),
                "mobile":  str(r.get("Supplier Mobile","")).strip(),
                "address": str(r.get("Supplier Address","")).strip(),
            }
        return out
    except Exception as e:
        print("Suppliers load error:", e);  return {}

# ---------- Header normalizer for Challan rows ----------
CANON_KEYS = [
    "Firm","Supplier Code","Challan_Number","INVOICE_MTR","Description","Qty","MTR","Rate","Amount"
]
def _norm_key(s): return re.sub(r"[^a-z0-9]+","", (s or "").lower())

# map many variants to our canonical keys
KEY_SYNONYMS = {
    "Firm": ["firm","company","companyname"],
    "Supplier Code": ["suppliercode","partycode","supplier_code","supplier"],
    "Challan_Number": ["challanno","challan_no","challannumber","challan"],
    "INVOICE_MTR": ["invoice_mtr","invoicemtr","invoicemeter"],
    "Description": ["description","desc","productname","item","particulars"],
    "Qty": ["qty","quantity","qnt","meter","metre","meters","mtrs","mtr_qty"],
    "MTR": ["mtr","meter","metre","meters","qty"],  # fallback mirror
    "Rate": ["rate","price","per","perunit","per_mtr"],
    "Amount": ["amount","amt","total","subtotal","grandtotal"],
}

def load_challan_rows():
    """Return list of rows with canonical keys so the Invoice UI always sees them."""
    try:
        ws = _ws(CHALLAN_TAB_NAME)
        values = ws.get_all_values()
        if not values: return []
        raw_header = values[0]
        # build map from actual header to canonical key
        canon_map = {}
        for h in raw_header:
            keyn = _norm_key(h)
            for canon, alts in KEY_SYNONYMS.items():
                if keyn in { _norm_key(x) for x in ([canon] + alts) }:
                    canon_map[h] = canon
                    break
        rows = []
        for r in values[1:]:
            if not any(r): continue
            rec = {}
            for i, cell in enumerate(r):
                if i >= len(raw_header): break
                h = raw_header[i]
                canon = canon_map.get(h)
                if canon:
                    rec[canon] = cell
            # Ensure all canonical keys exist (maybe empty)
            for ck in CANON_KEYS:
                rec.setdefault(ck, "")
            rows.append(rec)
        return rows
    except Exception as e:
        print("Challan load error:", e);  return []

def get_next_invoice_number():
    try:
        ws = _ws(INVOICE_TAB_NAME)
        rows = ws.get_all_records()
        max_num = 0
        for r in rows:
            raw = str(r.get("Invoice_Number","")).strip()
            m = re.search(r"(\d+)$", raw)
            num = int(raw) if raw.isdigit() else (int(m.group(1)) if m else None)
            if isinstance(num, int) and num > max_num: max_num = num
        return str(max_num + 1 if max_num > 0 else 1)
    except Exception:
        return "1"

def get_next_challan_number():
    try:
        ws = _ws(CHALLAN_TAB_NAME)
        rows = ws.get_all_records()
        max_num = 0
        for r in rows:
            raw = str(r.get("Challan_Number","")).strip()
            m = re.search(r"(\d+)$", raw)
            num = int(raw) if raw.isdigit() else (int(m.group(1)) if m else None)
            if isinstance(num, int) and num > max_num: max_num = num
        return str(max_num + 1 if max_num > 0 else 1)
    except Exception:
        return "1"

def check_login_from_sheet(username, password):
    """PASS sheet: A2 = ID, B2 = PASS"""
    try:
        ws = _ws(PASS_TAB_NAME)
        uid  = ws.acell("A2").value or ""
        upwd = ws.acell("B2").value or ""
        return (username.strip() == (uid or "").strip()) and (password.strip() == (upwd or "").strip())
    except Exception as e:
        print("PASS sheet error:", e);  return False

# ---------- Ensure/append Challan row with supplier challan no ----------
REQ_CHALLAN_HEADER = [
    "Firm","Createed_Date","Invoice_Date","Challan_Number","Supplier Code",
    "Supplier_Name","Gst_No","Description","Qty","Amount","Taxable_Amount","Grand_Total",
    "INVOICE_MTR","supplier_challan_number"
]
def _ensure_challan_header(ws):
    vals = ws.get_all_values()
    header = vals[0] if vals else []
    if not header:
        ws.update("A1", [REQ_CHALLAN_HEADER])
        header = REQ_CHALLAN_HEADER[:]
    else:
        # add missing required cols at the end
        lower = [h.strip().lower() for h in header]
        changed = False
        for col in REQ_CHALLAN_HEADER:
            if col.lower() not in lower:
                header.append(col); changed = True
        if changed:
            ws.update("A1", [header])
    # index map
    return header, {h:i for i,h in enumerate(header)}

def append_row_to_invoice(row_values):
    try:
        _ws(INVOICE_TAB_NAME).append_row(row_values, value_input_option="USER_ENTERED")
    except Exception as e:
        print("Append Invoice failed:", e)

def append_row_to_challan(row_values, supplier_challan_number=None):
    """
    Append a challan row and, if provided, set the 'supplier_challan_number'
    column (auto-creating the column in header when missing).
    """
    try:
        ws = _ws(CHALLAN_TAB_NAME)
        header, idx = _ensure_challan_header(ws)
        # pad row to header length
        row = [""] * len(header)
        # map our standard 12 values first (in the order we generate them)
        base_cols = [
            "Firm","Createed_Date","Invoice_Date","Challan_Number","Supplier Code",
            "Supplier_Name","Gst_No","Description","Qty","Amount","Taxable_Amount","Grand_Total"
        ]
        for i, col in enumerate(base_cols):
            if col in idx and i < len(row_values):
                row[idx[col]] = row_values[i]
        # supplier challan no
        if supplier_challan_number and "supplier_challan_number" in [h.lower() for h in header]:
            row[idx["supplier_challan_number" if "supplier_challan_number" in idx else header[[h.lower() for h in header].index("supplier_challan_number")]]] = supplier_challan_number
        ws.append_row(row, value_input_option="USER_ENTERED")
    except Exception as e:
        print("Append Challan failed:", e)

# -------- write Invoice MTR back to Challan rows --------
def write_invoice_mtr_to_challan(company_name, supplier_code, items):
    """
    For each item (ch_no, desc, sac, mtr, rate, amt) written to invoice,
    find rows in Challan sheet that match:
        Firm == company_name
        Supplier Code == supplier_code
        Challan_Number == ch_no
        Description == desc
    and write mtr into column 'INVOICE_MTR'.
    """
    try:
        ws = _ws(CHALLAN_TAB_NAME)
        all_values = ws.get_all_values()
        if not all_values: return
        header = [h.strip() for h in all_values[0]]
        idx = {h:i for i,h in enumerate(header)}
        if "INVOICE_MTR" not in idx:
            header.append("INVOICE_MTR")
            ws.update('A1', [header])
            all_values[0] = header
            idx = {h:i for i,h in enumerate(header)}

        updates = []
        for row_num in range(2, len(all_values)+1):
            row = all_values[row_num-1]
            def get(col):
                i = idx.get(col)
                return (row[i].strip() if i is not None and i < len(row) else "")
            firm   = get("Firm")
            scode  = get("Supplier Code")
            chno   = get("Challan_Number")
            desc   = get("Description")
            for (ch, d, sac, q, r, a) in items:
                if (firm == company_name and scode == supplier_code and
                    str(ch).strip() == str(chno).strip() and (d or "").strip() == (desc or "").strip()):
                    col = idx["INVOICE_MTR"] + 1
                    updates.append((row_num, col, f"{float(q):.2f}"))
                    break

        if updates:
            data = []
            for r,c,val in updates:
                a1 = gspread.utils.rowcol_to_a1(r, c)
                data.append({'range': a1, 'values': [[val]]})
            ws.batch_update(data, value_input_option='USER_ENTERED')
    except Exception as e:
        print("write_invoice_mtr_to_challan error:", e)

# ==============================
# Auth helper
# ==============================
def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("user"):
            return redirect(url_for("login"))
        return fn(*args, **kwargs)
    return wrapper

# ==============================
# PDF helpers
# ==============================
def _wrap(text, max_width, font="Helvetica", size=9):
    text = (text or "").replace("\r"," ").replace("\n"," ").strip()
    if not text: return [""]
    words = text.split()
    lines, line = [], ""
    for w in words:
        test = (line + " " + w).strip()
        if pdfmetrics.stringWidth(test, font, size) <= max_width: line = test
        else:
            if line: lines.append(line)
            line = w
    if line: lines.append(line)
    return lines

def _unique_name(base="file.pdf"):
    stem, ext = os.path.splitext(base)
    i = 1; name = base
    while os.path.exists(name):
        name = f"{stem}_{i}{ext}"; i += 1
    return name

# Amount in words (Indian numbering)
def _num_words(n):
    units = ["","One","Two","Three","Four","Five","Six","Seven","Eight","Nine",
             "Ten","Eleven","Twelve","Thirteen","Fourteen","Fifteen","Sixteen",
             "Seventeen","Eighteen","Nineteen"]
    tens  = ["","Ten","Twenty","Thirty","Forty","Fifty","Sixty","Seventy","Eighty","Ninety"]
    def two(x):
        return units[x] if x < 20 else tens[x//10] + ((" " + units[x%10]) if x%10 else "")
    def three(x):
        h=x//100; r=x%100
        return (units[h]+" Hundred " + two(r)).strip() if h and r else (units[h]+" Hundred" if h else two(r))
    if n == 0: return "Zero"
    s=""; cr=n//10000000; n%=10000000
    la=n//100000;  n%=100000
    th=n//1000;    n%=1000
    if cr: s+=three(cr)+" Crore "
    if la: s+=three(la)+" Lakh "
    if th: s+=three(th)+" Thousand "
    if n:  s+=three(n)
    return " ".join(s.split())

def _rupees_words(v):  return f"{_num_words(int(round(v)))} Rupees Only"

def _image_reader_from_src(src):
    """Support http(s) URL, file path, or data URI (base64)."""
    if not src: return None
    src = src.strip()
    try:
        if src.startswith("data:image/"):
            header, b64 = src.split(",", 1)
            data = base64.b64decode(b64)
            return ImageReader(io.BytesIO(data))
        # http(s) URL or local path
        return ImageReader(src)
    except Exception as e:
        print("Logo load skipped:", e)
        return None

def _draw_logo(c, logo_src, x_right, y_top, max_w, max_h):
    """Draw logo whose top-right corner is at (x_right, y_top)."""
    try:
        img = _image_reader_from_src(logo_src)
        if not img: return
        iw, ih = img.getSize()
        scale = min(max_w/iw, max_h/ih)
        w = iw * scale
        h = ih * scale
        c.drawImage(img, x_right - w, y_top - h, w, h, preserveAspectRatio=True, mask='auto')
    except Exception as e:
        print("Logo draw skipped:", e)

# ===== Saving to local disk (optional) =====
def _save_copy(kind, firm_name, filename, data_bytes):
    """
    Save to SAVE_DIR\<kind>\<FIRM>\filename if SAVE_DIR is set & writable.
    kind: 'invoice' | 'challan'
    """
    try:
        if not SAVE_DIR: return
        base = os.path.abspath(SAVE_DIR)
        sub  = os.path.join(base, kind, (firm_name or "Unknown").strip().replace("/", "_"))
        os.makedirs(sub, exist_ok=True)
        path = os.path.join(sub, filename)
        with open(path, "wb") as f:
            f.write(data_bytes)
        print(f"Saved copy at: {path}")
    except Exception as e:
        print("Skip saving copy:", e)

# --------- Draw Challan (two copies) ---------
def draw_challan_pdf(buf, company, party, meta, items):
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4

    def one_copy(top_y):
        L, R = 24, width-24
        T = top_y

        # Header band
        c.setLineWidth(0.7)
        c.setFillColorRGB(0.93,0.93,0.93)
        c.rect(L+1, T-22, R-L-2, 22, fill=1, stroke=1)
        c.setFillColor(colors.black)
        c.setFont("Helvetica-Bold", 14)
        c.drawCentredString((L+R)/2, T-22+5, company["title_name"])

        # Company header block
        y = T-22-6
        c.setFont("Helvetica-Bold", 11)
        c.drawString(L+8, y-14, f"DELIVERY CHALLAN - {company['title_name']}")
        c.setFont("Helvetica", 9)
        inner_w = (R-L-2) - 16
        ay = y - 30
        for ln in _wrap(f"Address: {company['addr']}", inner_w - 160):
            c.drawString(L+8, ay, ln); ay -= 12
        c.drawString(L+8, ay, f"Mobile: {company['mobile']}   |   GST No.: {company['gst']}")
        _draw_logo(c, company.get("logo"), x_right=R-10, y_top=y-8, max_w=140, max_h=46)

        y = ay - 24   # spacing so text doesn't touch the partitions

        # Partitions
        part_h = 112
        left_w = (R - L - 2) / 2
        c.rect(L+1, y-part_h, left_w, part_h)            # left partition
        c.rect(L+1+left_w, y-part_h, left_w, part_h)     # right partition

        # Left content (party)
        c.setFont("Helvetica-Bold", 10)
        c.drawString(L+8, y-16, f"Party Details - {party.get('name') or '—'}")
        c.setFont("Helvetica", 9)
        vals = " | ".join([v for v in [party.get('gstin'), party.get('mobile')] if v])
        if vals: c.drawString(L+8, y-32, vals)
        label = "Address: "; lw = pdfmetrics.stringWidth(label, "Helvetica", 9)
        addr = _wrap((party.get('address') or ""), left_w-16 - lw)[:2]
        c.drawString(L+8, y-46, label + (addr[0] if addr else ""))
        if len(addr) > 1:
            c.drawString(L+8 + lw, y-58, addr[1])

        # Right content (meta)
        mx = L+10+left_w
        c.setFont("Helvetica-Bold", 10); c.drawString(mx, y-16, "Challan Details")
        c.setFont("Helvetica", 9)
        c.drawString(mx,     y-34, f"Challan No.: {meta['no']}")
        sup_no = meta.get("supplier_no") or meta.get("supplier_challan_no") or ""
        if sup_no:
            c.drawString(mx, y-50, f"Supplier Ch. No.: {sup_no}")
            c.drawString(mx, y-66, f"Date: {meta['date']}")
        else:
            c.drawString(mx, y-50, f"Date: {meta['date']}")

        # Items table
        ytbl = y-part_h-12
        table_w = (R-L-2)
        w_no, w_mtr, w_rate, w_amt = 40, 70, 90, 90
        w_desc = table_w - (w_no + w_mtr + w_rate + w_amt)
        widths  = [w_no, w_desc, w_mtr, w_rate, w_amt]
        headers = ["No.", "Product Name", "MTR", "Rate", "Amount"]
        total_h = 16 + CH_MAX_ROWS*18
        c.rect(L+1, ytbl-total_h, table_w, total_h)

        x = L+1; c.setFont("Helvetica-Bold", 9)
        for w,h in zip(widths,headers):
            c.rect(x, ytbl-16, w, 16); c.drawString(x+6, ytbl-12, h); x += w

        data_top_y = ytbl-16; data_h = CH_MAX_ROWS*18
        x = L+1
        for w in widths[:-1]:
            x += w; c.line(x, data_top_y, x, data_top_y - data_h)

        c.setFont("Helvetica", 9)
        for r in range(CH_MAX_ROWS):
            row_y = data_top_y - (r*18) - 12
            x = L+1
            if r < len(items): c.drawRightString(x+w_no-6, row_y, str(r+1))
            x += w_no
            if r < len(items): c.drawString(x+6, row_y, (items[r][0] or "")[:60])
            x += w_desc
            if r < len(items): c.drawRightString(x+w_mtr-6, row_y, f"{float(items[r][1]):.2f}")
            x += w_mtr
            if r < len(items): c.drawRightString(x+w_rate-6, row_y, f"{float(items[r][2]):.2f}")
            x += w_rate
            if r < len(items): c.drawRightString(x+w_amt-6, row_y, f"{float(items[r][3]):.2f}")

        # Grand Total
        sub_y_top = data_top_y - data_h
        c.setFont("Helvetica-Bold", 9)
        c.rect(L+1, sub_y_top-18, table_w - w_amt, 18)
        c.drawString(L+7, sub_y_top-12, "Grand Total (₹)")
        c.rect(L+1 + (table_w - w_amt), sub_y_top-18, w_amt, 18)
        total_val = sum(float(a) for *_, a in items)
        c.drawRightString(L+1+table_w-6, sub_y_top-12, f"{total_val:.2f}")

        # Signatures
        sig_top = sub_y_top - 26
        c.setFont("Helvetica", 9)
        c.drawString(L+10, sig_top-30, "Receiver's Signature")
        c.drawRightString(R-10, sig_top-30, "Authorised Signatory")

        # Outer border
        bottom_y = sig_top - 36
        c.rect(L, bottom_y, R-L, T - bottom_y)

    one_copy(height-24)
    one_copy((height/2)-8)
    c.save()

# --------- Draw Invoice (single copy) ---------
def draw_invoice_pdf(buf, company, supplier, inv_meta, items, discount):
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4
    L, R, T, B = 24, width-24, height-24, 42
    c.setLineWidth(0.7); c.rect(L, B, R-L, T-B)

    # Header
    band_h = 26
    c.setFillColorRGB(0.93,0.93,0.93)
    c.rect(L+1, T-band_h, R-L-2, band_h, fill=1, stroke=1)
    c.setFillColor(colors.black)
    c.setFont("Helvetica-Bold", 16)
    c.drawCentredString((L+R)/2, T-band_h+6, company["title_name"])

    # Company block
    y = T-band_h-8
    c.rect(L+1, y-54, R-L-2, 54)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(L+10, y-16, f"TAX INVOICE - {company['title_name']}")
    c.setFont("Helvetica", 9)
    inner_w = (R - L - 2) - 20
    ay = y - 30
    for ln in _wrap(f"Address: {company['addr']}", inner_w - 160):
        c.drawString(L+10, ay, ln); ay -= 12
    c.drawString(L+10, ay, f"Mobile: {company['mobile']}   |   GST No.: {company['gst']}")
    _draw_logo(c, company.get("logo"), x_right=R-10, y_top=y-8, max_w=140, max_h=46)
    y = ay - 24

    # Partitions
    part_h = 130
    left_w = (R - L - 2) / 2
    c.rect(L+1, y-part_h, left_w, part_h)
    c.rect(L+1+left_w, y-part_h, left_w, part_h)

    # Supplier details
    c.setFont("Helvetica-Bold", 10)
    c.drawString(L+8, y-18, "Supplier details")
    c.setFont("Helvetica", 9)
    sx = L+8; sy = y-34
    for ln in [
        f"Name: {supplier.get('name') or '—'}",
        *( [f"GSTIN: {supplier.get('gstin')}"] if supplier.get('gstin') else [] ),
        *( [f"Mobile: {supplier.get('mobile')}"] if supplier.get('mobile') else [] ),
    ]:
        c.drawString(sx, sy, ln); sy -= 12
    c.drawString(sx, sy, "Address:"); sy -= 12
    for ln in _wrap(supplier.get('address',''), left_w-16)[:8]:
        c.drawString(sx+12, sy, ln); sy -= 12

    # Invoice meta
    mx = L+10+left_w
    c.setFont("Helvetica-Bold", 10); c.drawString(mx, y-18, "Invoice Details")
    c.setFont("Helvetica", 9)
    c.drawString(mx,     y-36, f"Invoice No.: {inv_meta['no']}")
    c.drawString(mx,     y-52, f"Date: {inv_meta['date']}")

    # Items table
    ytbl = y-part_h-12
    table_w = (R-L-2)
    w_ch, w_desc, w_sac, w_mtr, w_rate = 65, 240, 70, 60, 60
    w_amt = table_w - (w_ch + w_desc + w_sac + w_mtr + w_rate)
    widths = [w_ch, w_desc, w_sac, w_mtr, w_rate, w_amt]
    headers = ["Ch. No", "Product Name", "SAC", "MTR", "Rate", "Amount"]

    total_h = 16 + INV_MAX_ROWS*18
    c.rect(L+1, ytbl-total_h, table_w, total_h)

    x = L+1; c.setFont("Helvetica-Bold", 9)
    for w,h in zip(widths,headers):
        c.rect(x, ytbl-16, w, 16); c.drawString(x+6, ytbl-12, h); x += w

    data_top_y = ytbl-16; data_h = INV_MAX_ROWS*18
    x = L+1
    for w in widths[:-1]:
        x += w; c.line(x, data_top_y, x, data_top_y - data_h)

    c.setFont("Helvetica", 9)
    for r in range(INV_MAX_ROWS):
        row_y = data_top_y - (r*18) - 12
        x = L+1
        if r < len(items): c.drawString(x+6, row_y, str(items[r][0] or ""))
        x += w_ch
        if r < len(items): c.drawString(x+6, row_y, (items[r][1] or "")[:50])
        x += w_desc
        if r < len(items): c.drawString(x+6, row_y, items[r][2] or SAC_DEFAULT)
        x += w_sac
        if r < len(items): c.drawRightString(x+w_mtr-6, row_y, f"{float(items[r][3]):.2f}")
        x += w_mtr
        if r < len(items): c.drawRightString(x+w_rate-6, row_y, f"{float(items[r][4]):.2f}")
        x += w_rate
        if r < len(items): c.drawRightString(x+w_amt-6, row_y, f"{float(items[r][5]):.2f}")

    # Totals
    sub_total = sum(float(i[5]) for i in items)
    discount = float(discount or 0)
    taxable = max(sub_total - discount, 0.0)
    cgst = taxable * (CGST_RATE/100.0)
    sgst = taxable * (SGST_RATE/100.0)
    gross = taxable + cgst + sgst
    rounded = round(gross, 0)
    round_off = rounded - gross

    sub_y_top = data_top_y - data_h
    c.setFont("Helvetica-Bold", 9)
    c.rect(L+1, sub_y_top-18, w_ch+w_desc+w_sac+w_mtr, 18)
    c.drawString(L+7, sub_y_top-12, "Sub Total")
    c.rect(L+1+w_ch+w_desc+w_sac+w_mtr, sub_y_top-18, w_rate, 18)
    c.rect(L+1+w_ch+w_desc+w_sac+w_mtr+w_rate, sub_y_top-18, w_amt, 18)
    c.drawRightString(L+1+table_w-6, sub_y_top-12, f"{sub_total:.2f}")

    # Bottom area (Amounts in Words + Bank details + Summary)
    ybot = sub_y_top - 26
    bottom_h = 200
    c.rect(L+1, ybot-bottom_h, table_w, bottom_h)

    left_w2 = table_w/2
    words_h = 110
    bank_h  = bottom_h - words_h

    # Amounts in Words
    c.rect(L+1, ybot-words_h, left_w2, words_h)
    c.setFont("Helvetica-Bold", 10); c.drawString(L+8, ybot-16, "Amounts in Words:")
    c.setFont("Helvetica", 9)
    xw = L+8; yline = ybot-32
    for label, text in [("Taxable", _rupees_words(taxable)),
                        ("GST",     _rupees_words(cgst + sgst)),
                        ("Total",   _rupees_words(rounded))]:
        for wln in _wrap(f"{label}: {text}", left_w2-16):
            c.drawString(xw, yline, wln); yline -= 12

    # Bank box
    c.rect(L+1, ybot-bottom_h, left_w2, bank_h)
    c.setFont("Helvetica-Bold", 10); c.drawString(L+8, ybot-words_h-16, "Bank Details:")
    c.setFont("Helvetica", 9)
    by = ybot-words_h-32
    for line in company.get("bank_lines", []):
        c.drawString(L+8, by, line); by -= 12

    # Summary (right)
    c.rect(L+1+left_w2, ybot-bottom_h, left_w2, bottom_h)
    rx = L+10+left_w2; rv = L+1+left_w2 + left_w2 - 8
    c.setFont("Helvetica-Bold", 10); c.drawString(rx, ybot-16, "Summary")
    c.setFont("Helvetica-Bold", 9)
    yy = ybot-32
    def pr(lbl,val):
        nonlocal yy
        c.drawString(rx, yy, f"{lbl}:"); c.drawRightString(rv, yy, val); yy -= 14
    pr("Taxable Amount", f"{taxable:.2f}")
    pr("Discount", f"{discount:.2f}")
    pr(f"CGST ({CGST_RATE:.1f}%)", f"{cgst:.2f}")
    pr(f"SGST ({SGST_RATE:.1f}%)", f"{sgst:.2f}")
    pr("Round Off", f"{round_off:+.2f}")
    c.setFont("Helvetica-Bold", 10)
    c.drawString(rx, yy-2, "Grand Total (₹)")
    c.drawRightString(rv, yy-2, f"{int(round(rounded))}")

    # Signatures at page bottom
    c.setFont("Helvetica", 9)
    c.drawString(L+10, B+22, "Customer Signature")
    c.drawRightString(R-10, B+22, f"For {company['company_name']}")
    c.drawRightString(R-10, B+8, "Authorised Signatory")

    c.save()

# ==============================
# Templates (in-memory)
# ==============================
TEMPLATES = {
"base.html": r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{{ title or "Billing App" }}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root { --blue:#2563eb; --grey:#6b7280; --b:#e5e7eb; --text:#111827; }
    body { font-family: system-ui, -apple-system, "Segoe UI", Roboto, Arial, sans-serif; margin: 18px; color: var(--text); }
    header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 16px; }
    .btn { display:inline-block; padding:8px 12px; background:var(--blue); color:white; text-decoration:none; border-radius:6px; border:0; cursor:pointer; }
    .btn.secondary { background:var(--grey); }
    .btn.small { padding:6px 10px; font-size: 14px; }
    .card { border:1px solid var(--b); border-radius:10px; padding:16px; margin:12px 0; }
    input, select, textarea { padding:8px; border:1px solid #cbd5e1; border-radius:6px; width: 100%; box-sizing: border-box; }
    table { border-collapse: collapse; width:100%; }
    th, td { border:1px solid #e5e7eb; padding:8px; text-align:left; }
    th.right, td.right { text-align:right; }
    .row { display:flex; gap:12px; flex-wrap:wrap; }
    .grow { flex:1 1 250px; }
    .right { text-align:right; }
    .msg { color:#dc2626; margin-bottom:8px; }
    label { font-size: 13px; color:#374151; }
  </style>
</head>
<body>
  <header>
    <div><strong>Billing App</strong></div>
    <nav>
      {% if session.get('user') %}
        <a class="btn secondary" href="{{ url_for('dashboard') }}">Dashboard</a>
        <a class="btn secondary" href="{{ url_for('challan') }}">Challan</a>
        <a class="btn secondary" href="{{ url_for('invoice') }}">Invoice</a>
        <a class="btn" href="{{ url_for('logout') }}">Logout</a>
      {% endif %}
    </nav>
  </header>

  {% with messages = get_flashed_messages(category_filter=["error"]) %}
    {% if messages %}
      <div class="msg">{{ messages[0] }}</div>
    {% endif %}
  {% endwith %}

  {% block content %}{% endblock %}
</body>
</html>
""",
"login.html": r"""
{% extends "base.html" %}
{% block content %}
<h2>Login</h2>
<form method="post" class="card" style="max-width:420px;">
  <div class="row">
    <div class="grow">
      <label>ID</label><br>
      <input name="username" required>
    </div>
  </div>
  <div class="row">
    <div class="grow">
      <label>Password</label><br>
      <input type="password" name="password" required>
    </div>
  </div>
  <div class="row">
    <label><input type="checkbox" name="remember"> Remember me</label>
  </div>
  <button class="btn" type="submit">Sign in</button>
</form>
<p style="max-width:520px;">Ask admin for password.</p>
{% endblock %}
""",
"dashboard.html": r"""
{% extends "base.html" %}
{% block content %}
<h2>Dashboard</h2>
<div class="card">
  <p><b>Firms :</b></p>
  {% if firms %}
    <ul>
      {% for key, firm in firms.items() %}
        <li>{{ firm.company_name }} — GST: {{ firm.gst }}</li>
      {% endfor %}
    </ul>
  {% else %}
    <p>No firms loaded.</p>
  {% endif %}
</div>

<div class="row">
  <a class="btn" href="{{ url_for('challan') }}">Create Challan</a>
  <a class="btn" href="{{ url_for('invoice') }}">Create Invoice</a>
</div>
{% endblock %}
""",
"challan.html": r"""
{% extends "base.html" %}
{% block content %}
<h2>Create Challan</h2>

<form id="challanForm" method="post" class="card">
  <div class="row">
    <div class="grow">
      <label>Firm</label><br>
      <select name="firm_key" id="ch_firm" required>
        {% for k, v in firms.items() %}
          <option value="{{k}}" {{ 'selected' if k == firm_default else '' }}>{{ v.company_name }}</option>
        {% endfor %}
      </select>
    </div>
    <div>
      <label>Challan Date</label><br>
      <input name="challan_date" value="{{ today }}" required>
    </div>
    <div>
      <label>Challan No.</label><br>
      <input name="challan_no" value="{{ next_no }}" required>
    </div>
    <div>
      <label>Supplier Challan No.</label><br>
      <input name="supplier_challan_number" placeholder="Supplier ch. no.">
    </div>
  </div>

  <div class="row">
    <div class="grow">
      <label>Party Code</label><br>
      <select name="party_code" id="ch_party_code">
        <option value="">-- optional --</option>
        {% for code, s in suppliers.items() %}
          <option value="{{code}}">{{ code }} - {{ s.name }}</option>
        {% endfor %}
      </select>
    </div>
  </div>

  <div class="row">
    <div class="grow"><label>Party Name</label><br><input name="party_name" id="ch_name"></div>
    <div class="grow"><label>GSTIN</label><br><input name="party_gstin" id="ch_gstin"></div>
    <div class="grow"><label>Mobile</label><br><input name="party_mobile" id="ch_mobile"></div>
  </div>
  <div class="row">
    <div class="grow"><label>Address</label><br><textarea name="party_address" id="ch_address" rows="2"></textarea></div>
  </div>

  <h3>Items (max {{ CH_MAX_ROWS }})</h3>
  <table id="items">
    <thead><tr><th>Description</th><th class="right">MTR</th><th class="right">Rate</th><th></th></tr></thead>
    <tbody></tbody>
  </table>
  <p><button type="button" class="btn small" onclick="addRow()">Add Row</button></p>

  <button id="ch_submit" class="btn" type="submit">Generate PDF & Log</button>
</form>

<script>
const SUPPLIERS = {{ suppliers|tojson }};

function fillParty(){
  const code = document.getElementById('ch_party_code').value;
  const s = SUPPLIERS[code]; if(!s) return;
  document.getElementById('ch_name').value = s.name || '';
  document.getElementById('ch_gstin').value = s.gstin || '';
  document.getElementById('ch_mobile').value = s.mobile || '';
  document.getElementById('ch_address').value = s.address || '';
}
document.getElementById('ch_party_code').addEventListener('change', fillParty);
document.getElementById('ch_party_code').addEventListener('blur', fillParty);

function addRow(){
  const tr = document.createElement('tr');
  tr.innerHTML = `
    <td><input name="desc[]" required></td>
    <td class="right"><input name="qty[]" type="number" step="0.01" min="0.01" required></td>
    <td class="right"><input name="rate[]" type="number" step="0.01" min="0" required></td>
    <td><button class="btn secondary small" type="button" onclick="this.closest('tr').remove()">Delete</button></td>`;
  document.querySelector('#items tbody').appendChild(tr);
}
addRow();

// --- Submit via fetch -> download -> reset form ---
document.getElementById('challanForm').addEventListener('submit', async (e)=>{
  e.preventDefault();
  const btn = document.getElementById('ch_submit');
  btn.disabled = true;
  try{
    const fd = new FormData(e.target);
    const res = await fetch("{{ url_for('challan') }}", { method: "POST", body: fd, credentials: "same-origin" });
    if(!res.ok) throw new Error("Failed to generate PDF");
    const blob = await res.blob();
    const dispo = res.headers.get('Content-Disposition') || '';
    const m = /filename\*=UTF-8''([^;]+)|filename="?([^"]+)"?/i.exec(dispo);
    const fname = decodeURIComponent((m && (m[1]||m[2])) || 'challan.pdf');
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a'); a.href = url; a.download = fname; document.body.appendChild(a); a.click();
    setTimeout(()=>{ URL.revokeObjectURL(url); a.remove(); }, 500);
    // reset form (clear party and items)
    e.target.reset();
    document.querySelector('#items tbody').innerHTML = '';
    addRow();
  }catch(err){
    alert(err.message || err);
  }finally{
    btn.disabled = false;
  }
});
</script>
{% endblock %}
""",
"invoice.html": r"""
{% extends "base.html" %}
{% block content %}
<h2>Create Invoice</h2>

<form id="invoiceForm" method="post" class="card">
  <div class="row">
    <div class="grow">
      <label>Firm</label><br>
      <select name="firm_key" id="inv_firm" required>
        {% for k, v in firms.items() %}
          <option value="{{k}}" {{ 'selected' if k == firm_default else '' }}>{{ v.company_name }}</option>
        {% endfor %}
      </select>
    </div>
    <div>
      <label>Invoice Date</label><br>
      <input name="invoice_date" value="{{ today }}" required>
    </div>
    <div>
      <label>Invoice No.</label><br>
      <input name="invoice_no" value="{{ next_no }}" required>
    </div>
  </div>

  <div class="row">
    <div class="grow">
      <label>Supplier Code</label><br>
      <select name="supplier_code" id="inv_supplier_code">
        <option value="">-- optional --</option>
        {% for code, s in suppliers.items() %}
          <option value="{{code}}">{{ code }} - {{ s.name }}</option>
        {% endfor %}
      </select>
    </div>
    <div>
      <label>Discount (₹)</label><br>
      <input name="discount" type="number" step="0.01" value="0">
    </div>
    <div>
      <label>SAC (global)</label><br>
      <input name="sac_global" value="{{ sac_default }}">
    </div>
  </div>

  <div class="row">
    <div class="grow"><label>Supplier Name</label><br><input name="supplier_name" id="inv_name"></div>
    <div class="grow"><label>GSTIN</label><br><input name="supplier_gstin" id="inv_gstin"></div>
    <div class="grow"><label>Mobile</label><br><input name="supplier_mobile" id="inv_mobile"></div>
  </div>
  <div class="row">
    <div class="grow"><label>Address</label><br><textarea name="supplier_address" id="inv_address" rows="2"></textarea></div>
  </div>

  <!-- Import from Challan -->
  <div class="card" style="margin-top:8px;">
    <div class="row">
      <div class="grow">
        <label>Import items from Challan</label><br>
        <select id="inv_import_challan">
          <option value="">-- select challan --</option>
        </select>
      </div>
      <div style="align-self:flex-end">
        <button type="button" class="btn small" onclick="addFromChallan()">Add From Challan</button>
      </div>
    </div>
    <small>
      List is filtered by selected Firm + Supplier Code. A challan is shown only if it has at least one row with empty <b>INVOICE_MTR</b>. When adding, only the not-yet-invoiced rows are inserted.
    </small>
  </div>

  <h3>Items (max {{ INV_MAX_ROWS }})</h3>
  <table id="items">
    <thead>
      <tr>
        <th>Ch. No</th><th>Description</th><th class="right">MTR</th><th class="right">Rate</th><th></th>
      </tr>
    </thead>
    <tbody></tbody>
  </table>
  <p><button type="button" class="btn small" onclick="addRow()">Add Row</button></p>

  <button id="inv_submit" class="btn" type="submit">Generate PDF & Log</button>
</form>

<script>
const SUPPLIERS    = {{ suppliers|tojson }};
const CHALLAN_ROWS = {{ challans|tojson }};
const INV_MAX_ROWS = {{ INV_MAX_ROWS|int }};

function addRow(prefill){
  const tr = document.createElement('tr');
  tr.innerHTML = `
    <td><input name="ch[]"  value="${prefill?.ch ?? ''}"></td>
    <td><input name="desc[]" value="${prefill?.desc ?? ''}" required></td>
    <td class="right"><input name="qty[]"  type="number" step="0.01" min="0.01" value="${prefill?.qty ?? ''}" required></td>
    <td class="right"><input name="rate[]" type="number" step="0.01" min="0"     value="${prefill?.rate ?? ''}" required></td>
    <td><button class="btn secondary small" type="button" onclick="this.closest('tr').remove()">Delete</button></td>`;
  document.querySelector('#items tbody').appendChild(tr);
  return tr;
}
addRow(); // one empty row

function fillSupplier(){
  const code = document.getElementById('inv_supplier_code').value;
  const s = SUPPLIERS[code];
  if(s){
    document.getElementById('inv_name').value    = s.name   || '';
    document.getElementById('inv_gstin').value   = s.gstin  || '';
    document.getElementById('inv_mobile').value  = s.mobile || '';
    document.getElementById('inv_address').value = s.address|| '';
  }
  refreshChallanOptions();
}
document.getElementById('inv_supplier_code').addEventListener('change', fillSupplier);
document.getElementById('inv_supplier_code').addEventListener('blur', fillSupplier);
document.getElementById('inv_firm').addEventListener('change', refreshChallanOptions);

function refreshChallanOptions(){
  const firm  = (document.getElementById('inv_firm').value || '').toUpperCase();
  const scode = document.getElementById('inv_supplier_code').value || '';
  const sel   = document.getElementById('inv_import_challan');
  sel.innerHTML = '<option value="">-- select challan --</option>';
  if(!firm || !scode) return;

  // group rows by challan number for this firm+supplier
  const grouped = {};
  CHALLAN_ROWS.forEach(r=>{
    const rf = String(r['Firm']||'').toUpperCase();
    const rc = String(r['Supplier Code']||'');
    if(rf!==firm || rc!==scode) return;
    const ch = String(r['Challan_Number']||'').trim();
    if(!ch) return;
    if(!grouped[ch]) grouped[ch] = [];
    grouped[ch].push(r);
  });

  // Show challan if it has at least one UNINVOICED row (INVOICE_MTR is blank)
  Object.keys(grouped).sort().forEach(ch=>{
    const rows = grouped[ch];
    const hasUninvoiced = rows.some(x => String(x['INVOICE_MTR']||'').trim() === '');
    if(!hasUninvoiced) return;
    const firstDesc = (rows.find(x => String(x['INVOICE_MTR']||'').trim() === '')?.['Description']) || rows[0]['Description'] || '';
    const short = String(firstDesc).slice(0,28);
    const opt = document.createElement('option');
    opt.value = ch; opt.textContent = short ? `${ch} (${short})` : ch;
    sel.appendChild(opt);
  });
}

function safeNum(v){ const n = Number(v); return isNaN(n)?0:n; }

function addFromChallan(){
  const firm  = (document.getElementById('inv_firm').value || '').toUpperCase();
  const scode = document.getElementById('inv_supplier_code').value || '';
  const chSel = document.getElementById('inv_import_challan').value || '';
  if(!firm || !scode || !chSel) return;

  const rows = CHALLAN_ROWS.filter(r =>
    String(r['Firm']||'').toUpperCase() === firm &&
    String(r['Supplier Code']||'') === scode &&
    String(r['Challan_Number']||'').trim() === chSel &&
    String(r['INVOICE_MTR']||'').trim() === '' // ONLY not yet invoiced
  );

  const tbody = document.querySelector('#items tbody');
  let current = tbody.querySelectorAll('tr').length;
  for(const r of rows){
    if(current >= INV_MAX_ROWS) break;
    const desc = String(r['Description']||'');
    const qtyRaw = r['Qty'] !== undefined && r['Qty'] !== "" ? r['Qty'] : (r['MTR'] ?? '');
    const qty = String(qtyRaw ?? '');
    let rate = '';
    if(r['Rate'] !== undefined && r['Rate'] !== null && r['Rate'] !== ''){
      rate = String(r['Rate']);
    }else{
      const amt = safeNum(r['Amount']);
      const qn  = safeNum(qtyRaw);
      if(qn > 0) rate = (amt / qn).toFixed(2);
    }
    addRow({ ch: chSel, desc: desc, qty: qty, rate: rate });
    current++;
  }
}

window.addEventListener('DOMContentLoaded', ()=>{ refreshChallanOptions(); });

// --- Submit via fetch -> download -> reset form ---
document.getElementById('invoiceForm').addEventListener('submit', async (e)=>{
  e.preventDefault();
  const btn = document.getElementById('inv_submit');
  btn.disabled = true;
  try{
    const fd = new FormData(e.target);
    const res = await fetch("{{ url_for('invoice') }}", { method: "POST", body: fd, credentials: "same-origin" });
    if(!res.ok) throw new Error("Failed to generate PDF");
    const blob = await res.blob();
    const dispo = res.headers.get('Content-Disposition') || '';
    const m = /filename\*=UTF-8''([^;]+)|filename="?([^"]+)"?/i.exec(dispo);
    const fname = decodeURIComponent((m && (m[1]||m[2])) || 'invoice.pdf');
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a'); a.href = url; a.download = fname; document.body.appendChild(a); a.click();
    setTimeout(()=>{ URL.revokeObjectURL(url); a.remove(); }, 500);
    // reset form
    e.target.reset();
    document.querySelector('#items tbody').innerHTML = '';
    addRow();
    refreshChallanOptions();
  }catch(err){
    alert(err.message || err);
  }finally{
    btn.disabled = false;
  }
});
</script>
{% endblock %}
"""
}

# mount in-memory templates
app.jinja_loader = DictLoader(TEMPLATES)

# ==============================
# Routes
# ==============================
@app.route("/healthz")
def healthz():
    return "ok", 200

@app.route("/", methods=["GET"])
def root():
    return redirect(url_for("dashboard") if session.get("user") else url_for("login"))

@app.route("/login", methods=["GET","POST"])
def login():
    if request.method == "POST":
        user = request.form.get("username","").strip()
        pwd  = request.form.get("password","").strip()
        remember = bool(request.form.get("remember"))
        if check_login_from_sheet(user, pwd):
            session["user"] = user;  session.permanent = remember
            return redirect(url_for("dashboard"))
        flash("Invalid ID or password.", "error")
    return render_template("login.html", PASS_TAB_NAME=PASS_TAB_NAME)

@app.route("/logout")
def logout():
    session.clear();  return redirect(url_for("login"))

@app.route("/dashboard")
@login_required
def dashboard():
    firms = load_firms()
    return render_template("dashboard.html",
                           firms=firms,
                           ID_TAB_NAME=ID_TAB_NAME)

# ---------- Challan ----------
@app.route("/challan", methods=["GET","POST"])
@login_required
def challan():
    firms     = load_firms()
    suppliers = load_suppliers()
    firm_keys = list(firms.keys())
    if request.method == "GET":
        return render_template("challan.html",
                               firms=firms, suppliers=suppliers,
                               next_no=get_next_challan_number(),
                               today=datetime.now(IST).strftime("%d/%m/%Y"),
                               CH_MAX_ROWS=CH_MAX_ROWS,
                               firm_default=(firm_keys[0] if firm_keys else ""))
    # POST: build items and PDF, append rows
    chosen_firm_key = request.form.get("firm_key")
    company = firms.get(chosen_firm_key, next(iter(firms.values()))) if firms else {
        "title_name":"", "company_name":"", "addr":"", "mobile":"", "gst":"", "bank_lines":[], "logo":""
    }
    party_code = request.form.get("party_code","")
    party_src = suppliers.get(party_code, {"name":"", "gstin":"", "mobile":"", "address":""})
    party = {
        "name":    request.form.get("party_name",  party_src.get("name","")),
        "gstin":   request.form.get("party_gstin", party_src.get("gstin","")),
        "mobile":  request.form.get("party_mobile",party_src.get("mobile","")),
        "address": request.form.get("party_address",party_src.get("address","")),
    }

    ch_no  = request.form.get("challan_no") or "1"
    ch_dt  = request.form.get("challan_date") or datetime.now(IST).strftime("%d/%m/%Y")
    supplier_challan_number = request.form.get("supplier_challan_number","").strip()

    descs = request.form.getlist("desc[]")
    qtys  = request.form.getlist("qty[]")
    rates = request.form.getlist("rate[]")

    items = []
    for d,q,r in zip(descs, qtys, rates):
        if not d.strip(): continue
        try:
            qf = float(q); rf = float(r)
            if qf <= 0 or rf < 0: continue
        except: continue
        items.append([d.strip(), qf, rf, qf*rf])

    if not items:
        flash("Add at least one valid item.", "error")
        return redirect(url_for("challan"))

    # Create PDF in memory
    buf = io.BytesIO()
    draw_challan_pdf(
        buf,
        company=company,
        party=party,
        meta={"no": ch_no, "date": ch_dt, "supplier_no": supplier_challan_number},
        items=items[:CH_MAX_ROWS]
    )
    data = buf.getvalue()

    # Log rows to "Challan"
    created = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    for d, q, r, a in items:
        row = [
            company["company_name"],  # Firm
            created,                  # Createed_Date
            ch_dt,                    # Invoice_Date (kept name)
            ch_no,                    # Challan_Number
            party_code,               # Supplier Code
            party["name"],            # Supplier_Name
            party["gstin"],           # Gst_No
            d,                        # Description
            f"{q:.2f}",               # Qty
            f"{a:.2f}",               # Amount
            f"{a:.2f}",               # Taxable_Amount
            f"{a:.2f}",               # Grand_Total
        ]
        append_row_to_challan(row, supplier_challan_number=supplier_challan_number)

    # Optional server copy
    timestamp = datetime.now(IST).strftime("%Y%m%d-%H%M%S")
    dl_name = f"Challan_{ch_no}_{(party['name'] or 'Party').strip().replace(' ','_')}_{timestamp}.pdf"
    _save_copy("challan", company["company_name"], dl_name, data)

    return send_file(io.BytesIO(data), as_attachment=True, download_name=_unique_name(dl_name), mimetype="application/pdf")

# ---------- Invoice ----------
@app.route("/invoice", methods=["GET","POST"])
@login_required
def invoice():
    firms     = load_firms()
    suppliers = load_suppliers()
    challans  = load_challan_rows()   # for import dropdown
    firm_keys = list(firms.keys())
    if request.method == "GET":
        return render_template("invoice.html",
                               firms=firms, suppliers=suppliers, challans=challans,
                               next_no=get_next_invoice_number(),
                               today=datetime.now(IST).strftime("%d/%m/%Y"),
                               sac_default=SAC_DEFAULT,
                               gst_total=GST_TOTAL,
                               INV_MAX_ROWS=INV_MAX_ROWS,
                               firm_default=(firm_keys[0] if firm_keys else ""))
    # POST
    chosen_firm_key = request.form.get("firm_key")
    company = firms.get(chosen_firm_key, next(iter(firms.values()))) if firms else {
        "title_name":"", "company_name":"", "addr":"", "mobile":"", "gst":"", "bank_lines":[], "logo":""
    }
    sup_code = request.form.get("supplier_code","")
    sup_src = suppliers.get(sup_code, {"name":"", "gstin":"", "mobile":"", "address":""})
    sup = {
        "name":    request.form.get("supplier_name",  sup_src.get("name","")),
        "gstin":   request.form.get("supplier_gstin", sup_src.get("gstin","")),
        "mobile":  request.form.get("supplier_mobile",sup_src.get("mobile","")),
        "address": request.form.get("supplier_address",sup_src.get("address","")),
    }

    inv_no = request.form.get("invoice_no") or "XXX"
    inv_dt = request.form.get("invoice_date") or datetime.now(IST).strftime("%d/%m/%Y")
    discount = float(request.form.get("discount","0") or 0)
    sac_global = request.form.get("sac_global", SAC_DEFAULT).strip() or SAC_DEFAULT

    chnos = request.form.getlist("ch[]")
    descs = request.form.getlist("desc[]")
    qtys  = request.form.getlist("qty[]")
    rates = request.form.getlist("rate[]")

    items = []
    for ch, d, q, r in zip(chnos, descs, qtys, rates):
        if not d.strip(): continue
        try:
            qf=float(q); rf=float(r)
            if qf <= 0 or rf < 0: continue
        except: continue
        a = qf*rf
        items.append([ch.strip(), d.strip(), sac_global, qf, rf, a])

    if not items:
        flash("Add at least one valid item.", "error")
        return redirect(url_for("invoice"))

    buf = io.BytesIO()
    draw_invoice_pdf(buf, company, sup, {"no":inv_no, "date":inv_dt}, items[:INV_MAX_ROWS], discount)
    data = buf.getvalue()

    # Log row-per-item to "Invoice"
    created = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    sub_total = sum(i[5] for i in items)
    gross_all = max(sub_total - discount, 0.0)
    cgst_all  = gross_all * (CGST_RATE/100.0)
    sgst_all  = gross_all * (SGST_RATE/100.0)
    rounded_total   = round(gross_all + cgst_all + sgst_all, 0)
    round_off_total = rounded_total - (gross_all + cgst_all + sgst_all)

    for i, (ch, d, sac, q, r, a) in enumerate(items):
        share = (a / sub_total) if sub_total > 0 else 0.0
        row_discount = discount * share
        row_taxable  = max(a - row_discount, 0.0)
        row_cgst     = row_taxable * (CGST_RATE/100.0)
        row_sgst     = row_taxable * (SGST_RATE/100.0)
        row_round    = round_off_total if i == len(items)-1 else 0.0
        row_gross    = row_taxable + row_cgst + row_sgst + row_round

        append_row_to_invoice([
            company["company_name"],          # Firm
            created,                          # Createed_Date
            inv_dt,                           # Invoice_Date
            inv_no,                           # Invoice_Number
            sup_code,                         # Supplier Code
            sup["name"],                      # Supplier_Name
            sup["gstin"],                     # Gst_No
            str(ch or ""),                    # Challan_Number
            d,                                # Description
            f"{q:.2f}",                       # Qty
            f"{a:.2f}",                       # Amount
            f"{row_taxable:.2f}",             # Taxable_Amount
            f"{row_discount:.2f}",            # Discount
            f"{GST_TOTAL:.0f}%",              # Gst_Percentage
            f"{row_cgst:.2f}",                # CGST
            f"{row_sgst:.2f}",                # SGST
            f"{row_round:.2f}",               # Round_Off
            int(round(row_gross)),            # Grand_Total
        ])

    # write back MTR to Challan rows (INVOICE_MTR)
    write_invoice_mtr_to_challan(company["company_name"], sup_code, items)

    # Optional server copy
    timestamp = datetime.now(IST).strftime("%Y%m%d-%H%M%S")
    dl_name = f"Invoice_{inv_no}_{(sup['name'] or 'Supplier').strip().replace(' ','_')}_{timestamp}.pdf"
    _save_copy("invoice", company["company_name"], dl_name, data)

    return send_file(io.BytesIO(data), as_attachment=True, download_name=_unique_name(dl_name), mimetype="application/pdf")

# ==============================
# Main
# ==============================
if __name__ == "__main__":
  app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")), debug=True)
