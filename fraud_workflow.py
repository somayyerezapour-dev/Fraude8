# -*- coding: utf-8 -*-
"""
سامانه ساخت «پایگاه داده ترخیص‌های مظنون» از روی پایگاه داده ترخیص کالا
(پیاده‌سازی دیاگرام سه‌ماژوله‌ی ارسالی)

این نرم‌افزار یک ابزار ETL/ساخت پایگاه‌داده است، نه یک رابط بررسی تعاملی:
هر چهار ورودی از پیش در فایل‌های اکسل (و فایل‌های تصویر) آماده شده‌اند و برنامه
فقط آن‌ها را می‌خواند، باهم ترکیب می‌کند و «پایگاه داده ترخیص‌های مظنون» را
می‌سازد. تصمیم تأیید/رد کارشناسان جای دیگری گرفته می‌شود؛ اکسل‌های ورودی ۳ و ۴
فقط وضعیت (تأیید/عدم تأیید) و متن کامنتی را که کارشناس مجازی و کارشناس بازبینی
برای هر اظهارنامه نوشته‌اند، به این نرم‌افزار منتقل می‌کنند.

۴ ورودی (مطابق پیام کاربر):

  ورودی ۱ - یک یا چند اکسل «ارزش قلم کالا» (بخشی از پایگاه داده ترخیص کالا):
      شامل هم «ارزش قلم کالای یک اظهارنامه» و هم «سوابق ارزش قلم کالا»
      (سوابق = ردیف‌های تاریخی بیشتر با همان ساختار). برنامه از روی مجموع این
      ردیف‌ها، سابقه‌ی ارزش هر کد تعرفه را می‌سازد و با AI آماری مقایسه می‌کند.

  ورودی ۲ - یک یا چند اکسل «طرفین اظهارنامه» + فایل‌های تصویر اسناد ضمیمه:
      مشخصات صاحب کالا، مشخصات اظهارکننده، اطلاعات اظهارنامه، و نام فایل
      تصویر سند ضمیمه (که باید در پوشه‌ی تصاویر انتخابی موجود باشد).

  ورودی ۳ - یک اکسل «بررسی کارشناس مجازی»:
      مشخصات کارشناس مجازی + وضعیت تأیید/عدم تأیید + متن کامل کامنت، به ازای
      هر اظهارنامه.

  ورودی ۴ - یک اکسل «بررسی کارشناس بازبینی»:
      مشخصات کارشناس بازبینی + وضعیت تأیید/عدم تأیید + متن کامل کامنت، به ازای
      هر اظهارنامه.

خروجی: پایگاه‌داده SQLite «ترخیص‌های مظنون» شامل:
  - جدول اظهارنامه‌های مظنون (شماره کوتاژ، کد تعرفه، مقدار/درصد تفاوت و غیره)
  - پروفایل افراد مظنون (صاحب کالا و اظهارکننده) با مقدار تخلف، مشخصات و
    وضعیت/کامنت هر دو کارشناس، تا جایی که در اکسل‌های ۳ و ۴ موجود باشد.
  و یک فایل اکسل گزارش قابل‌خواندن برای انسان.

Python 3.8.10 (32-bit)  |  فقط pandas/numpy/openpyxl؛ ساخت exe با PyInstaller

اجرا:
  رابط گرافیکی :  python fraud_workflow.py
  خط فرمان     :
    python fraud_workflow.py build --db out.db \
        --values v1.xlsx v2.xlsx \
        --parties p1.xlsx --images-dir ./docs_images \
        --virtual virtual.xlsx --inspection inspection.xlsx \
        --threshold 10
    python fraud_workflow.py export out.db report.xlsx
"""
import argparse
import datetime
import json
import os
import sqlite3
import sys

import numpy as np
import pandas as pd

APP_TITLE = "ساخت پایگاه داده ترخیص‌های مظنون"
DEFAULT_THRESHOLD_PCT = 10.0
DEFAULT_Z_MIN = 2.5

_DIGITS = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")


def norm_text(s):
    s = "" if s is None or (isinstance(s, float) and np.isnan(s)) else str(s)
    s = s.replace("ي", "ی").replace("ك", "ک").replace("\u200c", " ").replace("\u200f", "")
    return " ".join(s.split())


def to_num(s):
    if pd.api.types.is_numeric_dtype(s):
        return s.astype(float)
    t = s.astype(str).str.translate(_DIGITS).str.replace(",", "", regex=False).str.strip()
    return pd.to_numeric(t, errors="coerce")


def now():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def norm_status(x):
    s = norm_text(x).lower()
    yes = {"تایید", "تأیید", "تاییدشده", "تأییدشده", "approved", "accept", "accepted", "1", "بله", "yes", "true"}
    no = {"رد", "عدم تایید", "عدم تأیید", "rejected", "reject", "0", "خیر", "no", "false"}
    if s in yes:
        return "approved"
    if s in no:
        return "rejected"
    return None


# ----------------------------------------------------------------------------
# نام ستون‌های اکسل‌های ورودی
# ----------------------------------------------------------------------------
C1 = {"decl_no": "شماره کوتاژ", "item_no": "ردیف قلم کالا", "tariff": "کد تعرفه کالا",
      "title": "عنوان کالا", "rial": "ارزش ریالی کالا"}
C2 = {"decl_no": "شماره کوتاژ", "owner_id": "شماره ملی صاحب کالا", "owner_name": "نام صاحب کالا",
      "decl_id": "شماره ملی اظهارکننده", "decl_name": "نام اظهارکننده",
      "decl_info": "اطلاعات اظهارنامه", "docs_image": "نام فایل تصویر سند ضمیمه"}
C3 = {"decl_no": "شماره کوتاژ", "expert_id": "کد کارشناس مجازی", "expert_name": "نام کارشناس مجازی",
      "status": "وضعیت تایید", "comment": "متن کامنت"}
C4 = {"decl_no": "شماره کوتاژ", "expert_id": "کد کارشناس بازبینی", "expert_name": "نام کارشناس بازبینی",
      "status": "وضعیت تایید", "comment": "متن کامنت"}
for _c in (C1, C2, C3, C4):
    for _k in _c:
        _c[_k] = norm_text(_c[_k])
REQ1 = ["decl_no", "tariff", "rial"]
REQ2 = ["decl_no", "owner_id", "decl_id"]
REQ3 = ["decl_no", "status"]
REQ4 = ["decl_no", "status"]

SCHEMA = """
CREATE TABLE IF NOT EXISTS clearance_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    decl_no TEXT, item_no TEXT, tariff TEXT, title TEXT, rial REAL,
    source_file TEXT, imported_at TEXT
);
CREATE TABLE IF NOT EXISTS declaration_parties (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    decl_no TEXT, owner_id TEXT, owner_name TEXT, decl_id TEXT, decl_name TEXT,
    decl_info TEXT, docs_image TEXT, source_file TEXT, imported_at TEXT
);
CREATE TABLE IF NOT EXISTS virtual_reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    decl_no TEXT, expert_id TEXT, expert_name TEXT, status TEXT, comment TEXT,
    source_file TEXT, imported_at TEXT
);
CREATE TABLE IF NOT EXISTS inspection_reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    decl_no TEXT, expert_id TEXT, expert_name TEXT, status TEXT, comment TEXT,
    source_file TEXT, imported_at TEXT
);
CREATE TABLE IF NOT EXISTS suspicious_declarations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    decl_no TEXT, tariff TEXT, title TEXT, rial REAL, ref_median REAL,
    diff_amount REAL, diff_percent REAL, robust_z REAL, threshold_percent REAL,
    created_at TEXT
);
CREATE TABLE IF NOT EXISTS suspect_profiles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    decl_no TEXT, role TEXT, person_id TEXT, person_name TEXT,
    violation_amount REAL, info_json TEXT, docs_image TEXT,
    virtual_expert_id TEXT, virtual_expert_name TEXT,
    virtual_status TEXT, virtual_comment TEXT,
    inspection_expert_id TEXT, inspection_expert_name TEXT,
    inspection_status TEXT, inspection_comment TEXT,
    stage TEXT, created_at TEXT
);
"""


def get_conn(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


def _read_many(paths, colmap, required, log):
    frames = []
    for p in paths:
        log("خواندن فایل: %s" % os.path.basename(p))
        raw = pd.read_excel(p, dtype=object)
        raw = raw.dropna(how="all")
        raw.columns = [norm_text(c) for c in raw.columns]
        missing = [colmap[k] for k in required if colmap[k] not in raw.columns]
        if missing:
            raise ValueError("در فایل «%s» ستون‌های زیر یافت نشد:\n- %s" %
                             (os.path.basename(p), "\n- ".join(missing)))
        d = pd.DataFrame()
        for key, col in colmap.items():
            d[key] = raw[col] if col in raw.columns else ""
        d["source_file"] = os.path.basename(p)
        frames.append(d)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=list(colmap) + ["source_file"])


# ----------------------------------------------------------------------------
# ورودی ۱: ارزش و سوابق ارزش قلم کالا
# ----------------------------------------------------------------------------
def import_values(conn, paths, log=print):
    d = _read_many(paths, C1, REQ1, log)
    if d.empty:
        log("هیچ ردیفی خوانده نشد.")
        return 0
    d["decl_no"] = d["decl_no"].map(norm_text)
    d["item_no"] = d["item_no"].map(norm_text)
    d["tariff"] = d["tariff"].map(norm_text)
    d["title"] = d["title"].map(norm_text)
    d["rial"] = to_num(d["rial"]).fillna(0.0)
    d["imported_at"] = now()
    d.to_sql("clearance_items", conn, if_exists="append", index=False)
    conn.commit()
    log("تعداد %d ردیف ارزش قلم کالا ذخیره شد." % len(d))
    return len(d)


# ----------------------------------------------------------------------------
# ورودی ۲: طرفین اظهارنامه + تصویر اسناد ضمیمه
# ----------------------------------------------------------------------------
def import_parties(conn, paths, log=print):
    d = _read_many(paths, C2, REQ2, log)
    if d.empty:
        log("هیچ ردیفی خوانده نشد.")
        return 0
    for c in ["decl_no", "owner_id", "owner_name", "decl_id", "decl_name", "decl_info", "docs_image"]:
        d[c] = d[c].map(norm_text)
    d["imported_at"] = now()
    d.to_sql("declaration_parties", conn, if_exists="append", index=False)
    conn.commit()
    log("تعداد %d ردیف مشخصات طرفین اظهارنامه ذخیره شد." % len(d))
    return len(d)


def check_images(conn, images_dir, log=print):
    """بررسی وجود فایل تصویر اسناد ضمیمه در پوشه‌ی انتخابی (فقط گزارش، مانع ساخت نمی‌شود)."""
    if not images_dir:
        log("پوشه‌ی تصاویر مشخص نشده؛ فقط نام فایل‌ها در پروفایل ثبت می‌شود.")
        return
    df = pd.read_sql("SELECT DISTINCT docs_image FROM declaration_parties WHERE docs_image <> ''", conn)
    missing = [f for f in df["docs_image"] for f in str(f).split(",") if f.strip()
              and not os.path.exists(os.path.join(images_dir, f.strip()))]
    if missing:
        log("هشدار: %d فایل تصویر در پوشه‌ی انتخابی یافت نشد (مثال: %s)" %
           (len(missing), ", ".join(missing[:3])))
    else:
        log("همه‌ی فایل‌های تصویر ارجاع‌شده در پوشه موجودند.")


# ----------------------------------------------------------------------------
# ورودی ۳ و ۴: بررسی کارشناس مجازی / کارشناس بازبینی
# ----------------------------------------------------------------------------
def import_virtual_reviews(conn, path, log=print):
    d = _read_many([path], C3, REQ3, log)
    for c in ["decl_no", "expert_id", "expert_name", "comment"]:
        d[c] = d[c].map(norm_text)
    d["status"] = d["status"].map(norm_status)
    d["imported_at"] = now()
    d.to_sql("virtual_reviews", conn, if_exists="append", index=False)
    conn.commit()
    log("تعداد %d بررسی کارشناس مجازی ذخیره شد." % len(d))
    return len(d)


def import_inspection_reviews(conn, path, log=print):
    d = _read_many([path], C4, REQ4, log)
    for c in ["decl_no", "expert_id", "expert_name", "comment"]:
        d[c] = d[c].map(norm_text)
    d["status"] = d["status"].map(norm_status)
    d["imported_at"] = now()
    d.to_sql("inspection_reviews", conn, if_exists="append", index=False)
    conn.commit()
    log("تعداد %d بررسی کارشناس بازبینی ذخیره شد." % len(d))
    return len(d)


# ----------------------------------------------------------------------------
# ساخت «پایگاه داده ترخیص‌های مظنون» از روی ۴ جدول خام بالا
# ----------------------------------------------------------------------------
def _latest_per_decl(df, decl_col="decl_no"):
    if df.empty:
        return df
    return df.sort_values("imported_at").groupby(decl_col, as_index=False).last()


def build_suspicious_db(conn, threshold_percent=DEFAULT_THRESHOLD_PCT, z_min=DEFAULT_Z_MIN,
                        images_dir=None, log=print):
    # ---- ماژول ۱: مقایسه‌ی AI آماری ارزش با سوابق هم‌تعرفه ----
    log("ماژول ۱: محاسبه‌ی سوابق ارزش هر کد تعرفه و مقایسه‌ی AI ...")
    items = pd.read_sql("SELECT * FROM clearance_items", conn)
    if items.empty:
        raise ValueError("ابتدا ورودی ۱ (ارزش و سوابق ارزش قلم کالا) را وارد کنید.")
    items["rial"] = pd.to_numeric(items["rial"], errors="coerce").fillna(0.0)

    g = items.groupby("tariff")["rial"]
    median = g.transform("median")
    mad = (items["rial"] - median).abs().groupby(items["tariff"]).transform("median")
    scale = (1.4826 * mad).replace(0, np.nan)
    robust_z = ((items["rial"] - median) / scale).fillna(0.0)
    diff_amount = items["rial"] - median
    diff_percent = (diff_amount / median.replace(0, np.nan)).fillna(0.0) * 100

    observed = robust_z.abs() >= z_min
    beyond_limit = diff_percent.abs() >= threshold_percent
    flagged = observed & beyond_limit
    log("تفاوت مشاهده‌شده: %d | فراتر از حد مجاز %.1f٪: %d (از %d ردیف)" %
       (int(observed.sum()), threshold_percent, int(flagged.sum()), len(items)))

    susp = items.loc[flagged, ["decl_no", "tariff", "title", "rial"]].copy()
    susp["ref_median"] = median.loc[flagged].values
    susp["diff_amount"] = diff_amount.loc[flagged].values
    susp["diff_percent"] = diff_percent.loc[flagged].values
    susp["robust_z"] = robust_z.loc[flagged].values
    susp["threshold_percent"] = threshold_percent
    susp["created_at"] = now()

    cur = conn.cursor()
    cur.execute("DELETE FROM suspect_profiles")
    cur.execute("DELETE FROM suspicious_declarations")
    susp.to_sql("suspicious_declarations", conn, if_exists="append", index=False)
    conn.commit()

    if susp.empty:
        log("هیچ اظهارنامه‌ی مظنونی یافت نشد؛ پایگاه‌داده خالی ساخته شد.")
        return {"suspicious": 0, "profiles": 0}

    # یک اظهارنامه ممکن است چند قلم مظنون داشته باشد؛ بیشینه‌ی تفاوت را برای پروفایل ملاک می‌گیریم
    per_decl = susp.loc[susp.groupby("decl_no")["diff_amount"].apply(lambda s: s.abs().idxmax())]
    log("تعداد اظهارنامه‌های مظنون (یکتا): %d" % len(per_decl))

    # ---- ماژول ۲: پروفایل صاحب کالا و اظهارکننده ----
    log("ماژول ۲: پیوند با مشخصات صاحب کالا/اظهارکننده و اسناد ضمیمه ...")
    parties = _latest_per_decl(pd.read_sql("SELECT * FROM declaration_parties", conn))
    virt = _latest_per_decl(pd.read_sql("SELECT * FROM virtual_reviews", conn))
    insp = _latest_per_decl(pd.read_sql("SELECT * FROM inspection_reviews", conn))
    check_images(conn, images_dir, log)

    missing_parties = set(per_decl["decl_no"]) - set(parties.get("decl_no", []))
    if missing_parties:
        log("هشدار: مشخصات طرفین برای %d اظهارنامه (ورودی ۲) یافت نشد؛ پروفایل آن‌ها ناقص می‌ماند." %
           len(missing_parties))

    rows = []
    for _, s in per_decl.iterrows():
        decl_no = s["decl_no"]
        p = parties[parties.get("decl_no", pd.Series(dtype=object)) == decl_no]
        p = p.iloc[0] if len(p) else None
        v = virt[virt.get("decl_no", pd.Series(dtype=object)) == decl_no] if not virt.empty else pd.DataFrame()
        v = v.iloc[0] if len(v) else None
        ins = insp[insp.get("decl_no", pd.Series(dtype=object)) == decl_no] if not insp.empty else pd.DataFrame()
        ins = ins.iloc[0] if len(ins) else None

        info = {
            "شماره کوتاژ": decl_no, "کد تعرفه کالا": s["tariff"], "عنوان کالا": s["title"],
            "ارزش ریالی اظهارشده": s["rial"], "میانه سوابق ارزش": s["ref_median"],
            "مقدار تفاوت": s["diff_amount"], "درصد تفاوت": round(float(s["diff_percent"]), 2),
            "اطلاعات اظهارنامه": (p["decl_info"] if p is not None else ""),
        }
        v_status = v["status"] if v is not None else None
        v_stage = "inspection_pending" if v_status == "approved" else (
            "closed" if v_status == "rejected" else "awaiting_virtual_review")
        i_status = ins["status"] if ins is not None else None
        stage = v_stage
        if v_stage == "inspection_pending":
            stage = "closed" if i_status in ("approved", "rejected") else "awaiting_inspection_review"

        for role, pid, pname in [
            ("owner", p["owner_id"] if p is not None else "", p["owner_name"] if p is not None else ""),
            ("declarant", p["decl_id"] if p is not None else "", p["decl_name"] if p is not None else ""),
        ]:
            rows.append((
                decl_no, role, pid, pname, float(s["diff_amount"]),
                json.dumps(info, ensure_ascii=False),
                (p["docs_image"] if p is not None else ""),
                (v["expert_id"] if v is not None else ""), (v["expert_name"] if v is not None else ""),
                v_status, (v["comment"] if v is not None else ""),
                (ins["expert_id"] if ins is not None else ""), (ins["expert_name"] if ins is not None else ""),
                i_status, (ins["comment"] if ins is not None else ""),
                stage, now(),
            ))

    cur.executemany(
        "INSERT INTO suspect_profiles (decl_no, role, person_id, person_name, violation_amount, "
        "info_json, docs_image, virtual_expert_id, virtual_expert_name, virtual_status, "
        "virtual_comment, inspection_expert_id, inspection_expert_name, inspection_status, "
        "inspection_comment, stage, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows,
    )
    conn.commit()
    log("تعداد %d پروفایل (صاحب کالا/اظهارکننده) در پایگاه داده ترخیص‌های مظنون ثبت شد." % len(rows))

    n_closed = sum(1 for r in rows if r[15] == "closed")
    n_wait_v = sum(1 for r in rows if r[15] == "awaiting_virtual_review")
    n_wait_i = sum(1 for r in rows if r[15] == "awaiting_inspection_review")
    log("وضعیت: %d بسته‌شده | %d منتظر ورود اکسل کارشناس مجازی | %d منتظر ورود اکسل کارشناس بازبینی"
       % (n_closed, n_wait_v, n_wait_i))
    return {"suspicious": len(per_decl), "profiles": len(rows)}


# ----------------------------------------------------------------------------
# خروجی اکسل برای انسان
# ----------------------------------------------------------------------------
STAGE_FA = {
    "closed": "بسته‌شده", "awaiting_virtual_review": "منتظر کارشناس مجازی",
    "awaiting_inspection_review": "منتظر کارشناس بازبینی",
}
STATUS_FA = {"approved": "تأیید", "rejected": "عدم تأیید", None: "-"}


def export_report(conn, out_path, log=print):
    df = pd.read_sql("SELECT * FROM suspect_profiles ORDER BY decl_no, role", conn)
    if df.empty:
        log("پروفایلی برای خروجی وجود ندارد؛ ابتدا پایگاه‌داده را بسازید.")
    role_fa = {"owner": "صاحب کالا", "declarant": "اظهارکننده"}
    rep = pd.DataFrame({
        "شماره کوتاژ": df.get("decl_no", []),
        "نقش فرد": df.get("role", pd.Series(dtype=object)).map(role_fa),
        "کد/شماره ملی": df.get("person_id", []),
        "نام": df.get("person_name", []),
        "مقدار تخلف (ریال)": df.get("violation_amount", []),
        "تصویر سند ضمیمه": df.get("docs_image", []),
        "کارشناس مجازی": df.get("virtual_expert_name", []),
        "وضعیت کارشناس مجازی": df.get("virtual_status", pd.Series(dtype=object)).map(STATUS_FA),
        "کامنت کارشناس مجازی": df.get("virtual_comment", []),
        "کارشناس بازبینی": df.get("inspection_expert_name", []),
        "وضعیت کارشناس بازبینی": df.get("inspection_status", pd.Series(dtype=object)).map(STATUS_FA),
        "کامنت کارشناس بازبینی": df.get("inspection_comment", []),
        "وضعیت پرونده": df.get("stage", pd.Series(dtype=object)).map(STAGE_FA),
    }) if not df.empty else pd.DataFrame()

    susp = pd.read_sql("SELECT decl_no AS 'شماره کوتاژ', tariff AS 'کد تعرفه کالا', "
                       "title AS 'عنوان کالا', rial AS 'ارزش ریالی اظهارشده', "
                       "ref_median AS 'میانه سوابق ارزش', diff_amount AS 'مقدار تفاوت', "
                       "diff_percent AS 'درصد تفاوت', threshold_percent AS 'حد مجاز (درصد)' "
                       "FROM suspicious_declarations ORDER BY decl_no", conn)

    final_violations = rep[(rep.get("وضعیت کارشناس بازبینی") == "تأیید")] if not rep.empty else rep

    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        susp.to_excel(w, sheet_name="اظهارنامه‌های مظنون", index=False)
        rep.to_excel(w, sheet_name="پروفایل افراد مظنون", index=False)
        final_violations.to_excel(w, sheet_name="تخلفات نهایی تأییدشده", index=False)
        for name in ["اظهارنامه‌های مظنون", "پروفایل افراد مظنون", "تخلفات نهایی تأییدشده"]:
            ws = w.sheets[name]
            ws.sheet_view.rightToLeft = True
            ws.freeze_panes = "A2"
    log("فایل گزارش ذخیره شد: %s (%d اظهارنامه مظنون، %d پروفایل)" % (out_path, len(susp), len(rep)))
    return len(rep)


# ----------------------------------------------------------------------------
# رابط گرافیکی
# ----------------------------------------------------------------------------
def launch_gui():
    import tkinter as tk
    from tkinter import filedialog, messagebox, ttk

    root = tk.Tk()
    root.title(APP_TITLE)
    root.geometry("980x680")
    font = ("Tahoma", 10)

    db_var = tk.StringVar(value=os.path.abspath("suspicious_clearance.db"))
    conn_holder = {"conn": None}

    def ensure_conn():
        if conn_holder["conn"] is None:
            conn_holder["conn"] = get_conn(db_var.get())
        return conn_holder["conn"]

    top = ttk.Frame(root, padding=8); top.pack(fill="x")
    ttk.Label(top, text="فایل پایگاه‌داده خروجی:", font=font).pack(side="right")
    ttk.Entry(top, textvariable=db_var, width=50, justify="right").pack(side="right", padx=6)

    def pick_db():
        p = filedialog.asksaveasfilename(defaultextension=".db", filetypes=[("SQLite DB", "*.db")],
                                         initialfile=os.path.basename(db_var.get()))
        if p:
            db_var.set(p); conn_holder["conn"] = None

    ttk.Button(top, text="انتخاب/ساخت...", command=pick_db).pack(side="right")

    nb = ttk.Notebook(root); nb.pack(fill="both", expand=True, padx=8, pady=8)
    tabs = [ttk.Frame(nb) for _ in range(6)]
    titles = ["۱) ارزش قلم کالا", "۲) طرفین + تصاویر", "۳) کارشناس مجازی",
             "۴) کارشناس بازبینی", "۵) ساخت پایگاه مظنونین", "۶) خروجی اکسل"]
    for t, title in zip(tabs, titles):
        nb.add(t, text=title)

    def make_log(parent):
        box = tk.Text(parent, height=20, font=font, state="disabled", wrap="word")
        box.pack(fill="both", expand=True, pady=8)

        def L(msg):
            box.configure(state="normal"); box.insert("end", str(msg) + "\n")
            box.see("end"); box.configure(state="disabled"); box.update()
        return L

    # ---- Tab 1: مقادیر ----
    f1 = ttk.Frame(tabs[0], padding=10); f1.pack(fill="both", expand=True)
    files1 = tk.StringVar()

    def pick1():
        p = filedialog.askopenfilenames(filetypes=[("Excel", "*.xlsx *.xlsm")])
        if p:
            files1.set(";".join(p))

    row = ttk.Frame(f1); row.pack(fill="x")
    ttk.Button(row, text="انتخاب یک یا چند اکسل ارزش قلم کالا...", command=pick1).pack(side="right")
    L1 = make_log(f1)
    ttk.Button(f1, text="وارد کردن", command=lambda: (
        files1.get() and L1("شروع...") and None,
        _try(lambda: import_values(ensure_conn(), files1.get().split(";"), L1), L1)
        if files1.get() else messagebox.showwarning("توجه", "فایلی انتخاب نشده."),
    )).pack(pady=4)

    # ---- Tab 2: طرفین ----
    f2 = ttk.Frame(tabs[1], padding=10); f2.pack(fill="both", expand=True)
    files2 = tk.StringVar(); imgdir_var = tk.StringVar()

    def pick2():
        p = filedialog.askopenfilenames(filetypes=[("Excel", "*.xlsx *.xlsm")])
        if p:
            files2.set(";".join(p))

    def pick_imgdir():
        p = filedialog.askdirectory()
        if p:
            imgdir_var.set(p)

    row2a = ttk.Frame(f2); row2a.pack(fill="x")
    ttk.Button(row2a, text="انتخاب اکسل(های) طرفین اظهارنامه...", command=pick2).pack(side="right")
    row2b = ttk.Frame(f2); row2b.pack(fill="x", pady=4)
    ttk.Label(row2b, text="پوشه‌ی تصاویر اسناد ضمیمه:", font=font).pack(side="right")
    ttk.Entry(row2b, textvariable=imgdir_var, justify="right").pack(side="right", fill="x", expand=True, padx=6)
    ttk.Button(row2b, text="انتخاب پوشه...", command=pick_imgdir).pack(side="right")
    L2 = make_log(f2)
    ttk.Button(f2, text="وارد کردن", command=lambda: (
        _try(lambda: import_parties(ensure_conn(), files2.get().split(";"), L2), L2)
        if files2.get() else messagebox.showwarning("توجه", "فایلی انتخاب نشده."),
    )).pack(pady=4)

    # ---- Tab 3: کارشناس مجازی ----
    f3 = ttk.Frame(tabs[2], padding=10); f3.pack(fill="both", expand=True)
    file3 = tk.StringVar()

    def pick3():
        p = filedialog.askopenfilename(filetypes=[("Excel", "*.xlsx *.xlsm")])
        if p:
            file3.set(p)

    row3 = ttk.Frame(f3); row3.pack(fill="x")
    ttk.Button(row3, text="انتخاب اکسل بررسی کارشناس مجازی...", command=pick3).pack(side="right")
    ttk.Entry(row3, textvariable=file3, justify="right").pack(side="right", fill="x", expand=True, padx=6)
    L3 = make_log(f3)
    ttk.Button(f3, text="وارد کردن", command=lambda: (
        _try(lambda: import_virtual_reviews(ensure_conn(), file3.get(), L3), L3)
        if file3.get() else messagebox.showwarning("توجه", "فایلی انتخاب نشده."),
    )).pack(pady=4)

    # ---- Tab 4: کارشناس بازبینی ----
    f4 = ttk.Frame(tabs[3], padding=10); f4.pack(fill="both", expand=True)
    file4 = tk.StringVar()

    def pick4():
        p = filedialog.askopenfilename(filetypes=[("Excel", "*.xlsx *.xlsm")])
        if p:
            file4.set(p)

    row4 = ttk.Frame(f4); row4.pack(fill="x")
    ttk.Button(row4, text="انتخاب اکسل بررسی کارشناس بازبینی...", command=pick4).pack(side="right")
    ttk.Entry(row4, textvariable=file4, justify="right").pack(side="right", fill="x", expand=True, padx=6)
    L4 = make_log(f4)
    ttk.Button(f4, text="وارد کردن", command=lambda: (
        _try(lambda: import_inspection_reviews(ensure_conn(), file4.get(), L4), L4)
        if file4.get() else messagebox.showwarning("توجه", "فایلی انتخاب نشده."),
    )).pack(pady=4)

    # ---- Tab 5: ساخت پایگاه مظنونین ----
    f5 = ttk.Frame(tabs[4], padding=10); f5.pack(fill="both", expand=True)
    thr_var = tk.StringVar(value=str(DEFAULT_THRESHOLD_PCT))
    row5 = ttk.Frame(f5); row5.pack(fill="x")
    ttk.Label(row5, text="حد مجاز تفاوت ارزش (درصد):", font=font).pack(side="right")
    ttk.Spinbox(row5, from_=1, to=100, textvariable=thr_var, width=8).pack(side="right", padx=6)
    L5 = make_log(f5)
    ttk.Button(f5, text="ساخت پایگاه داده ترخیص‌های مظنون", command=lambda: _try(
        lambda: build_suspicious_db(ensure_conn(), float(thr_var.get()),
                                    images_dir=imgdir_var.get(), log=L5), L5)).pack(pady=4)

    # ---- Tab 6: خروجی ----
    f6 = ttk.Frame(tabs[5], padding=10); f6.pack(fill="both", expand=True)
    out_var = tk.StringVar(value=os.path.abspath("گزارش_ترخیص_های_مظنون.xlsx"))

    def pick_out():
        p = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel", "*.xlsx")])
        if p:
            out_var.set(p)

    row6 = ttk.Frame(f6); row6.pack(fill="x")
    ttk.Button(row6, text="انتخاب مسیر خروجی...", command=pick_out).pack(side="right")
    ttk.Entry(row6, textvariable=out_var, justify="right").pack(side="right", fill="x", expand=True, padx=6)
    L6 = make_log(f6)
    ttk.Button(f6, text="ساخت فایل اکسل گزارش", command=lambda: _try(
        lambda: export_report(ensure_conn(), out_var.get(), L6), L6)).pack(pady=4)

    def _try(fn, L):
        try:
            fn()
        except Exception as e:
            L("خطا: %s" % e)
            messagebox.showerror("خطا", str(e))

    root.mainloop()


# ----------------------------------------------------------------------------
# خط فرمان
# ----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=APP_TITLE)
    sub = ap.add_subparsers(dest="cmd")

    p_b = sub.add_parser("build")
    p_b.add_argument("--db", required=True)
    p_b.add_argument("--values", nargs="+", default=[])
    p_b.add_argument("--parties", nargs="+", default=[])
    p_b.add_argument("--virtual", default=None)
    p_b.add_argument("--inspection", default=None)
    p_b.add_argument("--images-dir", default=None)
    p_b.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD_PCT)

    p_e = sub.add_parser("export")
    p_e.add_argument("db")
    p_e.add_argument("xlsx")

    args = ap.parse_args()
    if args.cmd == "build":
        conn = get_conn(args.db)
        if args.values:
            import_values(conn, args.values)
        if args.parties:
            import_parties(conn, args.parties)
        if args.virtual:
            import_virtual_reviews(conn, args.virtual)
        if args.inspection:
            import_inspection_reviews(conn, args.inspection)
        build_suspicious_db(conn, threshold_percent=args.threshold, images_dir=args.images_dir)
    elif args.cmd == "export":
        conn = get_conn(args.db)
        export_report(conn, args.xlsx)
    else:
        launch_gui()


if __name__ == "__main__":
    main()
