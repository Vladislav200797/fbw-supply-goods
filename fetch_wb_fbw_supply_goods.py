#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Товары поставок (Supplies API → Supabase/public).

Режимы:
- FULL_REFRESH=true  -> полный refresh: DELETE всей таблицы -> INSERT всех собранных строк
- FULL_REFRESH=false -> инкрементальный UPSERT по (wb_key, barcode) без удалений

Фильтр:
- GOODS_UPDATED_DAYS="7" — берём поставки, обновлявшиеся за последние 7 дней (если "", берём все)

Лимит WB: 30 req/min → REQUEST_SLEEP_SEC ~ 2.1 сек.
"""

import os
import sys
import time
from typing import List, Dict, Any, Tuple
import requests
from supabase import create_client, Client

WB_SUPPLIES_TOKEN     = os.getenv("WB_SUPPLIES_TOKEN")
SUPABASE_URL          = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY  = os.getenv("SUPABASE_SERVICE_KEY")
SCHEMA                = os.getenv("SUPABASE_SCHEMA", "public")
SUPPLIES_TABLE        = os.getenv("SUPABASE_SUPPLIES_TABLE", "fbw_supplies")
GOODS_TABLE           = os.getenv("SUPABASE_GOODS_TABLE",   "fbw_supply_goods")

REQUEST_SLEEP_SEC     = float(os.getenv("REQUEST_SLEEP_SEC", "2.1"))
GOODS_UPDATED_DAYS    = os.getenv("GOODS_UPDATED_DAYS", "").strip()
MAX_KEYS_ENV          = os.getenv("MAX_KEYS", "").strip()
LOG_EVERY_ENV         = os.getenv("LOG_EVERY", "25").strip()
FULL_REFRESH          = os.getenv("FULL_REFRESH", "false").strip().lower() in ("1","true","yes","y")

API_BASE = "https://supplies-api.wildberries.ru/api/v1"
HEADERS = {"Authorization": WB_SUPPLIES_TOKEN or "", "Content-Type": "application/json"}

def fail(msg: str, code: int = 1):
    print(f"ERROR: {msg}", file=sys.stderr, flush=True)
    sys.exit(code)

def parse_wb_key(wb_key: str) -> Tuple[bool, int]:
    if wb_key.startswith("S:"): return (False, int(wb_key[2:]))
    if wb_key.startswith("P:"): return (True, int(wb_key[2:]))
    raise ValueError(f"Bad wb_key format: {wb_key}")

def fetch_goods_for_supply(id_value: int, is_preorder: bool) -> List[Dict[str, Any]]:
    url = f"{API_BASE}/supplies/{id_value}/goods"
    out: List[Dict[str, Any]] = []
    limit, offset = 1000, 0
    while True:
        params = {"isPreorderID": "true" if is_preorder else "false", "limit": limit, "offset": offset}
        backoffs = [0, 2, 5]
        for attempt, wait in enumerate(backoffs, start=1):
            if wait: time.sleep(wait)
            resp = requests.get(url, headers=HEADERS, params=params, timeout=60)
            if resp.status_code == 200:
                data = resp.json()
                if not isinstance(data, list): fail(f"Unexpected WB response for goods: {data}")
                out.extend(data)
                break
            if resp.status_code in (404, 410): return []
            if resp.status_code == 429 and attempt < len(backoffs): continue
            fail(f"WB API {resp.status_code}: {resp.text}")
        if len(out) - offset < limit: break
        offset += limit
        if REQUEST_SLEEP_SEC > 0: time.sleep(REQUEST_SLEEP_SEC)
    return out

def normalize_goods(wb_key: str, is_preorder: bool, supply_id: int, preorder_id: int, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        tech_size = r.get("techSize", r.get("techsize"))
        out.append({
            "wb_key": wb_key,
            "is_preorder": is_preorder,
            "supply_id": supply_id,
            "preorder_id": preorder_id,
            "barcode": r.get("barcode"),
            "vendor_code": r.get("vendorCode"),
            "nm_id": r.get("nmID"),
            "need_kiz": r.get("needKiz"),
            "tnved": r.get("tnved"),
            "tech_size": tech_size,
            "color": r.get("color"),
            "supplier_box_amount": r.get("supplierBoxAmount"),
            "quantity": r.get("quantity"),
            "ready_for_sale_quantity": r.get("readyForSaleQuantity"),
            "accepted_quantity": r.get("acceptedQuantity"),
            "unloading_quantity": r.get("unloadingQuantity"),
        })
    return out

def chunked(seq: List[Dict[str, Any]], size: int):
    for i in range(0, len(seq), size): yield seq[i:i+size]

def main():
    if not WB_SUPPLIES_TOKEN: fail("WB_SUPPLIES_TOKEN is empty")
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY: fail("Supabase URL or SERVICE KEY is empty")

    sb: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    # 1) Ключи поставок (с фильтром по updated_date при необходимости)
    query = sb.schema(SCHEMA).table(SUPPLIES_TABLE).select("wb_key,supply_id,preorder_id,updated_date")
    if GOODS_UPDATED_DAYS:
        try:
            ndays = int(GOODS_UPDATED_DAYS)
            from datetime import datetime, timedelta, timezone
            cutoff = (datetime.now(timezone.utc) - timedelta(days=ndays)).isoformat(timespec="seconds").replace("+00:00","Z")
            query = query.gte("updated_date", cutoff)
        except Exception: pass

    resp = query.execute()
    src_rows = getattr(resp, "data", None) or resp.data
    keys = src_rows if isinstance(src_rows, list) else []

    # Клиентская фильтрация (на всякий случай)
    if GOODS_UPDATED_DAYS:
        try:
            ndays = int(GOODS_UPDATED_DAYS)
            from datetime import datetime, timedelta, timezone
            cutoff_dt = datetime.now(timezone.utc) - timedelta(days=ndays)
            filtered = []
            for k in keys:
                ud = k.get("updated_date")
                if not ud: continue
                try:
                    iso = ud.replace("Z", "+00:00") if isinstance(ud, str) else ud
                    dt = datetime.fromisoformat(iso)
                    if dt >= cutoff_dt: filtered.append(k)
                except Exception:
                    filtered.append(k)
            keys = filtered
        except Exception: pass

    # Лимит и лог
    max_keys = None
    if MAX_KEYS_ENV:
        try: max_keys = int(MAX_KEYS_ENV)
        except Exception: max_keys = None
    if max_keys is not None: keys = keys[:max_keys]

    try: log_every = max(1, int(LOG_EVERY_ENV))
    except Exception: log_every = 25

    total = len(keys)
    print(f"Supplies to fetch goods for: {total} (updated<= {GOODS_UPDATED_DAYS or 'ALL'} days, limit={max_keys or '∞'}), FULL_REFRESH={FULL_REFRESH}", flush=True)

    if total == 0 and not FULL_REFRESH:
        print("No supplies to process in incremental mode. Skipping.", flush=True)
        return

    collected: List[Dict[str, Any]] = []
    processed = 0
    errors = 0

    # 2) Обход поставок
    for k in keys:
        wb_key = k.get("wb_key"); supply_id = k.get("supply_id"); preorder_id = k.get("preorder_id")
        try:
            is_preorder, id_value = parse_wb_key(wb_key)
            goods = fetch_goods_for_supply(id_value, is_preorder)
            if goods:
                collected.extend(normalize_goods(wb_key, is_preorder, supply_id, preorder_id, goods))
        except Exception as e:
            errors += 1
            print(f"[WARN] {wb_key}: {e}", flush=True)

        processed += 1
        if processed % log_every == 0 or processed == total:
            print(f"Processed {processed}/{total} (collected {len(collected)}, errors {errors})", flush=True)

        if REQUEST_SLEEP_SEC > 0: time.sleep(REQUEST_SLEEP_SEC)

    print(f"Total collected rows: {len(collected)}; errors: {errors}", flush=True)

    # 3) Загрузка: FULL_REFRESH vs UPSERT
    if FULL_REFRESH:
        print("FULL_REFRESH=true → clearing target table...", flush=True)
        sb.schema(SCHEMA).table(GOODS_TABLE).delete().neq("wb_key", "").execute()
        print("Inserting data...", flush=True)
        inserted = 0
        for batch in chunked(collected, 500):
            sb.schema(SCHEMA).table(GOODS_TABLE).insert(batch).execute()
            inserted += len(batch)
        print(f"Inserted rows: {inserted}", flush=True)
    else:
        print("FULL_REFRESH=false → UPSERT by (wb_key, barcode) (no deletions)...", flush=True)
        upserted = 0
        for batch in chunked(collected, 500):
            sb.schema(SCHEMA).table(GOODS_TABLE).upsert(batch, on_conflict="wb_key,barcode").execute()
            upserted += len(batch)
        print(f"Upserted rows: {upserted}", flush=True)

    print("Goods sync OK", flush=True)

if __name__ == "__main__":
    main()
