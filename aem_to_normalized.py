#!/usr/bin/env python3
# v1.1
# Usage examples (change URL as needed):
# python aem_to_normalized.py --url 'https://publish-p151554-e1560130.adobeaemcloud.com/api/teladoc-core/v1/education?folder=/content/dam/teladoc-headless/en-us/education-service'
#   --out output.json
#
# The script fetches AEM public JSON for Education service and normalizes it into
# four tables: content, content_to_text, content_to_content, content_to_attribute.
# It normalizes asset URLs, derives asset metadata (title/width/height/mime), and
# builds relationships between Curriculum, Unit, Lesson and its children pages(imagePage,questionPage etc).

import sys, os, json, argparse, ssl, base64
from typing import Tuple, Optional, Dict, Set, List
from datetime import datetime, timezone
from urllib.parse import urlparse
import re

# --- HTTP ---
def http_get(url: str, headers: dict, verify_ssl: bool = True, timeout: int = 60) -> str:
    try:
        import requests
        r = requests.get(url, headers=headers or {}, timeout=timeout, verify=verify_ssl)
        r.raise_for_status()
        r.encoding = r.encoding or 'utf-8'
        return r.text
    except ImportError:
        import urllib.request, urllib.error
        req = urllib.request.Request(url, headers=headers or {})
        ctx = None
        if not verify_ssl:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        with urllib.request.urlopen(req, context=ctx, timeout=timeout) as r:
            ct = r.headers.get_content_charset() or 'utf-8'
            return r.read().decode(ct, errors='replace')

# --- IDs & Maps ---
CONTENT_TYPE_ID = {'Curriculum':1,'Unit':2,'Lesson':3,'Page':4,'Answer':5,'Asset':6,'Term':7,'Tag':8}
PATH_TOKEN_TO_TYPE_ID = {'curriculum':1,'unit':2,'lesson':3,'question-answer':5,'term':7,'tag':8}
LABEL_MAP = {
    'iconPage':303,'tipPage':304,'imagePage':305,'questionPage':310,
    'lessonTableOfContentsPage':311,'lessonIntroPage':313,'curriculumIntroPage':314,
}
IMAGE_ROLE_LABEL = {
    'icon':401,'iconImage':401,
    'posterImage':402,
    'thumbnailImage':403,
    'heroImage':404,
    'titleImage':405,
    'title_image':405,
    'titleimage':405,
}
ROLE_PRIORITY = {405:5, 404:4, 403:3, 401:2, 402:1, None:0}
ATTR_ID_MAP = {'DM':1,'HTN':2,'DPP':3,'WL':4,'None':5,'Daily':6,'Weekly':7,'Monthly':8,'AWM':9,'CWC':10}
TAG_TO_ATTR = {'dm':1,'htn':2,'dpp':3,'wl':4,'awm':9,'cwc':10}
TEXT_FIELDS = {
    'title':1,'subtitle':2,'description':3,'beginCaption':4,'finishCaption':5,'completedMessage':6,'body':7,
    'tipText':8,'questionId':9,'question':10,'otherCaption':13,'otherPlaceholder':14,'acknowledgement':15,
    'name':16,'definition':17,'tipType':22,'includeOther':23,'cadenceSkip':24,'lastRequiredPageIndex':25,'answerText':26,
}
SPECIAL_TO_TEXT = {'titleRequiredEnglish':1,'descriptionRequiredEnglish':7}
IMAGE_URL_KEYS = ['heroImage','thumbnailImage','posterImage','titleImage','icon','iconImage','title_image','titleimage']
REL_DAM_RE = re.compile(r'^/content/dam/teladoc-headless/image/')

# Switch default to PROD later when content has been promoted to that AEM ENV.
# DEV
DEFAULT_DAM_BASE = 'https://publish-p151554-e1560130.adobeaemcloud.com'
# PROD
# DEFAULT_DAM_BASE = 'https://publish-p151554-e1560174.adobeaemcloud.com'

# --- URL Helpers ---
def _pick_base_url(items: List[dict]) -> Optional[str]:
    for it in items:
        data = (it.get('data') or {})
        for k in IMAGE_URL_KEYS:
            url = data.get(k)
            if isinstance(url, str) and url.startswith('http'):
                pu = urlparse(url)
                return f"{pu.scheme}://{pu.netloc}"
        imgs = data.get('images')
        if isinstance(imgs, list):
            for url in imgs:
                if isinstance(url, str) and url.startswith('http'):
                    pu = urlparse(url)
                    return f"{pu.scheme}://{pu.netloc}"
    return None

def _normalize_url(url: str, base: Optional[str]) -> str:
    if not isinstance(url, str) or url == '':
        return url
    if url.startswith('http://') or url.startswith('https://'):
        return url
    if url.startswith('/') and REL_DAM_RE.match(url):
        chosen_base = (base or DEFAULT_DAM_BASE)
        return chosen_base.rstrip('/') + url
    return url

# --- Asset Metadata Helpers ---
def _dig(obj, path_list, default=None):
    cur = obj
    for k in path_list:
        try:
            if isinstance(k, int) and isinstance(cur, list):
                cur = cur[k]
            elif isinstance(cur, dict):
                cur = cur[k]
            else:
                return default
        except (KeyError, IndexError, TypeError):
            return default
    return cur

def _to_int(v):
    try:
        if v is None:
            return None
        return int(str(v).strip())
    except Exception:
        return None

def derive_asset_id(url: str) -> Optional[str]:
    import os
    b = os.path.basename(url or '')
    return b.split('.')[0] if b else None

def derive_asset_ext(url: str) -> str:
    try:
        import os
        b = os.path.basename(url or '')
        if '.' in b:
            return b.rsplit('.', 1)[1].split('?')[0].lower().strip()
    except Exception:
        pass
    return ''

def _extract_image_url(img) -> Optional[str]:
    if isinstance(img, str) and img:
        return img
    if isinstance(img, dict):
        for k in ('src','url','fileReference','fileRef','path','href'):
            v = img.get(k)
            if isinstance(v, str) and v:
                return v
    return None

def fetch_asset_metadata(asset_id: str, base: Optional[str], timeout: int = 60, insecure: bool = False, ext_hint: Optional[str] = None) -> Optional[dict]:
    """
    Opt 2: use ONLY ext_hint; if missing, skip fetch (return None).
    """
    if not asset_id or not ext_hint:
        return None
    hb = (base or DEFAULT_DAM_BASE).rstrip('/')
    headers = {'Accept': 'application/json'}
    ext = ext_hint.lower().strip()
    url = f"{hb}/content/dam/teladoc-headless/image/{asset_id}.{ext}.-1.json"
    try:
        raw = http_get(url, headers=headers, verify_ssl=(not insecure), timeout=timeout)
        meta = json.loads(raw)
        title_raw = _dig(meta, ['jcr:content','metadata','dc:title'])
        if isinstance(title_raw, list):
            title = title_raw[0] if title_raw else asset_id
        else:
            title = title_raw if title_raw not in (None, '') else asset_id
        mime = _dig(meta, ['jcr:content','metadata','dc:format'])
        width = _to_int(_dig(meta, ['jcr:content','metadata','tiff:ImageWidth']))
        height = _to_int(_dig(meta, ['jcr:content','metadata','tiff:ImageLength']))
        return {
            'title': str(title) if title is not None else asset_id,
            'width': width if width is not None else 0,
            'height': height if height is not None else 0,
            'mime': str(mime) if mime else 'image/jpeg'
        }
    except Exception:
        return None

# --- Misc Helpers ---
def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00','Z')

def infer_type_label(path: str):
    if not path:
        return None, None
    parts = path.strip('/').split('/')
    ctype, clabel = None, None
    for seg in parts:
        if seg in LABEL_MAP:
            return CONTENT_TYPE_ID['Page'], LABEL_MAP[seg]
        if seg in PATH_TOKEN_TO_TYPE_ID:
            ctype = PATH_TOKEN_TO_TYPE_ID[seg]
    return ctype, clabel

# --- Transform ---
def transform(items: List[dict], link_lessons_to_assets: bool = True, base_url: Optional[str] = None, asset_meta_timeout: int = 60, insecure: bool = False) -> dict:
    created = now_iso()
    if base_url is None:
        base_url = _pick_base_url(items) or DEFAULT_DAM_BASE

    content: List[dict] = []
    ctt: List[dict] = []
    ctc: List[dict] = []
    cta: List[dict] = []

    content_index: Dict[str, dict] = {}
    curricula: Dict[str, dict] = {}
    units: Dict[str, dict] = {}
    pages: Dict[str, dict] = {}
    lessons: Dict[str, dict] = {}
    terms: Dict[str, dict] = {}
    known_ids: Set[str] = set()
    include_mask: Dict[str, bool] = {}
    type_cache: Dict[str, Tuple[Optional[int], Optional[int]]] = {}

    def _basename(p: str) -> str:
        return (p or '').rstrip('/').split('/')[-1]

    # Build index of items by name for easy lookup
    items_index: Dict[str, dict] = {}

    # Inclusion
    for it in items:
        name = it.get('name')
        if name:
            items_index[name] = it
        path = it.get('path', '')
        ctype, clabel = infer_type_label(path)
        type_cache[name] = (ctype, clabel)
        include_mask[name] = (ctype is not None)
        if ctype == CONTENT_TYPE_ID['Page']:
            include_mask[name] = True

    # content rows
    for it in items:
        name = it.get('name')
        if not name or not include_mask.get(name, False):
            continue
        ctype, clabel = type_cache[name]
        row = {'cms_id': name,'content_version': 1,'content_type_id': ctype,'content_label_id': clabel,'created_date': created,'deleted_date': ''}
        content.append(row)
        content_index[name] = row
        known_ids.add(name)
        if ctype == CONTENT_TYPE_ID['Curriculum']:
            curricula[name] = it
        elif ctype == CONTENT_TYPE_ID['Unit']:
            units[name] = it
        elif ctype == CONTENT_TYPE_ID['Lesson']:
            lessons[name] = it
        elif ctype == CONTENT_TYPE_ID['Term']:
            terms[name] = it
        elif ctype == CONTENT_TYPE_ID['Page']:
            pages[name] = it

    # text fields → ctt
    for it in items:
        name = it.get('name')
        if not name or not include_mask.get(name, False):
            continue
        data = it.get('data', {}) or {}
        for k, tid in TEXT_FIELDS.items():
            if k in data and data[k] not in (None, ''):
                ctt.append({'content_cms_id': name,'created_date': created,'deleted_date': '','id': None,'locale_id': 1,'text_index': 0,'text_type_id': tid,'text_value': str(data[k])})
        for k, tid in SPECIAL_TO_TEXT.items():
            if k in data and data[k]:
                ctt.append({'content_cms_id': name,'created_date': created,'deleted_date': '','id': None,'locale_id': 1,'text_index': 0,'text_type_id': tid,'text_value': str(data[k])})

    # Build map of AnswerID → answerText from CTT rows (answers processed above)
    answer_text_index: Dict[str, str] = {}
    for row in ctt:
        try:
            if int(row.get('text_type_id', 0)) == 26: # answerText
                cid = row.get('content_cms_id')
                if cid:
                    answer_text_index[cid] = str(row.get('text_value', ''))
        except Exception:
            continue

    # attributes → cta
    for it in items:
        name = it.get('name')
        if not name or not include_mask.get(name, False):
            continue
        data = it.get('data', {}) or {}
        cats = data.get('categories')
        if isinstance(cats, list):
            for c in cats:
                aid = ATTR_ID_MAP.get(str(c)) or ATTR_ID_MAP.get(str(c).upper())
                if aid:
                    cta.append({'id': None,'content_cms_id': name,'attribute_id': aid,'created_date': created,'deleted_date': ''})
        cad = data.get('cadence')
        if cad:
            aid = ATTR_ID_MAP.get(str(cad)) or ATTR_ID_MAP.get(str(cad).capitalize())
            if aid:
                cta.append({'id': None,'content_cms_id': name,'attribute_id': aid,'created_date': created,'deleted_date': ''})
        tags = data.get('tags')
        if isinstance(tags, list):
            for t in tags:
                aid = TAG_TO_ATTR.get(str(t).lower())
                if aid:
                    cta.append({'id': None,'content_cms_id': name,'attribute_id': aid,'created_date': created,'deleted_date': ''})

    # Asset caches
    url_row_written: Set[str] = set()
    asset_ext_hint: Dict[str, str] = {}
    asset_meta_cache: Dict[str, Optional[dict]] = {}
    asset_url_map: Dict[str, str] = {}

    # Curriculum → Unit
    for cid, cit in curricula.items():
        data = cit.get('data', {}) or {}
        arr = data.get('units')
        if isinstance(arr, list) and arr:
            for idx, ref in enumerate(arr):
                p = ref.get('path','') if isinstance(ref, dict) else ''
                child = p.rstrip('/').split('/')[-1] if p else None
                if child and child in known_ids:
                    ctc.append({'id': None,'parent_cms_id': cid,'child_cms_id': child,'child_content_label_id': None,'child_index': idx,'created_date': created,'deleted_date': ''})
        else:
            idx = 0
            marker = f"/curriculum/{cid}/"
            for uid, uit in units.items():
                up = uit.get('path', '')
                if marker in up and uid in known_ids:
                    ctc.append({'id': None,'parent_cms_id': cid,'child_cms_id': uid,'child_content_label_id': None,'child_index': idx,'created_date': created,'deleted_date': ''})
                    idx += 1

    # Unit → Lesson (explicit)
    for it in items:
        pname = it.get('name')
        if not pname or not include_mask.get(pname, False):
            continue
        data = it.get('data', {}) or {}
        if isinstance(data.get('lessons'), list):
            for idx, ref in enumerate(data['lessons']):
                p = ref.get('path','') if isinstance(ref, dict) else ''
                child = p.rstrip('/').split('/')[-1] if p else None
                if child and child in known_ids:
                    ctc.append({'id': None,'parent_cms_id': pname,'child_cms_id': child,'child_content_label_id': None,'child_index': idx,'created_date': created,'deleted_date': ''})

    # Lesson → Page
    lesson_to_pages: Dict[str, List[str]] = {l: [] for l in lessons}
    for it in items:
        pname = it.get('name')
        if not pname or not include_mask.get(pname, False):
            continue
        data = it.get('data', {}) or {}
        if isinstance(data.get('pages'), list):
            for idx, ref in enumerate(data['pages']):
                p = ref.get('path','') if isinstance(ref, dict) else ''
                child = p.rstrip('/').split('/')[-1] if p else None
                if not child or child not in known_ids:
                    continue
                label = None
                for seg in p.strip('/').split('/'):
                    if seg in LABEL_MAP:
                        label = LABEL_MAP[seg]
                        break
                ctc.append({'id': None,'parent_cms_id': pname,'child_cms_id': child,'child_content_label_id': label,'child_index': idx,'created_date': created,'deleted_date': ''})
                if pname in lesson_to_pages:
                    lesson_to_pages[pname].append(child)

    
    # ---- Term.contentReference[] → (ImagePage → Term) edges ----
    # Accept object OR array (also supports 'contentReferences'). Only link to existing ImagePages.
    per_page_term_index: Dict[str, int] = {}
    for term_id, term_it in terms.items():
        tdata = (term_it.get('data') or {})
        refs = tdata.get('contentReference') or tdata.get('contentReferences')
        if not refs:
            continue
        # Normalize to list
        if isinstance(refs, (dict, str)):
            refs = [refs]
        elif not isinstance(refs, list):
            continue

        for ref in refs:
            if isinstance(ref, dict):
                rpath = ref.get('path', '')
            elif isinstance(ref, str):
                rpath = ref
            else:
                rpath = ''
            parent_page_id = _basename(rpath) if rpath else ''
            # Only link if the parent exists and is an ImagePage
            if not parent_page_id or parent_page_id not in pages:
                continue
            _, page_label = type_cache.get(parent_page_id, (None, None))
            if page_label != LABEL_MAP.get('imagePage'):
                continue
            idx = per_page_term_index.get(parent_page_id, 0)
            ctc.append({
                'id': None,
                'parent_cms_id': parent_page_id,
                'child_cms_id': term_id,
                'child_content_label_id': None,
                'child_index': idx,
                'created_date': created,
                'deleted_date': ''
            })
            per_page_term_index[parent_page_id] = idx + 1


    # Helper: record asset with multi-role support
    def _record_asset(aid: str, url: str, role: Optional[int], parent_id: str,
                      order_list: List[str], roles_per_asset: Dict[str, Set[Optional[int]]]):
        if aid not in known_ids:
            row = {'cms_id':aid,'content_version':1,'content_type_id':CONTENT_TYPE_ID['Asset'],'content_label_id':None,'created_date':created,'deleted_date': ''}
            content.append(row); content_index[aid] = row; known_ids.add(aid)
        norm = _normalize_url(url, base_url)
        if aid not in url_row_written:
            ctt.append({'content_cms_id': aid,'created_date': created,'deleted_date': '','id': None,'locale_id': 1,'text_index': 0,'text_type_id': 18,'text_value': str(norm)})
            url_row_written.add(aid)
        asset_url_map[aid] = str(norm)
        ext = derive_asset_ext(str(url))
        if ext and aid not in asset_ext_hint:
            asset_ext_hint[aid] = ext
        if aid not in roles_per_asset:
            roles_per_asset[aid] = set()
        order_list.append(aid)
        roles_per_asset[aid].add(role)
        # maintain asset content_label_id as highest role seen overall
        arow = content_index.get(aid)
        if arow is not None:
            current = arow.get('content_label_id')
            if current in (None, '') or ROLE_PRIORITY.get(role,0) > ROLE_PRIORITY.get(current,0):
                if role is not None:
                    arow['content_label_id'] = role

    # Non-Page images (Lessons included) — generic image/images[] → 404; multi-role
    for it in items:
        parent_id = it.get('name')
        if not parent_id or not include_mask.get(parent_id, False):
            continue
        ctype, _ = type_cache.get(parent_id, (None, None))
        if ctype in (CONTENT_TYPE_ID['Page'], None):
            continue
        pdata = it.get('data', {}) or {}
        roles_per_asset: Dict[str, Set[Optional[int]]] = {}
        encounter_order: List[str] = []

        # generic single image → 404
        gen_url = _extract_image_url(pdata.get('image'))
        if gen_url:
            aid = derive_asset_id(str(gen_url))
            if aid:
                _record_asset(aid, gen_url, 404, parent_id, encounter_order, roles_per_asset)

        # single-image semantic keys
        for key in IMAGE_URL_KEYS:
            val = pdata.get(key)
            url = _extract_image_url(val)
            if url:
                aid = derive_asset_id(str(url))
                if aid:
                    _record_asset(aid, url, IMAGE_ROLE_LABEL.get(key), parent_id, encounter_order, roles_per_asset)

        # images[] → 404
        imgs = pdata.get('images')
        if isinstance(imgs, list):
            for url in imgs:
                if url:
                    aid = derive_asset_id(str(url))
                    if aid:
                        _record_asset(aid, url, 404, parent_id, encounter_order, roles_per_asset)

        # emit one edge PER ROLE for each asset (priority-desc)
        for aid in encounter_order:
            roles = sorted(list(roles_per_asset.get(aid, {None})), key=lambda r: ROLE_PRIORITY.get(r,0), reverse=True)
            for role in roles:
                ctc.append({'id': None,'parent_cms_id': parent_id,'child_cms_id': aid,'child_content_label_id': role,'child_index': len(ctc),
                            'created_date': created,'deleted_date': ''})

    # Page → Asset (multi-role)
    page_to_assets: Dict[str, List[Tuple[str, Set[Optional[int]]]]] = {}
    for pid, pit in pages.items():
        if not include_mask.get(pid, False):
            continue
        pdata = (pit.get('data') or {})
        roles_per_asset: Dict[str, Set[Optional[int]]] = {}
        encounter_order: List[str] = []

        # generic single image on Page → 404
        pg_gen_url = _extract_image_url(pdata.get('image'))
        if pg_gen_url:
            aid = derive_asset_id(str(pg_gen_url))
            if aid:
                _record_asset(aid, pg_gen_url, 404, pid, encounter_order, roles_per_asset)

        # single-image semantic keys
        for key in IMAGE_URL_KEYS:
            val = pdata.get(key)
            url = _extract_image_url(val)
            if url:
                aid = derive_asset_id(str(url))
                if aid:
                    _record_asset(aid, url, IMAGE_ROLE_LABEL.get(key), pid, encounter_order, roles_per_asset)

        # images[] on Page → 404
        imgs = pdata.get('images')
        if isinstance(imgs, list):
            for url in imgs:
                if url:
                    aid = derive_asset_id(str(url))
                    if aid:
                        _record_asset(aid, url, 404, pid, encounter_order, roles_per_asset)

        # emit to Page
        listing: List[Tuple[str, Set[Optional[int]]]] = []
        for aid in encounter_order:
            roles = roles_per_asset.get(aid, set())
            # output edges for each role (priority-desc)
            for role in sorted(list(roles), key=lambda r: ROLE_PRIORITY.get(r,0), reverse=True):
                ctc.append({'id': None,'parent_cms_id': pid,'child_cms_id': aid,'child_content_label_id': role,'child_index': len(ctc),
                            'created_date': created,'deleted_date': ''})
            listing.append((aid, roles))
        page_to_assets[pid] = listing

    # --- Question Page → Answer edges (strings OR dicts) ---
    def _ensure_answer_content(answer_id: str):
        if answer_id and answer_id not in known_ids:
            row = {
                'cms_id': answer_id,
                'content_version': 1,
                'content_type_id': CONTENT_TYPE_ID['Answer'],
                'content_label_id': None,
                'created_date': created,
                'deleted_date': ''
            }
            content.append(row)
            content_index[answer_id] = row
            known_ids.add(answer_id)

    def _extract_answer_id(ref) -> str:
        if isinstance(ref, str):
            ref_path = ref
        elif isinstance(ref, dict):
            ref_path = (ref or {}).get('path', '')
        else:
            ref_path = ''
        return _basename(ref_path) if ref_path else ''

    for it in items:
        qid = it.get('name')
        if not qid or not include_mask.get(qid, False):
            continue
        ctype, clabel = type_cache.get(qid, (None, None))
        if ctype != CONTENT_TYPE_ID['Page'] or clabel != LABEL_MAP.get('questionPage'):
            continue
        data = it.get('data') or {}

        pot = data.get('potentialAnswers')
        if isinstance(pot, list) and pot:
            child_index = 0
            for ref in pot:
                answer_id = _extract_answer_id(ref)
                if not answer_id:
                    continue
                _ensure_answer_content(answer_id)
                ctc.append({'id': None,
                            'parent_cms_id': qid,
                            'child_cms_id': answer_id,
                            'child_content_label_id': 501, # potential answer
                            'child_index': child_index,
                            'created_date': created,
                            'deleted_date': ''})
                child_index += 1

        corr = data.get('correctAnswers')
        if isinstance(corr, list) and corr:
            child_index = 0
            for ref in corr:
                answer_id = _extract_answer_id(ref)
                if not answer_id:
                    continue
                _ensure_answer_content(answer_id)
                # CTC label 502
                ctc.append({'id': None,
                            'parent_cms_id': qid,
                            'child_cms_id': answer_id,
                            'child_content_label_id': 502, # correct answer
                            'child_index': child_index,
                            'created_date': created,
                            'deleted_date': ''})
                # CTT row on the Question: text_type_id=26, text_value = answerText
                # Try direct from items_index first
                ans_text = ''
                ait = items_index.get(answer_id)
                if ait:
                    ans_text = str(((ait.get('data') or {}).get('answerText')) or '')
                if not ans_text:
                    ans_text = answer_text_index.get(answer_id, '')
                if ans_text:
                    ctt.append({'content_cms_id': qid,
                                'created_date': created,
                                'deleted_date': '',
                                'id': None,
                                'locale_id': 1,
                                'text_index': child_index,
                                'text_type_id': 26,
                                'text_value': ans_text})
                child_index += 1

    # POST-PASS enrichment for assets (Opt 2/3/4)
    url_assets: Set[str] = set()
    for row in ctt:
        try:
            if int(row.get('text_type_id', 0)) == 18:
                cid = row.get('content_cms_id')
                if cid:
                    url_assets.add(cid)
        except Exception:
            continue

    def _enrich_asset_ctt_rows_for(aid: str,
                                   base_url: Optional[str],
                                   created: str,
                                   ctt: List[dict],
                                   asset_meta_cache: Dict[str, Optional[dict]],
                                   asset_ext_hint: Dict[str, str],
                                   asset_url_map: Dict[str, str],
                                   timeout: int,
                                   insecure: bool):
        if not aid:
            return
        existing_types: Set[int] = set()
        for row in ctt:
            if row.get('content_cms_id') == aid:
                try:
                    ttid = int(row.get('text_type_id'))
                except Exception:
                    continue
                existing_types.add(ttid)
        if {1,19,20,21}.issubset(existing_types):
            return

        url = asset_url_map.get(aid, '')
        should_fetch = isinstance(url, str) and '/content/dam/teladoc-headless/image/' in url
        ext = asset_ext_hint.get(aid)
        if not ext and isinstance(url, str):
            ext = derive_asset_ext(url)

        meta = None
        if should_fetch and ext:
            if aid not in asset_meta_cache:
                meta = fetch_asset_metadata(aid, base=base_url, timeout=timeout, insecure=insecure, ext_hint=ext)
                asset_meta_cache[aid] = meta
            else:
                meta = asset_meta_cache.get(aid)

        meta = meta or {}
        title = (meta.get('title') or aid)
        width = meta.get('width') if meta.get('width') is not None else 0
        height = meta.get('height') if meta.get('height') is not None else 0
        mime = meta.get('mime') or 'image/jpeg'

        if 1 not in existing_types:
            ctt.append({'id': None,'content_cms_id': aid,'text_type_id': 1,'text_value': str(title),'text_index': 0,'created_date': created,'deleted_date': '','locale_id': 1})
        if 19 not in existing_types:
            ctt.append({'id': None,'content_cms_id': aid,'text_type_id': 19,'text_value': width,'text_index': 0,'created_date': created,'deleted_date': '','locale_id': 1})
        if 20 not in existing_types:
            ctt.append({'id': None,'content_cms_id': aid,'text_type_id': 20,'text_value': height,'text_index': 0,'created_date': created,'deleted_date': '','locale_id': 1})
        if 21 not in existing_types:
            ctt.append({'id': None,'content_cms_id': aid,'text_type_id': 21,'text_value': str(mime),'text_index': 0,'created_date': created,'deleted_date': '','locale_id': 1})

    for aid in sorted(url_assets):
        _enrich_asset_ctt_rows_for(aid=aid, base_url=base_url, created=created, ctt=ctt,
                                   asset_meta_cache=asset_meta_cache, asset_ext_hint=asset_ext_hint,
                                   asset_url_map=asset_url_map, timeout=asset_meta_timeout, insecure=insecure)

    # NOTE: Removed the optional Lesson ← Page assets cascade. Assets remain at their native level.

    # De-dup content_to_text
    seen_ctt: Set[tuple] = set()
    deduped_ctt: List[dict] = []
    for row in ctt:
        key = (row.get('content_cms_id'),int(row.get('locale_id', 1)),int(row.get('text_type_id')),int(row.get('text_index', 0)),str(row.get('text_value')))
        if key in seen_ctt:
            continue
        seen_ctt.add(key)
        deduped_ctt.append(row)
    ctt = deduped_ctt

    return {'content': content,'content_to_text': ctt,'content_to_content': ctc,'content_to_attribute': cta}

# --- CLI ---
def main(argv=None):
    ap = argparse.ArgumentParser()
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument('--url')
    src.add_argument('--file')
    ap.add_argument('--out', required=True)
    ap.add_argument('--bearer')
    ap.add_argument('--basic-user')
    ap.add_argument('--basic-pass')
    ap.add_argument('--timeout', type=int, default=60)
    ap.add_argument('--insecure', action='store_true')
    ap.add_argument('--dam-base', help='Override base URL for DAM assets (e.g., https://publish-...adobeaemcloud.com)')
    ap.add_argument('--asset-meta-timeout', type=int, default=60, help='Timeout for fetching asset metadata JSON')
    args = ap.parse_args(argv)

    if args.url:
        headers = {'Accept': 'application/json'}
        token = args.bearer or os.getenv('AEM_BEARER_TOKEN')
        if token:
            headers['Authorization'] = f'Bearer {token}'
        elif args.basic_user and args.basic_pass:
            headers['Authorization'] = 'Basic ' + base64.b64encode(f"{args.basic_user}:{args.basic_pass}".encode()).decode('ascii')
        raw = http_get(args.url, headers, verify_ssl=not args.insecure, timeout=args.timeout)
        try:
            src = json.loads(raw)
        except json.JSONDecodeError:
            text = raw.strip().replace('\\n','').replace('\\/','/').replace('\\"','"')
            start = text.find('{')
            src = json.loads(text[start:] if start > 0 else text)
    else:
        with open(args.file, 'r', encoding='utf-8') as f:
            src = json.load(f)

    items = src.get('data', [])
    if not isinstance(items, list):
        raise ValueError("Input JSON does not contain a top-level 'data' array.")

    out = transform(items, link_lessons_to_assets=True, base_url=(args.dam_base or None), asset_meta_timeout=args.asset_meta_timeout, insecure=args.insecure)

    with open(args.out, 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2)
    print('OK', len(out['content']), len(out['content_to_text']), len(out['content_to_content']), len(out['content_to_attribute']))
    return 0

if __name__ == '__main__':
    raise SystemExit(main())