CREATE OR REPLACE FUNCTION dataplatform_public_published_dev.datasteward.clear_str(str STRING)
RETURNS STRING
RETURN 
  REGEXP_REPLACE(str, '^[\\u0009\\u0020\\u3000]+|[\\u0009\\u0020\\u3000]+$', '');


CREATE OR REPLACE FUNCTION dataplatform_public_published_dev.datasteward.normalize_text(prefix_text STRING, text STRING)
RETURNS STRING
RETURN CONCAT(prefix_text, REGEXP_REPLACE(dataplatform_public_published_dev.datasteward.clear_str(text), '(?<!  )\\r?\\n', CONCAT('  ', CHR(10))), "  \n");


CREATE OR REPLACE FUNCTION dataplatform_public_published_dev.datasteward.normalize_url(prefix_text STRING, link_text STRING, url STRING)
RETURNS STRING
RETURN CONCAT(prefix_text, "[", link_text, "](", dataplatform_public_published_dev.datasteward.clear_str(url), ")  \n");


CREATE OR REPLACE FUNCTION dataplatform_public_published_dev.datasteward.cron_to_japanese(cron_expr STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
import re

WEEK_EN = ["SUN","MON","TUE","WED","THU","FRI","SAT"]
WEEK_JA = ["日","月","火","水","木","金","土"]
WEEK_TO_NUM = {name:i for i, name in enumerate(WEEK_EN)}
MONTH_EN = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]
MONTH_TO_NUM = {name:i+1 for i, name in enumerate(MONTH_EN)}

def _pad2(n:int) -> str:
    return f"{int(n):02d}"


# ===== 前処理 & 正規化（5フィールドへ） =====
def _unwrap(expr: str) -> str:
    s = expr.strip().replace("\u3000", " ")
    m = re.fullmatch(r"(?is)\s*cron\(\s*(.*?)\s*\)\s*", s)
    if m:
        s = m.group(1)
    return re.sub(r"\s+", " ", s).strip()


def _normalize_to_5(expr: str):
    """
    5 / 6 / 7 フィールドを 5フィールド (min hour dom mon dow) に正規化。
    - 6フィールド: 末尾が年なら落とす（AWS）/ 先頭が秒なら落とす（Quartz）
    - 7フィールド: 先頭の秒と末尾の年を落とす（Quartz）
    - '?' は '*' として扱う
    返り値: (minute, hour, dom, mon, dow, year_or_none, sec_or_none)
    """
    s = _unwrap(expr)
    parts = s.split()
    parts = [("*" if p == "?" else p).upper() for p in parts]

    if len(parts) == 5:
        sec, year = None, None
        minute, hour, dom, mon, dow = parts
    elif len(parts) == 6:
        # 年の判定（*, 2025, 2020-2030, */2, 2020/2, 2020,2022,... を許容）
        last = parts[-1]
        is_yearish = re.fullmatch(r"(?:\*|\*/\d+|\d{4}(?:-\d{4})?(?:/\d+)?(?:,\d{4})*)", last) is not None
        if is_yearish:
            sec = None
            minute, hour, dom, mon, dow, year = parts
        else:
            sec, minute, hour, dom, mon, dow = parts
            year = None
    elif len(parts) == 7:
        sec, minute, hour, dom, mon, dow, year = parts
    else:
        raise ValueError(f"未対応のフィールド数: {len(parts)} -> {parts}")

    return (minute, hour, dom, mon, dow, year, sec)


# ===== ユーティリティ（数値/英名処理） =====
def _num_or_name_to_num(tok: str, kind: str) -> int:
    t = tok.strip().upper()
    if kind == "dow":
        if t in WEEK_TO_NUM: return WEEK_TO_NUM[t]
        if t == "7": return 0  # 0/7=Sun
    if kind == "mon":
        if t in MONTH_TO_NUM: return MONTH_TO_NUM[t]
    return int(t)


def _piece_list(expr: str) -> list[str]:
    return [p for p in expr.split(",")]


def _range_to_jp(a: str, b: str, unit: str, kind: str|None=None) -> str:
    if kind == "dow":
        a_n = _num_or_name_to_num(a, "dow")
        b_n = _num_or_name_to_num(b, "dow")
        return f"{WEEK_JA[a_n]}〜{WEEK_JA[b_n]}"
    if kind == "mon":
        a_n = _num_or_name_to_num(a, "mon")
        b_n = _num_or_name_to_num(b, "mon")
        return f"{a_n}月〜{b_n}月"
    return f"{int(a)}〜{int(b)}{unit}"


def _step_to_jp(base: str, step: str, unit: str) -> str:
    if base == "*":
        return f"{int(step)}{unit}ごと"
    if "-" in base:
        a, b = base.split("-")
        return f"{int(a)}〜{int(b)}{unit}の{int(step)}{unit}間隔"
    return f"{int(base)}{unit}から{int(step)}{unit}ごと"


def _list_to_jp(pieces: list[str], unit: str, kind: str|None=None) -> str:
    out = []
    for p in pieces:
        if "/" in p:
            base, st = p.split("/")
            out.append(_step_to_jp(base, st, unit))
        elif "-" in p:
            a, b = p.split("-")
            out.append(_range_to_jp(a, b, unit, kind))
        elif kind == "dow":
            n = _num_or_name_to_num(p, "dow")
            out.append(WEEK_JA[n])
        elif kind == "mon":
            n = _num_or_name_to_num(p, "mon")
            out.append(f"{n}月")
        else:
            out.append(f"{int(p)}{unit}")
    return "・".join(out)


# ===== DOM / DOW / MON 日本語化（Quartzの主要拡張に対応） =====
def _dom_to_jp(dom: str) -> str:
    dom = dom.upper()
    if dom == "*": return "毎日"
    if dom == "L": return "毎月の月末日"
    if dom in ("LW", "WL"): return "毎月の月末平日"
    m = re.fullmatch(r"(\d+)W", dom)
    if m: return f"毎月{int(m.group(1))}日に最も近い平日"

    if "," in dom: return f"毎月{_list_to_jp(_piece_list(dom), '日')}"
    if "/" in dom:
        base, st = dom.split("/")
        if base == "*":
            base_jp = "1〜31日"
        elif "-" in base:
            a, b = base.split("-")
            base_jp = f"{int(a)}〜{int(b)}日"
        else:
            base_jp = f"{int(base)}日"
        return f"毎月{base_jp}の{int(st)}日おき"
    if "-" in dom:
        a, b = dom.split("-")
        return f"毎月{int(a)}〜{int(b)}日"
    return f"毎月{int(dom)}日"


def _dow_to_jp(dow: str) -> str:
    d = dow.upper()
    if d == "*": return "毎週すべての曜日"
    if d in ("1-5","MON-FRI"): return "平日"
    if d in ("0,6","6,0","SUN,SAT","SAT,SUN"): return "土日"

    m = re.fullmatch(r"([0-7A-Z]{3})L", d)
    if m:
        n = _num_or_name_to_num(m.group(1), "dow")
        return f"毎月の最後の{WEEK_JA[n]}曜日"
    m = re.fullmatch(r"([0-7A-Z]{3})#([1-5])", d)
    if m:
        n = _num_or_name_to_num(m.group(1), "dow")
        k = int(m.group(2))
        return f"毎月第{k}{WEEK_JA[n]}曜日"
    if d == "L":
        return "毎月の最後の週（Quartz L）"

    if "," in d: return "毎週" + _list_to_jp(_piece_list(d), "曜", kind="dow")
    if "/" in d:
        base, st = d.split("/")
        if base == "*":
            base_jp = "全曜日"
        elif base in ("1-5", "MON-FRI"):
            base_jp = "平日"
        elif "-" in base:
            base_jp = _range_to_jp(*base.split("-"), "曜", "dow")
        else:
            base_jp = WEEK_JA[_num_or_name_to_num(base, "dow")]
        return f"{int(st)}週間ごと {base_jp}"
    if "-" in d:
        a, b = d.split("-")
        return "毎週" + _range_to_jp(a, b, "曜", "dow")
    n = _num_or_name_to_num(d, "dow")
    return f"毎週{WEEK_JA[n]}曜日"


def _mon_to_jp(mon: str) -> str:
    m = mon.upper()
    if m == "*": return "毎年"
    if "," in m: return "毎年" + _list_to_jp(_piece_list(m), "", kind="mon")
    if "/" in m:
        base, st = m.split("/")
        if base == "*": return f"{int(st)}か月ごと"
        if "-" in base:
            a, b = base.split("-")
            return f"{_range_to_jp(a, b, '月', 'mon')}の{int(st)}か月ごと"
        return f"{_num_or_name_to_num(base,'mon')}月から{int(st)}か月ごと"
    if "-" in m:
        a, b = m.split("-")
        return _range_to_jp(a, b, "月", "mon")
    return f"毎年{_num_or_name_to_num(m,'mon')}月"


def _year_to_jp(year: str|None) -> str:
    if not year or year == "*": return ""
    y = year.upper()
    if "," in y:
        ys = [int(x) for x in y.split(",")]
        return "（対象年: " + "・".join(f"{v}年" for v in ys) + "）"
    if "/" in y:
        base, st = y.split("/")
        if base == "*": return f"（対象年: {int(st)}年ごと）"
        if "-" in base:
            a, b = base.split("-")
            return f"（対象年: {int(a)}〜{int(b)}年の{int(st)}年ごと）"
        return f"（対象年: {int(base)}年から{int(st)}年ごと）"
    if "-" in y:
        a, b = y.split("-")
        return f"（対象年: {int(a)}〜{int(b)}年）"
    return f"（対象年: {int(y)}年）"


# ===== 分・時 を 1日の時刻例（HH:MM, …）へ =====
def _expand_num_field(expr: str, minv: int, maxv: int, field_name: str) -> list[int]:
    vals: set[int] = set()
    for tok in expr.split(","):
        tok = tok.strip()
        if tok == "*":
            vals.update(range(minv, maxv + 1))
            continue
        if "/" in tok:
            base, step_str = tok.split("/", 1)
            if not step_str.isdigit(): raise ValueError(f"{field_name}: ステップが数値ではありません: {tok}")
            step = int(step_str)
            if step <= 0: raise ValueError(f"{field_name}: ステップは1以上にしてください: {tok}")
            if base == "*":
                start, end = minv, maxv
            elif "-" in base:
                a, b = map(int, base.split("-", 1))
                if a > b: raise ValueError(f"{field_name}: レンジが逆順です: {tok}")
                if a < minv or b > maxv: raise ValueError(f"{field_name}: レンジが範囲外です: {tok}")
                start, end = a, b
            else:
                a = int(base)
                if a < minv or a > maxv: raise ValueError(f"{field_name}: 値が範囲外です: {tok}")
                start, end = a, maxv
            v = start
            while v <= end:
                vals.add(v)
                v += step
            continue
        if "-" in tok:
            a, b = map(int, tok.split("-", 1))
            if a > b: raise ValueError(f"{field_name}: レンジが逆順です: {tok}")
            if a < minv or b > maxv: raise ValueError(f"{field_name}: レンジが範囲外です: {tok}")
            vals.update(range(a, b + 1))
            continue
        v = int(tok)
        if v < minv or v > maxv: raise ValueError(f"{field_name}: 値が範囲外です: {tok}")
        vals.add(v)
    if not vals:
        raise ValueError(f"{field_name}: 値が解決できません: {expr!r}")
    return sorted(vals)


def _times_examples(min_expr: str, hour_expr: str, max_items: int = 48) -> str:
    """分・時の式から 1日の実行時刻表示を作る。
    - 簡潔レンジ表現（◯時〜◯時 / HH:MM〜HH:MM）
    - 分が */N かつ 時が A-B → 「N分ごと（A時〜B時）」
    - それ以外は HH:MM を列挙（多い場合 … で省略）
    """
    def is_single_num(s: str) -> bool:
        return s.isdigit()

    def is_simple_range(s: str) -> bool:
        if "," in s or "/" in s: return False
        if "-" not in s: return False
        a, b = s.split("-", 1)
        return a.isdigit() and b.isdigit()

    def is_step_all(s: str) -> tuple[bool, int|None]:
        # "*/N" → (True, N) / それ以外 → (False, None)
        if s.startswith("*/"):
            t = s[2:]
            if t.isdigit():
                n = int(t)
                return (n >= 1, n if n >= 1 else None)
        return (False, None)

    # 分が */N、時が単純レンジ A-B → 「N分ごと（A時〜B時）」
    step_min, n = is_step_all(min_expr)
    if step_min and is_simple_range(hour_expr):
        a, b = map(int, hour_expr.split("-", 1))
        if 0 <= a <= 23 and 0 <= b <= 23 and a <= b:
            return f"{n}分ごと（{a}時〜{b}時）"

    # 簡潔レンジ（時レンジ＋分* or 分単数）
    if is_simple_range(hour_expr) and (min_expr == "*" or is_single_num(min_expr)):
        a, b = map(int, hour_expr.split("-", 1))
        if 0 <= a <= 23 and 0 <= b <= 23 and a <= b:
            if min_expr == "*":
                return f"{a}時〜{b}時"
            else:
                m = int(min_expr)
                if 0 <= m <= 59:
                    return f"{_pad2(a)}:{_pad2(m)}〜{_pad2(b)}:{_pad2(m)}"

    # 簡潔レンジ（時単数＋分レンジ）
    if is_single_num(hour_expr) and is_simple_range(min_expr):
        h = int(hour_expr)
        a, b = map(int, min_expr.split("-", 1))
        if 0 <= h <= 23 and 0 <= a <= 59 and 0 <= b <= 59 and a <= b:
            return f"{_pad2(h)}:{_pad2(a)}〜{_pad2(h)}:{_pad2(b)}"

    # 列挙フォールバック（厳密展開＋ソート）
    hours = _expand_num_field(hour_expr, 0, 23, "時")
    mins  = _expand_num_field(min_expr,  0, 59, "分")
    times = [f"{_pad2(h)}:{_pad2(m)}" for h in hours for m in mins]
    times.sort()
    return ",".join(times[:max_items]) + (",..." if len(times) > max_items else "")


def _cadence_label(min_expr: str, hour_expr: str, base_mid: str) -> str:
    """分・時の形から見出し（毎日/毎時/毎分/毎日X時から毎分）を上書き調整。"""
    # ベースが「毎日」のときだけ上書き（平日/毎週◯曜日 などはそのまま）
    if base_mid == "毎日":
        # 例: "0 * ? * * *" → 分=固定, 時=*
        if hour_expr == "*" and min_expr.isdigit():
            return "毎日毎時"
        # 例: "* 9 ? * * *" / "* 9 ? * * 2025" → 分=*, 時=固定
        if min_expr == "*" and hour_expr.isdigit():
            return f"毎日{int(hour_expr)}時に毎分"
    return base_mid


def cron_to_japanese(expr: str, max_times: int = 5) -> str:
    """
    5/6/7フィールド対応
    分・時は HH:MM,HH:MM,... を列挙（多い場合は ... で省略）
    日/月/曜日は日本語に整形
    JST想定（時差計算なし）
    """
    normalized_expr = dataplatform_public_published_dev.datasteward.clear_str(expr)
    if normalized_expr == "":
      return ""
    minute, hour, dom, mon, dow, year, sec = _normalize_to_5(normalized_expr)

    # 月・日・曜日（DOMとDOWが同時指定なら OR 表現）
    mon_jp = _mon_to_jp(mon)
    dom_used, dow_used = (dom != "*"), (dow != "*")
    if dom_used and dow_used:
        mid = f"{_dom_to_jp(dom).replace('毎月','')} または {_dow_to_jp(dow)}"
    elif dom_used:
        mid = _dom_to_jp(dom)
    elif dow_used:
        mid = _dow_to_jp(dow)
    else:
        mid = "毎日"
    mid = _cadence_label(minute, hour, mid) 

    prefix = "" if mon_jp == "毎年" else (mon_jp + " ")
    times_example = _times_examples(minute, hour, max_items=max_times)
    year_note = _year_to_jp(year)

    example_prefix = ""
    if mid == "毎時" and hour == "*" and minute.isdigit():
        example_prefix = "例："

    return f"{prefix}{mid} {example_prefix}{times_example}{year_note}（JST）"
$$;


CREATE OR REPLACE FUNCTION dataplatform_public_published_dev.datasteward.generate_description_schema(name_ja STRING, explanation STRING)
RETURNS STRING
RETURN
  CONCAT(
    CONCAT("データソース名/システム名：", dataplatform_public_published_dev.datasteward.clear_str(name_ja), "  \n"),
    CONCAT("Overview：このスキーマに含まれるデータは、主に上記のシステムから取り込まれています。", "  \n"),
    CASE
      WHEN dataplatform_public_published_dev.datasteward.clear_str(explanation) != ""
      THEN dataplatform_public_published_dev.datasteward.normalize_text("データソース概要：", explanation)
      ELSE ""
    END,
    "連絡先：[#sys-データ基盤の相談や情報共有](https://www.youtube.com/)"
  );


CREATE OR REPLACE FUNCTION dataplatform_public_published_dev.datasteward.generate_description_table(name_ja STRING, explanation STRING, type_conversion STRING, rule STRING, cron_schedule STRING, query STRING, reference STRING, link STRING)
RETURNS STRING
RETURN
  CONCAT(
    CONCAT("TableName：", dataplatform_public_published_dev.datasteward.clear_str(name_ja), "  \n"),
    "Overview：  \n",
    CASE
      WHEN dataplatform_public_published_dev.datasteward.clear_str(explanation) != ""
      THEN dataplatform_public_published_dev.datasteward.normalize_text("", explanation)
      ELSE ""
    END,
    CASE
      WHEN dataplatform_public_published_dev.datasteward.clear_str(type_conversion) != ""
      THEN dataplatform_public_published_dev.datasteward.normalize_text("", type_conversion)
      ELSE ""
    END,
    CASE
      WHEN dataplatform_public_published_dev.datasteward.clear_str(rule) != ""
      THEN dataplatform_public_published_dev.datasteward.normalize_text("", rule)
      ELSE ""
    END,
    CASE
      WHEN dataplatform_public_published_dev.datasteward.clear_str(cron_schedule) != ""
      THEN CONCAT("更新頻度：", dataplatform_public_published_dev.datasteward.cron_to_japanese(cron_schedule), "  \n")
      ELSE ""
    END,
    CASE
      WHEN dataplatform_public_published_dev.datasteward.clear_str(link) != ""
      THEN dataplatform_public_published_dev.datasteward.normalize_url("元テーブル仕様書：", "link", link)
      ELSE ""
    END,
    CASE
      WHEN dataplatform_public_published_dev.datasteward.clear_str(query) != ""
      THEN dataplatform_public_published_dev.datasteward.normalize_url("作成クエリ：", "query", query)
      ELSE ""
    END,
    CASE
      WHEN dataplatform_public_published_dev.datasteward.clear_str(reference) != ""
      THEN dataplatform_public_published_dev.datasteward.normalize_url("対応チケット：", "reference", reference)
      ELSE ""
    END,
    "連絡先：[#sys-データ基盤の相談や情報共有](https://www.youtube.com/)"
  );


CREATE OR REPLACE FUNCTION dataplatform_public_published_dev.datasteward.generate_description_view(name_ja STRING, cron_schedule STRING, reference STRING)
RETURNS STRING
RETURN
  CONCAT(
    CONCAT("TableName：", dataplatform_public_published_dev.datasteward.clear_str(name_ja), "  \n"),
    "Overview：元テーブル名から必要なカラムを取り出したビュー。詳細を確認したい場合は、元テーブルを参照してください。  \n",
    CASE
      WHEN dataplatform_public_published_dev.datasteward.clear_str(cron_schedule) != ""
      THEN CONCAT("更新頻度：", dataplatform_public_published_dev.datasteward.cron_to_japanese(cron_schedule), "  \n")
      ELSE ""
    END,
    CASE
      WHEN dataplatform_public_published_dev.datasteward.clear_str(reference) != ""
      THEN dataplatform_public_published_dev.datasteward.normalize_url("対応チケット：", "reference", reference)
      ELSE ""
    END,
    "連絡先：[#sys-データ基盤の相談や情報共有](https://www.youtube.com/)"
  );

