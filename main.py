#!/usr/bin/env python3
import argparse
import hashlib
import requests
from datetime import datetime, timedelta
from dateutil import parser as dtparser
from lxml import etree
from feedgen.feed import FeedGenerator

BASE_URL = "https://dzzb.jnkgjtdzzbgs.com"
KEY_WORDS = ["晋圣", "天安"]
MAX_DAY = 10          # 只留最近 N 天

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; JNKG-Bot/1.0)"
}


def get_stop_day(max_day=MAX_DAY):
    return datetime.now() - timedelta(days=max_day)


def fetch_one_page(column_path, page=1):
    """网络模式：返回该页 (rows, earliest_date or None)。遇到网络错误返回 ([], None)。"""
    if page == 1:
        url = f"{BASE_URL}/cms/default/webfile/{column_path}/index.html"
    else:
        url = f"{BASE_URL}/cms/default/webfile/{column_path}/index_{page}.html"

    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except requests.RequestException:
        return [], None

    r.encoding = "utf-8"
    html = etree.HTML(r.text)

    rows = []
    trs = html.xpath('//div[@class="list"]//tr')
    if len(trs) > 0:
        trs = trs[1:]
    else:
        trs = []

    earliest = None
    for tr in trs:
        tds = tr.xpath("./td")
        if len(tds) < 3:
            continue
        pub = tds[0].xpath("string(.)").strip()

        title_nodes = tds[1].xpath("./a/@title")
        title = title_nodes[0].strip() if title_nodes else tds[1].xpath("string(.)").strip()

        href_nodes = tds[1].xpath("./a/@href")
        if not href_nodes:
            continue
        rel_link = href_nodes[0]
        abs_link = BASE_URL + rel_link if rel_link.startswith("/") else rel_link

        try:
            pub_dt = dtparser.parse(pub)
        except Exception:
            pub_dt = datetime.now()

        if earliest is None or pub_dt < earliest:
            earliest = pub_dt

        rows.append({
            "pub_date": pub_dt,
            "title": title,
            "link": abs_link,
            "kind": column_path,
        })

    return rows, earliest


def fetch_one_page_from_html(html_text, column_path):
    """解析给定 HTML 并返回 (rows, earliest)。用于 dry-run 测试。"""
    html = etree.HTML(html_text)

    rows = []
    trs = html.xpath('//div[@class="list"]//tr')
    if len(trs) > 0:
        trs = trs[1:]
    else:
        trs = []

    earliest = None
    for tr in trs:
        tds = tr.xpath("./td")
        if len(tds) < 3:
            continue
        pub = tds[0].xpath("string(.)").strip()

        title_nodes = tds[1].xpath("./a/@title")
        title = title_nodes[0].strip() if title_nodes else tds[1].xpath("string(.)").strip()

        href_nodes = tds[1].xpath("./a/@href")
        if not href_nodes:
            continue
        rel_link = href_nodes[0]
        abs_link = BASE_URL + rel_link if rel_link.startswith("/") else rel_link

        try:
            pub_dt = dtparser.parse(pub)
        except Exception:
            pub_dt = datetime.now()

        if earliest is None or pub_dt < earliest:
            earliest = pub_dt

        rows.append({
            "pub_date": pub_dt,
            "title": title,
            "link": abs_link,
            "kind": column_path,
        })

    return rows, earliest


def fetch_column(column, max_pages=50, fetcher=None):
    """翻页直到超出最近 N 天或遇到空页/max_pages。

    返回值已按日期过滤，只包含 >= stop_day 的条目。
    """
    page = 1
    all_rows = []
    stop_day = get_stop_day()
    while page <= max_pages:
        if fetcher:
            rows, earliest = fetcher(column, page)
        else:
            rows, earliest = fetch_one_page(column, page)
        if not rows:
            break
        all_rows.extend(rows)
        if earliest and earliest < stop_day:
            break
        page += 1

    return [r for r in all_rows if r["pub_date"] >= stop_day]


def main():
    parser = argparse.ArgumentParser(description="生成晋能控股招标 RSS，可用 --dry-run 在本地示例 HTML 上测试解析。")
    parser.add_argument("--dry-run", action="store_true", help="使用内置示例 HTML 运行（不访问网络）")
    args = parser.parse_args()

    fg = FeedGenerator()
    fg.title("晋能控股-晋圣/天安 招标监控")
    fg.link(href=BASE_URL, rel="alternate")
    fg.description(f"近 {MAX_DAY} 天且含关键词{KEY_WORDS}")
    fg.language("zh-cn")

    total = 0
    kw_lower = [k.lower() for k in KEY_WORDS]

    fetcher = None
    out = "filtered.xml"
    if args.dry_run:
        # 构造示例 HTML（包含一条匹配关键词、一条不匹配）
        today = datetime.now().date()
        d0 = today.strftime("%Y-%m-%d")
        d5 = (today - timedelta(days=5)).strftime("%Y-%m-%d")
        sample_html = f"""
<div class="list">
<table>
  <tr><th>date</th><th>title</th><th>type</th></tr>
  <tr>
    <td>{d0}</td>
    <td><a href="/notice/1" title="关于 晋圣 项目招标">关于 晋圣 项目招标</a></td>
    <td>招标</td>
  </tr>
  <tr>
    <td>{d5}</td>
    <td><a href="/notice/2" title="普通 项目">普通 项目</a></td>
    <td>公告</td>
  </tr>
</table>
</div>
"""
        def make_dry_fetch(html_text):
            def _f(column, page):
                if page > 1:
                    return [], None
                return fetch_one_page_from_html(html_text, column)
            return _f

        fetcher = make_dry_fetch(sample_html)
        out = "filtered_dry.xml"

    for col in ["1ywgg1", "2ywgg1", "3ywgg1"]:
        rows = fetch_column(col, fetcher=fetcher)
        for r in rows:
            if not any(k in r["title"].lower() for k in kw_lower):
                continue
            fe = fg.add_entry()
            fe.title(f"[{col}]{r['title']}")
            fe.link(href=r["link"])
            fe.description(r["title"])
            fe.guid(hashlib.md5(r["link"].encode()).hexdigest(), permalink=False)
            dt = r["pub_date"]
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.now().astimezone().tzinfo)
            fe.pubDate(dt)
            total += 1

    fg.rss_file(out)
    print(f"[{datetime.now():%F %T}] 输出 {out} 共 {total} 条 (dry-run={args.dry_run})")


if __name__ == "__main__":
    main()
