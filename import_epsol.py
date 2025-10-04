import asyncio
from typing import List, Optional, Tuple, Dict, Any
import argparse
import re

import httpx
from bs4 import BeautifulSoup

from database import async_session, init_db
from services import EquipmentService
from models import EquipmentCreate


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
}

BASE_URL = "https://epsol.ru"
CATALOG_URL = f"{BASE_URL}/katalog/"


def text_clean(s: Optional[str]) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip()


def absolute_url(href: str) -> Optional[str]:
    if not href:
        return None
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return BASE_URL + href
    return None


async def fetch_html(client: httpx.AsyncClient, url: str) -> Optional[str]:
    try:
        r = await client.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        return r.text
    except Exception:
        return None


TOP_CATEGORIES = [
    "Дозирующие насосы",
    "Дозировочные насосы",
    "Перистальтические насосы",
    "Анализаторы жидкости",
    "Системы дозирования и контроля",
    "Датчики и электроды",
    "Держатели датчиков",
    "Миксеры (мешалки)",
    "Импульсные расходомеры",
]


def parse_categories_and_subcats(html: str) -> List[Tuple[str, str, Optional[str]]]:
    """
    Returns list of tuples: (category, subcategory, subcategory_url)
    Heuristic parsing of the catalog page blocks.
    """
    soup = BeautifulSoup(html, "lxml")
    results: List[Tuple[str, str, Optional[str]]] = []

    # Strategy A: Walk DOM and collect links under known top-level categories
    current_cat = None
    for node in soup.find_all(["h1", "h2", "h3", "div", "section", "ul", "ol", "p", "a"]):
        # Update current category when encountering headings matching TOP_CATEGORIES
        if node.name in ("h1", "h2", "h3"):
            title = text_clean(node.get_text())
            if title in TOP_CATEGORIES:
                current_cat = title
            continue

        # Collect links as subcategories when inside a category context
        if current_cat and hasattr(node, "find_all"):
            for a in node.find_all("a", href=True):
                sub = text_clean(a.get_text())
                if not sub or len(sub) < 2:
                    continue
                href = absolute_url(a.get("href"))
                if not href or "/katalog/" not in href:
                    continue
                # Avoid using links that are the same as category root
                if sub == current_cat:
                    continue
                results.append((current_cat, sub, href))

    # Strategy B (fallback): any links under footer/sidebar lists labeled "Каталог"
    if not results:
        for header in soup.find_all(string=re.compile(r"Каталог", re.I)):
            container = header.parent if hasattr(header, "parent") else None
            if not container:
                continue
            for a in container.find_all("a", href=True):
                txt = text_clean(a.get_text())
                if txt in TOP_CATEGORIES:
                    current_cat = txt
                    continue
                href = absolute_url(a.get("href"))
                if current_cat and href and "/katalog/" in href and txt and txt != current_cat:
                    results.append((current_cat, txt, href))

    # Deduplicate
    seen = set()
    deduped: List[Tuple[str, str, Optional[str]]] = []
    for item in results:
        key = (item[0], item[1])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def extract_price_currency(text: str) -> Tuple[Optional[float], Optional[str]]:
    text = text_clean(text)
    m = re.search(r"(\d[\d\s\.,]{2,})\s*(руб|₽|р\.|RUB|USD|EUR)?", text, flags=re.IGNORECASE)
    if not m:
        return None, None
    num = m.group(1)
    try:
        price = float(num.replace(" ", "").replace("\u00A0", "").replace(",", "."))
    except Exception:
        price = None
    currency = None
    cur = m.group(2)
    if cur:
        u = cur.upper()
        if u in ["USD", "EUR", "RUB"]:
            currency = u
        elif "РУБ" in u or "Р." in u or "₽" in cur:
            currency = "RUB"
    return price, currency


def parse_product_page(html: str) -> Tuple[Optional[str], Optional[float], Optional[str], Optional[str], Optional[Dict[str, Any]]]:
    """
    Parse a product page for name, price, currency, description, specifications.
    Returns (name, price, currency, description, specifications_dict)
    """
    soup = BeautifulSoup(html, "lxml")
    # Name (prefer WooCommerce h1.entry-title)
    title_el = soup.select_one("h1.entry-title") or soup.select_one(".product_title, .product-title, .entry-title") or soup.find("h1")
    name = text_clean(title_el.get_text()) if title_el else None
    if name and name.lower().startswith("нужна консультация"):
        # clearly not a product name
        name = None

    # Price
    price_text = ""
    price_block = soup.select_one(".price, .product-price, .price-block") or soup.find(text=re.compile(r"руб|₽|USD|EUR", re.I))
    if price_block:
        price_text = price_block if isinstance(price_block, str) else price_block.get_text(" ")
    price, currency = extract_price_currency(price_text)

    # Description
    desc = None
    desc_el = soup.select_one(".woocommerce-product-details__short-description, .product-short-description, .entry-summary, .summary, .entry-content")
    if desc_el:
        desc = text_clean(desc_el.get_text(" "))
    else:
        # fallback to first paragraphs under main content
        p = soup.find("p")
        if p:
            desc = text_clean(p.get_text(" "))

    # Specifications: try to build a key-value dict from tables or definition lists
    specs: Dict[str, Any] = {}
    # Tables (prefer WooCommerce attributes tables)
    for table in soup.select("table.woocommerce-product-attributes, table.shop_attributes, table"): 
        for tr in table.find_all("tr"):
            tds = tr.find_all(["td", "th"])  # sometimes header + value
            if len(tds) >= 2:
                key = text_clean(tds[0].get_text())
                val = text_clean(tds[1].get_text())
                if key and val and len(key) <= 80:
                    specs[key] = val
    # Definition lists
    for dl in soup.find_all("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            key = text_clean(dt.get_text())
            val = text_clean(dd.get_text())
            if key and val and len(key) <= 80:
                specs[key] = val

    if not specs:
        specs = None

    return name, price, currency, desc, specs


def parse_products_from_subcat(html: str) -> List[Tuple[str, Optional[str]]]:
    """
    Returns list of (product_name, product_url) from a subcategory page.
    """
    soup = BeautifulSoup(html, "lxml")
    products: List[Tuple[str, Optional[str]]] = []

    # Product cards: common WooCommerce/WordPress patterns first
    selectors = [
        "ul.products li.product a.woocommerce-LoopProduct-link",
        "ul.products li.product a[href]",
        ".products .product a.woocommerce-LoopProduct-link",
        ".products .product a[href]",
        "a.woocommerce-LoopProduct-link",
        ".shop-container a[href]",
    ]

    links = []
    for sel in selectors:
        links.extend(soup.select(sel))

    # fallback: all anchors
    if not links:
        links = soup.select("a[href]")

    for a in links:
        href = a.get("href")
        url = absolute_url(href)
        if not url or not url.startswith(BASE_URL):
            continue
        # don't rely on link text as product name at listing stage; we'll parse inside page
        # Heuristic: product links often have slug-like path, exclude obvious category anchors
        if any(seg for seg in url.split("/") if len(seg) > 0 and "page" in seg.lower()):
            continue
        products.append(("", url))

    # Deduplicate by url
    seen = set()
    out: List[Tuple[str, Optional[str]]] = []
    for name, url in products:
        key = url
        if key in seen:
            continue
        seen.add(key)
        out.append((name, url))
    return out


async def import_catalog(start_urls: Optional[List[str]] = None) -> None:
    await init_db()
    async with async_session() as db, httpx.AsyncClient(follow_redirects=True) as client:
        service = EquipmentService(db)

        inserted = 0

        # If specific start URLs are provided, treat them as subcategory/listing pages
        if start_urls:
            for sub_url in start_urls:
                sub_html = await fetch_html(client, sub_url)
                if not sub_html:
                    continue
                # Try to infer category/subcategory from breadcrumbs/headings
                category = None
                subcategory = None
                soup = BeautifulSoup(sub_html, "lxml")
                crumbs = [text_clean(x.get_text()) for x in soup.select(".breadcrumbs a, .breadcrumb a")] or []
                if crumbs:
                    # Heuristic: last 2 items are (category, subcategory)
                    category = crumbs[-2] if len(crumbs) >= 2 else crumbs[-1]
                    subcategory = crumbs[-1]
                # Fallback to page title
                if not subcategory:
                    h1 = soup.find("h1")
                    subcategory = text_clean(h1.get_text()) if h1 else "Подкатегория"
                if not category:
                    category = next((c for c in TOP_CATEGORIES if c in subcategory), "Категория")

                # Try listing first
                product_links = parse_products_from_subcat(sub_html)
                print(f"Подкатегория: {subcategory} — найдено товаров (по ссылкам): {len(product_links)} — {sub_url}")

                # If listing not detected, try to parse page as a single product
                if not product_links:
                    n, p, c, d, s = parse_product_page(sub_html)
                    if n:
                        try:
                            data = EquipmentCreate(
                                name=n,
                                category=category or "Категория",
                                subcategory=subcategory or "Подкатегория",
                                description=d,
                                price=float(p) if p is not None else 0.0,
                                currency=c or "RUB",
                                brand=None,
                                model=None,
                                specifications=s,
                                availability=True,
                            )
                            await service.create_equipment(data)
                            inserted += 1
                            continue
                        except Exception:
                            pass
                for _, prod_url in product_links[:120]:  # higher limit for explicit runs
                    p_html = await fetch_html(client, prod_url)
                    if not p_html:
                        continue
                    name, price, currency, desc, specs = parse_product_page(p_html)
                    if not name:
                        continue
                    try:
                        data = EquipmentCreate(
                            name=name,
                            category=category,
                            subcategory=subcategory,
                            description=desc,
                            price=float(price) if price is not None else 0.0,
                            currency=currency or "RUB",
                            brand=None,
                            model=None,
                            specifications=specs,
                            availability=True,
                        )
                        await service.create_equipment(data)
                        inserted += 1
                    except Exception:
                        continue
        else:
            # Default: start from catalog page and walk down
            html = await fetch_html(client, CATALOG_URL)
            if not html:
                print("Не удалось загрузить каталог")
                return

            cat_sub_list = parse_categories_and_subcats(html)
            print(f"Найдено категорий/подкатегорий: {len(cat_sub_list)}")

            for category, subcategory, sub_url in cat_sub_list:
                if not sub_url:
                    continue
                sub_html = await fetch_html(client, sub_url)
                if not sub_html:
                    continue

                product_links = parse_products_from_subcat(sub_html)
                print(f"Подкатегория: {subcategory} — найдено товаров (по ссылкам): {len(product_links)} — {sub_url}")
                for _, prod_url in product_links[:80]:  # safety limit per subcategory
                    p_html = await fetch_html(client, prod_url)
                    if not p_html:
                        continue
                    name, price, currency, desc, specs = parse_product_page(p_html)
                    if not name:
                        continue
                    try:
                        data = EquipmentCreate(
                            name=name,
                            category=category,
                            subcategory=subcategory,
                            description=desc,
                            price=float(price) if price is not None else 0.0,
                            currency=currency or "RUB",
                            brand=None,
                            model=None,
                            specifications=specs,
                            availability=True,
                        )
                        await service.create_equipment(data)
                        inserted += 1
                    except Exception:
                        continue

        print(f"Импорт завершён. Добавлено записей: {inserted}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import EPSOL catalog")
    parser.add_argument("--start", nargs="*", help="Start URLs (subcategory/listing pages)")
    args = parser.parse_args()
    asyncio.run(import_catalog(start_urls=args.start))


