"""Lazada scraper.

This module contains `LazadaScraper`, a Selenium-based crawler tailored for Lazada Vietnam.

Two main responsibilities:
1) Collect product URLs from category pages (and persist URL metadata like `sold`).
2) Extract rich product information from each product page using `window.__moduleData__`
    (a more stable, structured source than parsing HTML).
"""

import json
import logging
import os
import time
import random
import re
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup
from pydash import result
from selenium.common import TimeoutException, ElementClickInterceptedException, NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait
from common_scraper3 import CommonScraper
from collections import Counter
logger = logging.getLogger(__name__)


class LazadaScraper(CommonScraper):
    """Scraper implementation for Lazada.

    Inherits:
        CommonScraper: provides Selenium driver lifecycle and shared helpers.

    Notes:
        - URL collection writes both a legacy `url.txt` (one URL per line) and a
          richer `url.ndjson` (one JSON object per line) containing `category`,
          `url`, and `sold`.
        - Product extraction uses `window.__moduleData__` (see `_crawl_product_moduledata`).
    """

    def __init__(self, num_page_to_scrape=10, data_dir='./data/lazada', wait_timeout=5, retry_num=3,
                 restart_num=20, is_headless=False, consumer_id=None):
        """Initialize the Lazada scraper.

        Args:
            num_page_to_scrape: Maximum number of pages to scrape per category.
            data_dir: Root directory where category folders and output files are stored.
            wait_timeout: Selenium explicit-wait timeout in seconds.
            retry_num: Number of retries for transient failures (timeouts, stale elements, etc.).
            restart_num: Reserved for driver restart policy (managed by `CommonScraper`).
            is_headless: Whether to run Chrome in headless mode.
            consumer_id: Kafka client id (only relevant if Kafka is enabled in `CommonScraper`).
        """
        if not os.path.exists(data_dir):
            os.mkdir(data_dir)
        main_page = 'https://www.lazada.vn/'
        super().__init__(num_page_to_scrape, data_dir, wait_timeout, retry_num, restart_num, is_headless, main_page,
                         'lazada_info', 'lazada_url', 'lazada_scraper', consumer_id)
        # Count how many items (URLs) we've processed since last restart.
        self._items_since_restart = 0

    def human_sleep(self, a=1.0, b=2.5):
        """Sleep for a random duration to mimic human browsing behavior.

        Args:
            a: Minimum sleep duration in seconds.
            b: Maximum sleep duration in seconds.
        """
        time.sleep(random.uniform(a, b))

    def get_product_urls(self):
        """Collect product URLs for every category configured under `data_dir`.

        For each category folder that contains `base_url.txt` (JSON), this method:
        - Opens the category URL.
        - Paginates up to `num_page_to_scrape` pages (or until last page).
        - Extracts product detail-page URLs.
        - Extracts a best-effort `sold` text on the listing card.
        - Persists:
          - `url.txt`: one URL per line (legacy format).
          - `url.ndjson`: one JSON object per line containing `category`, `url`, `sold`.

        Side Effects:
            Writes to files under `<data_dir>/<category>/`.
        """
        # go through all categories
        for root, dirs, files in os.walk(self.data_dir):
            if 'url.ndjson' in files:
                logger.info(f"Category already scraped, skip: {root}")
                continue
            if 'base_url.txt' in files:
                with open(os.path.join(root, 'base_url.txt')) as f:
                    d = json.load(f)
                category = d['category']
                category_url = d['url']

                # start scraping
                logger.info("Scraping category: " + category)
                output_dir = os.path.join(self.data_dir, category)
                if not os.path.exists(output_dir):
                    # os.mkdir(output_dir)
                    raise Exception("Category directory does not exist: " + output_dir)
                
                logger.info(f"Opening category page: {category_url}")
                self.human_sleep(2, 4)  # delay trước khi mở trang
                self.driver.get(category_url)
                self.human_sleep(2, 3)  # đợi trang load sơ bộ

                # scrape products link
                counter = 0
                while True:
                    has_product = False
                    # wait for products to be available, if not then check for popup
                    for i in range(self.retry_num):
                        try:
                            logger.info("Checking if the products are available on the page")
                            WebDriverWait(self.driver, self.wait_timeout).until(
                                ec.visibility_of_element_located((By.CLASS_NAME, "Bm3ON")))
                            has_product = True
                            break
                        except TimeoutException:
                            logger.info("Can't find the products after " + str(self.wait_timeout) + " seconds")
                            self.check_popup()
                            logger.info("Finished checking for popup")
                            self.driver.refresh()
                            logger.info("Refreshed the page")
                    # stop scraping this category if there's no product
                    if not has_product:
                        logger.info("No products are available from the category: " + category + ", stop scraping")
                        break
                    counter += 1
                    
                    self.human_sleep(0.5, 1.0)  # delay ngắn trước khi lấy thông tin trang
                    
                    curr_page_num = self.driver.find_element(By.CLASS_NAME, 'ant-pagination-item-active').get_attribute(
                        'title')
                    logger.info('Current page ' + str(curr_page_num))
                    
                    # Cuộn trang từ từ giống người dùng
                    for i in range(8):
                        self.driver.execute_script("window.scrollBy(0,400)")
                        time.sleep(random.uniform(0.1, 0.3))
                    
                    self.human_sleep(1.0, 2.0)  # đợi DOM tải xong
                    
                    soup = BeautifulSoup(self.driver.page_source, features="lxml")
                    products = soup.find_all(class_='Bm3ON')
                    logger.info('Number of products in page: ' + str(len(products)))
                    for product in products:
                        url = product.find('a')['href']
                        url = 'https:' + url
                        sold_tag = product.select_one('span._1cEkb span')
                        text = None
                        if sold_tag:
                            text = sold_tag.get_text(strip=True)

                        d = {
                            'category': category,
                            'url': url,
                            'sold': text
                        }
                        
                        # Lưu file
                        self.write_to_file(json.dumps(d, ensure_ascii=False), os.path.join(category, 'url.ndjson'))
                        # self.send_to_kafka(d, 'url')
                    logger.info("Finished scraping urls from page " + str(counter))
                    
                    self.human_sleep(1.5, 2.5)  

                    next_page_button_element = (By.CSS_SELECTOR, ".ant-pagination-next > button:nth-child(1)")
                    next_page_button = self.driver.find_element(*next_page_button_element)
                    is_last_page = not next_page_button.is_enabled()
                    if counter == self.num_page_to_scrape:
                        logger.info(f'Reached maximum number of pages to scrape in category: {category}')
                        break
                    elif is_last_page:
                        logger.info(f'Reached the last page of category: {category}')
                        break
                    
                    for attempt in range(3):
                        try:
                            # đưa nút vào giữa màn hình để tránh banner che
                            self.driver.execute_script(
                                "arguments[0].scrollIntoView({block:'center', inline:'center'});",
                                next_page_button
                            )
                            WebDriverWait(self.driver, self.wait_timeout).until(ec.element_to_be_clickable(next_page_button_element))
                            next_page_button.click()
                            logger.info("Clicked next page")
                            self.human_sleep(2.5, 4.0)
                            break
                        except ElementClickInterceptedException:
                            # mô phỏng “scroll xuống 1 tí rồi click lại”
                            self.driver.execute_script("window.scrollBy(0, 250);")
                            self.human_sleep(0.3, 0.7)
                            next_page_button = self.driver.find_element(*next_page_button_element)
                    else:
                        raise ElementClickInterceptedException("After several attempts, cannot click the next page button because it is being obscured by another element. In lazada, it may be \"Tin nhắn\"")

    # ==============================
    # PRODUCT INFO (moduleData-based)
    # ==============================
    def _scroll_page_natural(self, down_to: Optional[int] = None):
        """Scroll the current page gradually to trigger lazy-loaded content.

        Args:
            down_to: Optional absolute scroll height in pixels. If None, scrolls to
                the bottom of the document.
        """
        if down_to is None:
            total_height = int(self.driver.execute_script("return document.body.scrollHeight"))
        else:
            total_height = down_to

        for i in range(1, max(total_height, 1600), 450):
            self.driver.execute_script(f"window.scrollTo(0, {i});")
            time.sleep(random.uniform(0.06, 0.18))
        time.sleep(0.9)

    def _safe_get_moduledata(self) -> Dict[str, Any]:
        """Return Lazada's page state object `window.__moduleData__`.

        Returns:
            A dictionary-like object containing structured page data.

        Raises:
            RuntimeError: If `window.__moduleData__` is missing/empty.
        """
        raw = self.driver.execute_script("return window.__moduleData__;")
        if not raw:
            raise RuntimeError("Không lấy được window.__moduleData__ (có thể bị chặn/đổi cấu trúc).")
        return raw

    @staticmethod
    def _digits_only(s: str) -> str:
        """Extract only digits from a string.

        Args:
            s: Input string.

        Returns:
            A string containing only digits found in `s`.
        """
        m = re.findall(r"\d+", s or "")
        return "".join(m) if m else ""

    @staticmethod
    def _build_prop_lookup(properties: List[Dict[str, Any]]) -> Dict[str, str]:
        """Build a lookup mapping of Lazada property keys to human-readable names.

        Lazada variant properties are represented as pid/vid pairs.

        Args:
            properties: List of property dicts from module data.

        Returns:
            Dict mapping "<pid>:<vid>" -> display name.
        """
        lookup: Dict[str, str] = {}
        for prop in properties or []:
            pid = str(prop.get("pid"))
            for val in prop.get("values", []) or []:
                vid = str(val.get("vid"))
                name = val.get("name")
                if name:
                    lookup[f"{pid}:{vid}"] = name
        return lookup

    def _build_sku_map(self, skus_list: List[Dict[str, Any]]) -> Dict[str, str]:
        """Build map from skuId to property path.

        Args:
            skus_list: SKU list from module data (`productOption.skuBase.skus`).

        Returns:
            Dict mapping skuId (and its digit-only form) -> propPath/skuPropPath.
        """
        sku_map: Dict[str, str] = {}
        for item in skus_list or []:
            sku_id = str(item.get("skuId") or "").strip()
            prop_path = item.get("propPath") or item.get("skuPropPath") or ""
            if sku_id:
                sku_map[sku_id] = str(prop_path)
                dkey = self._digits_only(sku_id)
                if dkey and dkey not in sku_map:
                    sku_map[dkey] = str(prop_path)
        return sku_map

    @staticmethod
    def _parse_variant_name(prop_path: str, prop_lookup: Dict[str, str]) -> str:
        """Convert a Lazada propPath into a readable variant name.

        Args:
            prop_path: A semicolon-delimited string like "pid:vid;pid2:vid2".
            prop_lookup: Lookup mapping "pid:vid" to readable value names.

        Returns:
            A readable variant label, or "Mặc định" if not available.
        """
        if not prop_path:
            return "Mặc định"
        parts: List[str] = []
        for part in str(prop_path).split(";"):
            part = part.strip()
            if not part:
                continue

            if part in prop_lookup:
                parts.append(prop_lookup[part])
                continue

            if ":" in part:
                pid, vid = part.split(":", 1)
                key = f"{pid}:{vid}"
                if key in prop_lookup:
                    parts.append(prop_lookup[key])
                else:
                    parts.append(vid)
            else:
                parts.append(part)

        return " - ".join(parts) if parts else "Mặc định"

    @staticmethod
    def _extract_price_text(info: Dict[str, Any]) -> str:
        """Extract price text from a skuInfo structure.

        Args:
            info: SKU info dict from `fields.skuInfos[*]`.

        Returns:
            Price string if found; otherwise "---".
        """
        for fn in [
            lambda x: x.get("price", {}).get("salePrice", {}).get("text"),
            lambda x: x.get("price", {}).get("promotionPrice", {}).get("text"),
            lambda x: x.get("price", {}).get("originalPrice", {}).get("text"),
            lambda x: x.get("price", {}).get("text"),
        ]:
            try:
                v = fn(info)
                if v:
                    return str(v)
            except Exception:
                pass
        return "---"

    @staticmethod
    def _extract_stock(info: Dict[str, Any]) -> int:
        """Extract stock quantity from a skuInfo structure.

        Args:
            info: SKU info dict from `fields.skuInfos[*]`.

        Returns:
            Stock quantity as int; returns 0 if not found.
        """
        for fn in [
            lambda x: x.get("quantity", {}).get("limit", {}).get("max"),
            lambda x: x.get("quantity", {}).get("max"),
            lambda x: x.get("stock"),
            lambda x: x.get("inventory"),
        ]:
            try:
                v = fn(info)
                if isinstance(v, (int, float)):
                    return int(v)
                if isinstance(v, str) and v.isdigit():
                    return int(v)
            except Exception:
                pass
        return 0


    def _extract_highlights_and_description_json(self, raw: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], str]:
        """Extract highlights/description from Lazada moduleData (JSON-only).

        Lazada VN để sẵn nội dung ở `root.fields.product`:
        - `product.desc`: HTML mô tả sản phẩm
        - `product.highlights`: HTML highlights

        Args:
            raw: module data (`window.__moduleData__`).

        Returns:
            A tuple (highlights, description, source_tag).
        """
        fields = raw.get("data", {}).get("root", {}).get("fields", {}) or {}
        product = fields.get("product") or {}

        if not isinstance(product, dict):
            return None, None

        highlights = product.get("highlights")
        if not isinstance(highlights, str) or not highlights.strip():
            highlights = None

        description = product.get("desc")
        if not isinstance(description, str) or not description.strip():
            description = None

        if highlights or description:
            return highlights, description

        return None, None

    def _read_url_items_from_file(self, url_file_path: str, default_category: str) -> List[Dict[str, Any]]:
        """
        Read URL items from an NDJSON file.

        Each line must be a JSON object, e.g.:
        {"category": "...", "url": "...", "sold": "..."}

        Returns:
        List of dict items: {"category": str, "url": str, "sold": Any}
        """
        items: List[Dict[str, Any]] = []
        with open(url_file_path, "r", encoding="utf-8", errors="replace") as f:
            for lineno, raw in enumerate(f, start=1):
                line = raw.strip()
                if not line:
                    continue

                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning("Invalid NDJSON at %s:%d: %r", url_file_path, lineno, line)
                    continue

                if not isinstance(obj, dict):
                    logger.warning("NDJSON must be an object at %s:%d: %r", url_file_path, lineno, obj)
                    continue

                url = obj.get("url")
                if not isinstance(url, str) or not url.strip():
                    logger.warning("Missing/invalid 'url' at %s:%d: %r", url_file_path, lineno, obj)
                    continue

                items.append(
                    {
                        "category": obj.get("category") or default_category,
                        "url": url.strip(),
                        "sold": obj.get("sold"),
                    }
                )

        return items
    @staticmethod
    def _generate_match_key(text):
        """Tạo key chuẩn hóa từ text variant (bỏ Key, lấy Value, sắp xếp alpha)."""
        if not text or not isinstance(text, str):
            return "default"
        text = text.lower()
        # Tách chuỗi dựa trên dấu phẩy, chấm phẩy hoặc gạch ngang
        raw_tokens = re.split(r'[,;\-]\s*', text)
        extracted_values = []
        for token in raw_tokens:
            token = token.strip()
            if not token: continue
            # Nếu có dấu hai chấm (VD: "màu:xanh"), chỉ lấy phần sau
            if ':' in token:
                parts = token.split(':', 1)
                value_part = parts[1]
            else:
                value_part = token
            # Chỉ giữ lại chữ và số
            clean_val = re.sub(r'[^\w]', '', value_part)
            if clean_val:
                extracted_values.append(clean_val)
        extracted_values.sort()
        return "_".join(extracted_values)

    def _scrape_reviews_mtop(self, url):
        """Dùng API nội bộ (MTOP) để quét toàn bộ review."""
        try:
            # Lấy Item ID từ URL
            match = re.search(r'-i(\d+)', url)
            if not match:
                return {}
            item_id = match.group(1)
            
            logger.info(f"Bắt đầu quét review cho Item ID: {item_id}")
            
            # JS Script: Quét đến khi rỗng (Until Empty)
            js_script = f"""
            return (async function() {{
                const itemId = "{item_id}";
                const pageSize = 20; 
                let allVariants = [];
                let page = 1;
                let hasMore = true;
                const safetyLimit = 50; // Giới hạn 50 trang (~1000 review)

                const callApi = (p) => {{
                    return new Promise((resolve) => {{
                        if (!window.lib || !window.lib.mtop) {{ resolve(null); return; }}
                        window.lib.mtop.request({{
                            api: 'mtop.lazada.review.item.getpcreviewlist',
                            v: '1.0',
                            data: {{ itemId: itemId, pageNo: p, pageSize: pageSize, sort: 0 }},
                            ecode: 1, type: 'GET', dataType: 'json'
                        }}).then(res => resolve(res)).catch(err => resolve(null));
                    }});
                }};

                while (hasMore && page <= safetyLimit) {{
                    const res = await callApi(page);
                    let found = 0;
                    if (res && res.data && res.data.module && res.data.module.reviews) {{
                        const revs = res.data.module.reviews;
                        if (revs.length > 0) {{
                            revs.forEach(r => {{
                                if (r.skuInfo) allVariants.push(r.skuInfo);
                                else if (r.variant) allVariants.push(r.variant);
                            }});
                            found = revs.length;
                        }}
                    }}
                    if (found === 0) hasMore = false;
                    else {{
                        page++;
                        await new Promise(r => setTimeout(r, 200 + Math.random() * 200));
                    }}
                }}
                return allVariants;
            }})();
            """
            
            # Cuộn nhẹ để load thư viện JS nếu cần
            self.driver.execute_script("window.scrollBy(0, 400);")
            time.sleep(1)
            
            variants_list = self.driver.execute_script(js_script)
            return Counter(variants_list) if variants_list else {}
            
        except Exception as e:
            logger.error(f"Lỗi khi quét review: {e}")
            return {}
    def _crawl_product_moduledata(self, url: str, include_specs_by_sku: bool = False,estimate_variant_ratio_sold_by_count_review:bool = False) -> Dict[str, Any]:
        """Open a product URL and extract structured product information.

        This method uses `window.__moduleData__` to extract:
        - product name, item id
        - ratings/review breakdown
        - variants (skuInfos + skuBase)
        - specifications (default and optionally full mapping by sku)
        - highlights/description (JSON-only from `window.__moduleData__`)

        Args:
            url: Lazada product detail page URL.
            include_specs_by_sku: If True, include the full `specifications` mapping
                by skuId in the output.
            estimate_variant_ratio_sold_by_count_review: If True, estimate the ratio of variants sold
                based on the count of reviews.
           
        Returns:
            A dict containing extracted product information.

        Raises:
            RuntimeError: If module data cannot be read.
            Exception: Propagates unexpected Selenium/JS errors.
        """
        logger.info(f"Open product: {url}")
        self.driver.get(url)
        time.sleep(random.uniform(4.5, 7.5))

        # ensure scripts loaded & detail module rendered
        self._scroll_page_natural()

        raw = self._safe_get_moduledata()
        fields = raw.get("data", {}).get("root", {}).get("fields", {}) or {}
        product = fields.get("product") or {}
        seller = fields.get("seller") or {}
        seller_info = {
            "name": seller.get("name"),
            "sellerId": seller.get("sellerId")
        }
        title = product.get("title") or "Unknown title"

        review = fields.get("review") or {}
        rating_average = review.get("averageRating")
        reviews_count = review.get("reviews")
        reviews_with_content = review.get("contentedNum")
        rating_breakdown = review.get("scores")

        sku_infos = fields.get("skuInfos") or {}
        sku_base = (fields.get("productOption") or {}).get("skuBase") or {}
        properties = sku_base.get("properties", []) or []
        skus_list = sku_base.get("skus", []) or []

        prop_lookup = self._build_prop_lookup(properties)
        sku_map = self._build_sku_map(skus_list)

        default_sku_id: Optional[str] = None
        if isinstance(sku_infos, dict) and "0" in sku_infos and isinstance(sku_infos["0"], dict):
            default_sku_id = str(sku_infos["0"].get("skuId") or "").strip() or None

        variants: List[Dict[str, Any]] = []
        for sku_key, info in (sku_infos or {}).items():
            if not isinstance(info, dict):
                continue
            sku_id = str(info.get("skuId") or "").strip()
            if not sku_id:
                continue

            prop_path = sku_map.get(sku_id) or sku_map.get(self._digits_only(sku_id)) or ""
            variant_name = self._parse_variant_name(prop_path, prop_lookup)

            variants.append({
                "sku_key": str(sku_key),
                "sku_id": sku_id,
                "variant": variant_name,
                "price": self._extract_price_text(info),
                # "stock": self._extract_stock(info),
                # "image": info.get("image") or "",
                # "prop_path": prop_path,
            })
        variants.sort(key=lambda x: x["variant"])

        specs_raw = fields.get("specifications")
        specifications_by_sku = specs_raw if (include_specs_by_sku and isinstance(specs_raw, dict)) else None
        specifications_default = None
        if isinstance(specs_raw, dict):
            sid = default_sku_id or (variants[0]["sku_id"] if variants else None)
            if sid and sid in specs_raw:
                specifications_default = specs_raw.get(sid)

        highlights, description = self._extract_highlights_and_description_json(raw)
        
        # --- [BẮT ĐẦU ĐOẠN CODE MỚI CẦN THÊM] ---
        if estimate_variant_ratio_sold_by_count_review:
            # 1. Gọi hàm quét review
            review_counter = self._scrape_reviews_mtop(url)
            
            # 2. Tạo Map chuẩn hóa từ review { "key_chuan_hoa": so_luong }
            normalized_review_map = {}
            for raw_text, count in review_counter.items():
                key = self._generate_match_key(raw_text)
                normalized_review_map[key] = normalized_review_map.get(key, 0) + count
                
            # 3. Nối thông tin vào danh sách variants hiện có
            total_sample_sold = 0
            for v in variants:
                # Tạo key chuẩn hóa cho variant của scraper ("Xanh - L")
                v_key = self._generate_match_key(v.get('variant', ''))
                
                # Tra cứu số lượng
                sold_est = normalized_review_map.get(v_key, 0)
                
                v['sold_sample'] = sold_est
                total_sample_sold += sold_est
                
            # 4. Tính tỷ lệ % (Optional)
            for v in variants:
                if total_sample_sold > 0:
                    v['sold_ratio'] = round((v['sold_sample'] / total_sample_sold) * 100, 1)
                else:
                    v['sold_ratio'] = 0

            # --- [KẾT THÚC ĐOẠN CODE MỚI] ---
        
        result = {
        "product_name": title,
        "url": url,
        "rating_average": rating_average,
        "reviews_count": reviews_count,
        "reviews_with_content": reviews_with_content,
        "default_sku_id": default_sku_id,
        "highlights_text": highlights,
        "description_text_or_html": description,
        "specifications_default": specifications_default,
        "specifications_by_sku": specifications_by_sku,
        "total_variants": len(variants),
        "variants": variants,
        "seller_info": seller_info,
        # "rating_breakdown": rating_breakdown,
        }

        if estimate_variant_ratio_sold_by_count_review:
            result["review_sample_size"] = total_sample_sold

        return result

    def get_product_info(
        self,
        url_file_path,
        output_file_name: str = 'product.ndjson',
        send_to_kafka: bool = False,
        include_specs_by_sku: bool = False,
    ):
        """Extract product info for a list of URLs and persist results.

        Current supported input sources:
        - A local file (`url_file_path`) containing either:
          - one URL per line, or
          - NDJSON lines with fields: `category`, `url`, `sold`.

        Args:
            url_file_path: Path to input file containing URLs (or URL metadata).
            output_file_name: Output NDJSON filename written under the category folder.
                Each line is one extracted product JSON.
            send_to_kafka: If True, attempt to send each extracted product to Kafka.
                This will be a no-op unless Kafka producers are enabled in `CommonScraper`.
            include_specs_by_sku: If True, include the full `specifications_by_sku` mapping.

        Returns:
            None. Results are persisted to disk and optionally sent to Kafka.
        """
        # Chế độ đọc từ file
        if url_file_path:
            logger.info(f"Reading URLs from file: {url_file_path}")
            url_dir = os.path.dirname(url_file_path)
            category = os.path.relpath(url_dir, self.data_dir)
            logger.info(f"Category: {category}")

            items = self._read_url_items_from_file(url_file_path, default_category=category)
            logger.info(f"Found {len(items)} URLs to scrape")

            output_file = os.path.join(self.data_dir, category, output_file_name)
            os.makedirs(os.path.dirname(output_file), exist_ok=True)

            for idx, it in enumerate(items, 1):
                url = it.get('url')
                if not url:
                    continue

                # Restart between items based on number of items processed (NOT retries).
                if self.restart_num and self._items_since_restart >= self.restart_num:
                    logger.info(
                        "Reached restart threshold (%d items). Restarting driver...",
                        self._items_since_restart,
                    )
                    self.restart_driver()
                    self._items_since_restart = 0
                    self.human_sleep(1.0, 2.0)
                category_it = it.get('category') or category
                sold = it.get('sold')
                logger.info(f"Processing URL {idx}/{len(items)}: {url}")

                # load + extract with retries
                extracted: Optional[Dict[str, Any]] = None
                last_err: Optional[Exception] = None
                
                
                for attempt in range(self.retry_num):
                    try:
                        extracted = self._crawl_product_moduledata(url, include_specs_by_sku=include_specs_by_sku)
                        break
                    except Exception as e:
                        last_err = e
                        logger.error(f"Extract failed (attempt {attempt + 1}/{self.retry_num}): {e}")
                        try:
                            self.driver.refresh()
                        except Exception:
                            pass
                        self.human_sleep(1.2, 2.2)

                if not extracted:
                    logger.error(f"Skipping URL due to extract failure: {url} | last_err={last_err}")
                    self._items_since_restart += 1
                    continue

                # merge url-metadata: category/url/sold
                extracted['category'] = category_it
                extracted['sold'] = sold

                # append ndjson for checking
                with open(output_file, 'a', encoding='utf-8') as f:
                    json.dump(extracted, f, ensure_ascii=False)
                    f.write('\n')

                # Count this item once (even if it needed retries).
                self._items_since_restart += 1
                

                # # optional kafka
                # if send_to_kafka:
                #     self.send_to_kafka(extracted, 'info')

            logger.info(f"Finished scraping all products from file -> {output_file}")
            return


