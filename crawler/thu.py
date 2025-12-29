import json
import logging
import os
import time
import random

from bs4 import BeautifulSoup
from selenium.common import TimeoutException, StaleElementReferenceException, NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
from crawler.common_scraper import CommonScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


class ShopeeScraper(CommonScraper):
    def __init__(self, num_page_to_scrape=10, data_dir='./data/shopee', wait_timeout=5, retry_num=3,
                 restart_num=20, is_headless=False, consumer_id=None):
        if not os.path.exists(data_dir):
            os.mkdir(data_dir)
        main_page = "https://shopee.vn/"
        super().__init__(num_page_to_scrape, data_dir, wait_timeout, retry_num, restart_num, is_headless, main_page,
                         'shopee_info', 'shopee_url', 'shopee_scraper', consumer_id)

    def human_sleep(self, a=1.0, b=2.5):
        """Tạo delay ngẫu nhiên giống người thật"""
        time.sleep(random.uniform(a, b))

    def get_product_urls(self):
        for cat_1 in os.listdir(self.data_dir):
            if cat_1 not in ['Thời Trang Nam', 'Thời Trang Nữ']:
                continue
            full_cat_1 = os.path.join(self.data_dir, cat_1)
            for cat_2 in os.listdir(full_cat_1):
                full_cat_2 = os.path.join(full_cat_1, cat_2)
                with open(os.path.join(full_cat_2, 'base_url.txt')) as f:
                    d = json.load(f)
                category = d['category']
                category_url = d['url']

                logger.info("Scraping category: " + category)
                output_dir = os.path.join(self.data_dir, category)
                if not os.path.exists(output_dir):
                    os.mkdir(output_dir)

                logger.info(f"Opening category page: {category_url}")
                self.human_sleep(2, 4)  # delay trước khi mở trang

                self.driver.get(category_url)
                self.human_sleep(2, 3)  # đợi trang load sơ bộ

                counter = 1
                prev_url = ''

                while True:
                    logger.info("Scraping urls from page " + str(counter))

                    if prev_url == self.driver.current_url:
                        logger.info('Page identical to previous; reached last page')
                        break
                    prev_url = self.driver.current_url

                    for retry in range(self.retry_num):
                        try:
                            self.driver.execute_script("window.scrollTo(0,0)")
                            self.human_sleep(0.6, 1.2)

                            WebDriverWait(self.driver, self.wait_timeout).until(
                                ec.visibility_of_element_located((By.CLASS_NAME, "shopee-search-item-result__item"))
                            )

                            # Cuộn xuống từ từ giống người dùng
                            for i in range(12):
                                self.driver.execute_script("window.scrollBy(0,350)")
                                time.sleep(random.uniform(0.1, 0.25))

                            self.human_sleep(1.0, 2.0)  # đợi DOM tải xong

                            soup = BeautifulSoup(self.driver.page_source, features="lxml")
                            product_list = soup.find_all(class_='shopee-search-item-result__item')

                            product_list_with_url = soup \
                                .find(class_='row shopee-search-item-result__items') \
                                .find_all('a', href=True)

                            num_product = len(product_list)
                            num_product_with_url = len(product_list_with_url)

                            logger.info(f'Found {num_product} products')
                            logger.info(f'Found {num_product_with_url} products with url')

                            if len(product_list_with_url) == len(product_list):
                                logger.info('All product URLs loaded. Extracting...')
                                self._get_product_urls(product_list, category)
                                break

                            elif retry == self.retry_num - 1:
                                logger.info(
                                    f'Only {num_product_with_url}/{num_product} product URLs '
                                    f'after {self.retry_num} retries')
                            else:
                                logger.info(
                                    f'{num_product_with_url}/{num_product} URLs found, retrying...')
                                self.human_sleep(1.5, 2.5)

                        except TimeoutException:
                            logger.info(f'Products not visible after {self.wait_timeout}s, retrying')
                            self.human_sleep(1, 2)

                    if counter == self.num_page_to_scrape:
                        logger.info(f'Reached max pages for category {category}')
                        break

                    counter += 1
                    logger.info('Going to the next page')

                    self.human_sleep(1.5, 3.0)  # đợi trước khi click

                    next_page_button = self.driver.find_element(
                        by=By.CLASS_NAME, value="shopee-icon-button--right"
                    )
                    next_page_button.click()

                    self.human_sleep(2.5, 4.0)  # đợi trang sau load
        self.driver.close()
                    
    def _get_product_urls(self, product_list, category):
        for product in product_list:
            product_url = 'https://shopee.vn' + product.a['href']
            self.write_to_file(product_url, os.path.join(category, 'url.txt'))
            # d = {
            #     'category': category,
            #     'url': product_url
            # }
            # self.send_to_kafka(d, 'url')
    def get_product_info(self):

        # Load danh sách URL
        url_file = r'C:\Users\Lenovo\Desktop\20251\BigData\code\BigData_nhom10\data\shopee\Thời Trang Nữ\Áo\url.txt'
        
        with open(url_file, 'r', encoding='utf-8') as f:
            urls = [line.strip() for line in f]

        for url in urls:

            # ========== CHỐNG BOT ==========
            # random delay như người thật
            self.human_sleep(2, 4)

            # random scroll trước khi vào URL (hành vi "người")
            try:
                self.driver.execute_script(f"window.scrollTo(0, {random.randint(200, 600)});")
            except:
                pass

            # open URL
            self.driver.get(url)
            logger.info(f"\nOpening product URL: {url}")
            self.human_sleep(2, 3)

            # scroll nhẹ để Shopee load variation
            for _ in range(random.randint(2, 4)):
                self.driver.execute_script("window.scrollBy(0, 400);")
                self.human_sleep(0.5, 1.0)

            # ========== LẤY PRODUCT TYPE ==========
            variation_xpath = "//button[contains(@class,'product-variation')]"

            try:
                WebDriverWait(self.driver, self.wait_timeout).until(
                    ec.presence_of_element_located((By.XPATH, variation_xpath))
                )
            except TimeoutException:
                logger.info("No variation found – skipping")
                continue

            logger.info("Product types found — collecting variations...")

            # Lấy toàn bộ nhóm variation (color, size, kiểu…)
            group_xpath = (
                "//div[contains(@class,'product-variation')]/ancestor::div"
                "[contains(@class,'flex') and contains(@class,'items-center')]"
            )

            try:
                variation_groups = self.driver.find_elements(By.XPATH, group_xpath)
            except:
                logger.info("Variation structure changed — skipping")
                continue

            type_arr = []

            for idx, group in enumerate(variation_groups):
                try:
                    buttons = group.find_elements(
                        By.XPATH, ".//button[contains(@class,'product-variation')]"
                    )
                    if len(buttons) == 0:
                        continue

                    logger.info(f" Variation group {idx+1}: {len(buttons)} options")

                    type_arr.append(buttons)

                except StaleElementReferenceException:
                    logger.info("Stale element — retrying group...")
                    continue

            if not type_arr:
                logger.info("No clickable variation found — skip product")
                continue

            # ========== COMBINATIONS ==========
            logger.info(f"Total variation groups: {len(type_arr)} — iterating...")

            try:
                self._iterate_all_product_type(0, type_arr, url=url)
            except Exception as e:
                logger.info(f"Error when iterating product types: {e}")
                continue



          
    def get_product_info_draf(self):
        """Scrape Shopee product info với anti-detection nâng cao"""
        url_file = r'C:\Users\Lenovo\Desktop\20251\BigData\code\BigData_nhom10\data\shopee\Thời Trang Nữ\Áo\url.txt'
        
        with open(url_file, 'r', encoding='utf-8') as f:
            urls = [line.strip() for line in f]
        
        for idx, url in enumerate(urls):
            try:
                # ========== CHỐNG BOT NÂNG CAO ==========
                
                # 1. Delay ngẫu nhiên giữa các sản phẩm (tăng range)
                if idx > 0:
                    self.human_sleep(5, 12)
                
                # 2. Mô phỏng di chuột ngẫu nhiên trước khi load trang
                self._simulate_mouse_movement()
                
                # 3. Random scroll trên trang hiện tại
                try:
                    for _ in range(random.randint(1, 3)):
                        scroll_amount = random.randint(100, 500)
                        self.driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
                        self.human_sleep(0.3, 0.8)
                except:
                    pass
                
                # 4. Load URL với random User-Agent rotation (nếu có setup)
                self.driver.get(url)
                logger.info(f"\n[{idx+1}/{len(urls)}] Opening: {url}")
                
                # 5. Delay sau khi load trang (giả lập đọc nội dung)
                self.human_sleep(3, 6)
                
                # 6. Scroll từ từ như người thật đang xem sản phẩm
                self._natural_scroll_behavior()
                
                # 7. Random hover vào các element (mô phỏng di chuột xem ảnh)
                self._random_hover_actions()
                
                # ========== LẤY PRODUCT TYPE ==========
                variation_xpath = "//button[contains(@class,'product-variation')]"
                try:
                    WebDriverWait(self.driver, self.wait_timeout).until(
                        ec.presence_of_element_located((By.XPATH, variation_xpath))
                    )
                except TimeoutException:
                    logger.info("No variation found – skipping")
                    # Delay trước khi chuyển sản phẩm
                    self.human_sleep(2, 4)
                    continue
                
                logger.info("Product types found — collecting variations...")
                
                # Delay nhẹ trước khi tương tác với variations
                self.human_sleep(1, 2)
                
                # Lấy toàn bộ nhóm variation
                group_xpath = (
                    "//div[contains(@class,'product-variation')]/ancestor::div"
                    "[contains(@class,'flex') and contains(@class,'items-center')]"
                )
                try:
                    variation_groups = self.driver.find_elements(By.XPATH, group_xpath)
                except:
                    logger.info("Variation structure changed — skipping")
                    continue
                
                type_arr = []
                for idx_group, group in enumerate(variation_groups):
                    try:
                        buttons = group.find_elements(
                            By.XPATH, ".//button[contains(@class,'product-variation')]"
                        )
                        if len(buttons) == 0:
                            continue
                        logger.info(f" Variation group {idx_group+1}: {len(buttons)} options")
                        type_arr.append(buttons)
                    except StaleElementReferenceException:
                        logger.info("Stale element — retrying group...")
                        continue
                
                if not type_arr:
                    logger.info("No clickable variation found — skip product")
                    self.human_sleep(1, 3)
                    continue
                
                # ========== COMBINATIONS ==========
                logger.info(f"Total variation groups: {len(type_arr)} — iterating...")
                try:
                    self._iterate_all_product_type(0, type_arr, url=url)
                except Exception as e:
                    logger.info(f"Error when iterating product types: {e}")
                    continue
                    
            except Exception as e:
                logger.error(f"Error processing URL {url}: {e}")
                self.human_sleep(3, 6)
                continue

    def _iterate_all_product_type(self, type_index, type_arr, **kwargs):
        """Iterate qua các variations với human-like behavior"""
        if type_index == len(type_arr):
            self._get_product_info_helper(kwargs['url'])
            return
        
        for btn_idx, btn in enumerate(type_arr[type_index]):
            try:
                # Scroll đến button một cách tự nhiên
                self.driver.execute_script(
                    "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", 
                    btn
                )
                
                # Delay ngẫu nhiên trước khi click
                self.human_sleep(0.6, 1.5)
                
                # Hover trước khi click (mô phỏng người thật)
                try:
                    actions = ActionChains(self.driver)
                    actions.move_to_element(btn).pause(random.uniform(0.2, 0.5)).perform()
                except:
                    pass
                
                # Click với delay nhỏ
                self.human_sleep(0.2, 0.5)
                btn.click()
                
                # Delay sau click để page update
                self.human_sleep(0.8, 1.5)
                
            except Exception as e:
                logger.debug(f"Cannot click variation button: {e}")
                continue
            
            self._iterate_all_product_type(type_index + 1, type_arr, url=kwargs['url'])

    def _get_product_info_helper(self, curr_url):
        """Extract product info với natural delays"""
        result = {"url": curr_url}
        
        try:
            # Delay nhẹ trước khi extract (giả lập đọc thông tin)
            self.human_sleep(1, 2)
            
            # ===== GET PRODUCT NAME =====
            title_xpath = "//h1[contains(@class,'attM6y')] | //h1"
            title = WebDriverWait(self.driver, self.wait_timeout).until(
                ec.presence_of_element_located((By.XPATH, title_xpath))
            )
            result['name'] = title.text
            logger.info("Product name extracted")
            
            # ===== PRICE =====
            price_xpath = "//div[contains(@class,'pmmxKx') or contains(@class,'Ybrg+') or contains(@class,'_2Shl1Z')]"
            price_ele = self.driver.find_element(By.XPATH, price_xpath)
            result['price'] = price_ele.text
            logger.info("Price extracted")
            
            # ===== RATINGS / REVIEWS / SOLD =====
            info_xpath = "//div[contains(@class,'flex') and contains(., 'đánh giá') or contains(., 'Đã bán')]"
            infos = self.driver.find_elements(By.XPATH, info_xpath)
            if len(infos) > 0:
                text = infos[0].text.split('\n')
                for t in text:
                    if "đánh giá" in t:
                        result["reviews"] = t
                    elif "Đã bán" in t:
                        result["sold"] = t
                    elif "trên" in t or "sao" in t:
                        result["rating"] = t
            logger.info("Rating / reviews / sold extracted")
            
            # ===== SHIPPING =====
            ship_xpath = "//div[contains(text(),'Vận chuyển')]/following-sibling::div"
            ship_ele = self.driver.find_elements(By.XPATH, ship_xpath)
            result["shipping"] = ship_ele[0].text if ship_ele else None
            logger.info("Shipping info extracted")
            
            # Delay sau khi extract xong (mô phỏng đọc xong)
            self.human_sleep(0.5, 1.2)
            
        except Exception as e:
            logger.error(f"Error extracting product info: {e}")
        finally:
            print("\nRESULT:", result)
            return result

    # ========== CÁC HÀM HỖ TRỢ CHỐNG BOT ==========

    def _simulate_mouse_movement(self):
        """Mô phỏng di chuyển chuột ngẫu nhiên"""
        try:
            actions = ActionChains(self.driver)
            for _ in range(random.randint(2, 4)):
                x_offset = random.randint(-200, 200)
                y_offset = random.randint(-200, 200)
                actions.move_by_offset(x_offset, y_offset).perform()
                time.sleep(random.uniform(0.1, 0.3))
        except Exception as e:
            logger.debug(f"Mouse movement simulation failed: {e}")

    def _natural_scroll_behavior(self):
        """Scroll tự nhiên như người thật xem sản phẩm"""
        try:
            viewport_height = self.driver.execute_script("return window.innerHeight")
            total_scroll = 0
            max_scroll = random.randint(800, 1500)
            
            while total_scroll < max_scroll:
                scroll_step = random.randint(150, 400)
                self.driver.execute_script(f"window.scrollBy(0, {scroll_step});")
                total_scroll += scroll_step
                
                # Delay ngẫu nhiên giữa các lần scroll
                self.human_sleep(0.5, 1.5)
                
                # Đôi khi scroll lên một chút (mô phỏng người xem lại)
                if random.random() < 0.3:
                    back_scroll = random.randint(50, 150)
                    self.driver.execute_script(f"window.scrollBy(0, -{back_scroll});")
                    self.human_sleep(0.3, 0.8)
        except Exception as e:
            logger.debug(f"Natural scroll failed: {e}")

    def _random_hover_actions(self):
        """Hover ngẫu nhiên vào các element (mô phỏng xem ảnh sản phẩm)"""
        try:
            # Hover vào ảnh sản phẩm
            images = self.driver.find_elements(By.TAG_NAME, "img")[:5]
            if images and random.random() < 0.7:
                target_img = random.choice(images)
                actions = ActionChains(self.driver)
                actions.move_to_element(target_img).pause(random.uniform(0.5, 1.5)).perform()
                self.human_sleep(0.3, 0.8)
        except Exception as e:
            logger.debug(f"Random hover failed: {e}")