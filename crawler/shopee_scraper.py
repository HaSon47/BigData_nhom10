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
    def _get_product_urls(self, product_list, category):
        for product in product_list:
            product_url = 'https://shopee.vn' + product.a['href']
            #self.write_to_file(product_url, os.path.join(category, 'url.txt'))
            # d = {
            #     'category': category,
            #     'url': product_url
            # }
            # self.send_to_kafka(d, 'url')

    def get_product_info(self):
        logger.info(f"Consuming from {self.url_topic}")
        terminate_count = 0
        while True:
            logger.info("Polling for messages")
            breakpoint()
            result = self.url_consumer.poll(timeout_ms=1000, max_records=1)
            if result:
                terminate_count = 0
                logger.info("Message found")
                records = list(result.values())[0]
                for record in records:
                    logger.info(f"Record: {record.value}")
                    d = json.loads(record.value)
                    category = d['category']
                    url = d['url']
                    logger.info('Scraping category: ' + category)

                    url = url.strip()
                    breakpoint()
                    self.driver.get(url)

                    try:
                        # get all product types
                        type_arr = []
                        WebDriverWait(self.driver, self.wait_timeout).until(
                            ec.visibility_of_element_located((By.CLASS_NAME, 'flex.rY0UiC.j9be9C')))
                        type_ele = self.driver.find_element(
                            By.CLASS_NAME, 'flex.rY0UiC.j9be9C')
                        logger.info(
                            'Product types found, iterating through each type...')
                        for retry in range(self.retry_num):
                            try:
                                # grab all product type button into an array
                                for counter, attr in enumerate(
                                        type_ele.find_elements(By.XPATH,
                                                               './div/div[@class=\'flex items-center\']')):
                                    buttons = attr.find_elements(By.XPATH,
                                                                 './div/button[@class=\'product-variation\']')
                                    if len(buttons) == 0:
                                        logger.info(
                                            f'Attribute number {counter} has no clickable buttons, skipping this attribute')
                                        continue
                                    type_arr.append(buttons)
                                logger.info(
                                    f'{len(type_arr)} types found, iterating through all of them')
                                self._iterate_all_product_type(
                                    0, type_arr, url=url)
                                break

                            except StaleElementReferenceException:
                                logger.info(
                                    'Cannot get product types, retrying')

                            if retry == self.retry_num - 1:
                                logger.info(
                                    f'Cannot get product types after {self.retry_num} attempts')
                                self._get_product_info_helper(url)
                    except TimeoutException:
                        logger.info('Product has no type, scraping directly')
                        self._get_product_info_helper(url)

                    except Exception as e:
                        logger.error(e)

            else:
                logger.info('No message found in buffer')
                terminate_count += 1
                if terminate_count == 900:
                    break

    def _iterate_all_product_type(self, type_index, type_arr, **kwargs):
        if type_index == len(type_arr):
            self._get_product_info_helper(kwargs['url'])
            return

        for element in type_arr[type_index]:
            element.click()
            self._iterate_all_product_type(
                type_index + 1, type_arr, url=kwargs['url'])

    def _get_product_info_helper(self, curr_url):
        try:
            result = {}

            WebDriverWait(self.driver, self.wait_timeout).until(
                ec.visibility_of_element_located((By.CLASS_NAME, "_44qnta")))
            product_name = self.driver.find_element(
                By.CLASS_NAME, '_44qnta').text
            logger.info('Product name extracted')

            price = self.driver.find_element(By.CLASS_NAME, 'pqTWkA').text
            logger.info('Price extracted')

            # this element contains average ratings, number of reviews, number sold
            avg_rating = None
            num_review = None
            num_sold = None
            some_info = self.driver.find_element(
                By.CLASS_NAME, 'flex.X5u-5c').text.split('\n')
            if len(some_info) == 6:  # has all info
                avg_rating = some_info[0]
                logger.info('Average rating extracted')
                num_review = some_info[1]
                logger.info('Number of ratings extracted')
                num_sold = some_info[3]
                logger.info('Number of product sold extracted')
            elif len(some_info) == 4:  # has no review
                logger.info('Product has no review')
                num_sold = some_info[3]
                logger.info('Number of product sold extracted')
            else:
                logger.info('Cannot parse review and number sold info')

            shipping = None
            try:
                shipping = self.driver.find_element(
                    By.CLASS_NAME, 'WZTmVh').text.split('\n')[-1]
                logger.info('Shipping info extracted')
            except NoSuchElementException:
                logger.info('No shipping info found')

            attrs = {}
            type_ele = self.driver.find_element(
                By.CLASS_NAME, 'flex.rY0UiC.j9be9C')
            for ele in type_ele.find_elements(By.XPATH, './div/div[@class=\'flex items-center\']'):
                attr_name = ele.find_element(By.CLASS_NAME, 'oN9nMU').text
                try:
                    attr_value = ele.find_element(
                        By.CLASS_NAME, 'product-variation.product-variation--selected')
                    attr_value = attr_value.text
                except NoSuchElementException:
                    logger.info(
                        f"Attribute of \"{attr_name}\" is not found, setting as null")
                    attr_value = None
                attrs[attr_name] = attr_value

            product_desc = self.driver.find_element(By.CLASS_NAME, 'product-detail.page-product__detail').get_attribute(
                'innerHTML')
            logger.info('Product description extracted')

            shop_info = None
            try:
                shop_info = self.driver.find_element(
                    By.CLASS_NAME, 'NLeTwo.page-product__shop').text
                logger.info('Shop info extracted')
            except NoSuchElementException:
                logger.info('Shop info not found')

            result['product_name'] = product_name
            result['shipping'] = shipping
            result['shop_info'] = shop_info
            result['price'] = price
            result['product_desc'] = product_desc
            result['avg_rating'] = avg_rating
            result['num_review'] = num_review
            result['num_sold'] = num_sold
            result['attrs'] = attrs
            result['url'] = curr_url

            # with open(os.path.join(category_path, 'product.ndjson'), 'a') as f:
            #     json.dump(result, f, ensure_ascii=False)
            #     f.write('\n')

            self.send_to_kafka(result, 'info')
        except (NoSuchElementException, TimeoutException) as e:
            logger.error(e)