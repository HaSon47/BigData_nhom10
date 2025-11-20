import json
import logging
import os
from random import uniform

from bs4 import BeautifulSoup
from selenium.common import TimeoutException, ElementClickInterceptedException, \
    MoveTargetOutOfBoundsException, StaleElementReferenceException, NoSuchElementException
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait

from crawler.common_scraper import CommonScraper

logger = logging.getLogger(__name__)


class LazadaScraper(CommonScraper):
    def __init__(self, num_page_to_scrape=10, data_dir='./data/lazada', wait_timeout=5, retry_num=3,
                 restart_num=10, is_headless=False, consumer_id=None):
        if not os.path.exists(data_dir):
            os.mkdir(data_dir)
        main_page = 'https://www.lazada.vn/'
        super().__init__(num_page_to_scrape, data_dir, wait_timeout, retry_num, restart_num, is_headless, main_page,
                         'lazada_info', 'lazada_url', 'lazada_scraper', consumer_id)

    def get_product_urls(self):
        # go through all categories
        for cat_1 in os.listdir(self.data_dir):
            full_cat_1 = os.path.join(self.data_dir, cat_1)
            for cat_2 in os.listdir(full_cat_1):
                full_cat_2 = os.path.join(full_cat_1, cat_2)
                for cat_3 in os.listdir(full_cat_2):
                    full_cat_3 = os.path.join(full_cat_2, cat_3)
                    with open(os.path.join(full_cat_3, 'base_url.txt')) as f:
                        d = json.load(f)
                    category = d['category']
                    category_url = d['url']

                    # start scraping
                    logger.info("Scraping category: " + category)
                    output_dir = os.path.join(self.data_dir, category)
                    if not os.path.exists(output_dir):
                        os.mkdir(output_dir)
                    self.driver.get(category_url)

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
                        curr_page_num = self.driver.find_element(By.CLASS_NAME, 'ant-pagination-item-active').get_attribute(
                            'title')
                        logger.info('Current page ' + str(curr_page_num))
                        soup = BeautifulSoup(self.driver.page_source, features="lxml")
                        products = soup.find_all(class_='Bm3ON')
                        logger.info('Number of products in page: ' + str(len(products)))
                        for product in products:
                            url = product.find('a')['href'][2:]
                            url = 'https://' + url
                            d = {
                                'category': category,
                                'url': url
                            }
                            # self.write_to_file(url, os.path.join(category, 'url.txt'))
                            self.send_to_kafka(d, 'url')
                        logger.info("Finished scraping urls from page " + str(counter))

                        # WebDriverWait(self.driver, self.wait_timeout).until(
                        #     EC.visibility_of_element_located((By.CSS_SELECTOR, ".ant-pagination-next > button:nth-child(1)")))
                        next_page_button = self.driver.find_element(by=By.CSS_SELECTOR, value=".ant-pagination-next > "
                                                                                              "button:nth-child(1)")
                        is_last_page = not next_page_button.is_enabled()
                        if counter == self.num_page_to_scrape:
                            logger.info(f'Reached maximum number of pages to scrape in category: {category}')
                            break
                        elif is_last_page:
                            logger.info(f'Reached the last page of category: {category}')
                            break
                        try:
                            next_page_button.click()
                            logger.info("Clicked next page")
                        except ElementClickInterceptedException:
                            self.check_popup()

    def get_product_info(self, scroll_retry=3):
        logger.info(f"Consuming from {self.url_topic}")
        terminate_count = 0
        while True:
            logger.info("Polling for messages")
            result = self.url_consumer.poll(timeout_ms=1000, max_records=1)
            if result:
                terminate_count = 0
                logger.info("Message found")
                records = list(result.values())[0]
                for record in records:
                    logger.info(f"Record: {record.value}")
                    d = json.loads(record.value)
                    category = d['category']
                    url = d['url'].strip()
                    logger.info('Scraping category: ' + category)

                    page_loaded = False
                    for retry in range(self.retry_num):
                        try:
                            self.driver.get(url)
                            page_loaded = True
                            break
                        except TimeoutException:
                            if retry == self.retry_num - 1:
                                logger.error(f'Cannot load the website after {self.retry_num} retries')
                            else:
                                logger.error("Cannot load the website, retrying")

                    if not page_loaded:
                        continue

                    try:
                        # get all product types
                        type_arr = []
                        WebDriverWait(self.driver, self.wait_timeout).until(
                            ec.visibility_of_element_located((By.CLASS_NAME, 'sku-prop-content')))
                        for retry in range(self.retry_num):
                            try:
                                for ele in self.driver.find_elements(By.CLASS_NAME, 'sku-prop-content'):
                                    type_arr.append(ele.find_elements(By.XPATH, './*'))
                                logger.info(f'{len(type_arr)} types found, iterating through all of them')
                                self._iterate_all_product_type(0, type_arr, scroll_retry=scroll_retry, url=url, category=category)
                                break
                            # except IndexError as e:
                            #     logger.error(e)
                            #     break
                            except StaleElementReferenceException:
                                logger.error('Cannot get product types, retrying')
                            except Exception as e:
                                logger.error(e)
                                self.check_popup()
                                break
                            if retry == self.retry_num - 1:
                                logger.info(f'Cannot get product types after {self.retry_num} attempts')
                                self._get_product_info_helper(scroll_retry, url, category)
                    except TimeoutException:
                        logger.info('Product has no type, scraping directly')
                        self._get_product_info_helper(scroll_retry, url, category)

                    except Exception as e:
                        logger.error(e)
            else:
                logger.info('No message found in buffer')
                terminate_count += 1
                if terminate_count == 900:
                    break

    def _get_product_info_helper(self, scroll_retry, curr_url, category):
        try:
            result = {}

            # scroll down to load product description
            logger.info('Scrolling to find product descriptiion')
            found_desc = self._scroll_to_find_element(scroll_retry, 'pdp-product-desc', By.CLASS_NAME, curr_url)
            if not found_desc:
                logger.info('Product description not found')

            # scroll down to load average rating
            logger.info('Scrolling to find average rating')
            avg_rating = self._scroll_to_find_element(scroll_retry, 'score-average', By.CLASS_NAME, curr_url)
            if avg_rating:
                avg_rating = avg_rating.text
                logger.info('Average rating extracted')
            else:
                logger.info('Average rating not found')

            # create soup after dynamic elements are loaded
            soup = BeautifulSoup(self.driver.page_source, features="lxml")

            product_name = soup.find(class_='pdp-mod-product-badge-title').text
            if product_name:
                logger.info('Product name extracted')

            brand = soup.find(class_='pdp-product-brand__brand-link').text
            if brand:
                logger.info('Brand name extracted')

            num_review = soup.find(class_='pdp-review-summary__link').text
            if num_review:
                logger.info('Number of reviews extracted')

            price = soup.find(class_='pdp-price pdp-price_type_normal pdp-price_color_orange pdp-price_size_xl').text
            if price:
                logger.info('Price extracted')

            attrs = {}
            for attr in soup.find_all(class_='sku-prop-selection'):
                attr_name = attr.find('h6').text

                # handle kích cỡ since it's different from the others for some reason
                size = attr.find(class_='sku-tabpath-single')
                attr_value = None
                if size:
                    if size.text.strip() == 'Int':
                        attr_value = attr.find(class_='sku-variable-size-selected').text
                else:
                    attr_value = attr.find(class_='sku-name').text
                attrs[attr_name] = attr_value
            if attrs:
                logger.info('Product\'s attributes extracted')

            product_desc = None
            if found_desc:
                product_desc = str(soup.find(class_='pdp-product-desc height-limit'))
                logger.info('Product description extracted')

            shop_info = None
            try:
                shop_info = self.driver.find_element(By.CLASS_NAME, 'seller-container').text
                logger.info('Shop info extracted')
            except NoSuchElementException:
                logger.info('Shop info not found')

            result['product_name'] = product_name
            result['shop_info'] = shop_info
            result['price'] = price
            result['brand_name'] = brand
            result['num_review'] = num_review
            result['attrs'] = attrs
            result['product_desc'] = product_desc
            result['avg_rating'] = avg_rating
            result['category'] = category
            result['url'] = curr_url

            # with open(os.path.join(category_path, 'product.ndjson'), 'a') as f:
            #     json.dump(result, f, ensure_ascii=False)
            #     f.write('\n')

            self.send_to_kafka(result, 'info')
        except AttributeError as e:
            logger.error(e)

    def _scroll_to_find_element(self, scroll_retry, element_name, by, curr_url, scroll_by=1000):
        try:
            self.driver.execute_script("window.scrollTo(0,0)")
        except TimeoutException:
            logger.info('Failed to scroll page to the beginning')
            logger.info('Restarting driver')
            self.restart_driver()
            self.driver.get(curr_url)

        logger.info('Trying to find element: ' + element_name)
        for counter in range(scroll_retry):
            logger.info(f'Scrolling attempt: {counter}')
            try:
                # scroll up and down before
                if counter != 0:
                    # self.check_popup()
                    half_scroll = scroll_by / 2
                    self.driver.execute_script(f"window.scrollBy(0,{-half_scroll})")
                    self.driver.execute_script(f"window.scrollBy(0,{half_scroll})")

                # now actually scroll down
                self.driver.execute_script(f"window.scrollBy(0,{scroll_by})")
            except TimeoutException:
                logger.info('Failed to scroll page')
                logger.info('Restarting driver')
                self.restart_driver()
                self.driver.get(curr_url)
                continue

            try:
                WebDriverWait(self.driver, self.wait_timeout).until(
                    ec.visibility_of_element_located((by, element_name)))
                element = self.driver.find_element(by, element_name)
                logger.info('Element Found')
                return element
            except TimeoutException:
                if counter == scroll_retry - 1:
                    logger.info('Cannot find element')

    def _iterate_all_product_type(self, type_index, type_arr, **kwargs):
        if type_index == len(type_arr):
            self._get_product_info_helper(kwargs['scroll_retry'], kwargs['url'], kwargs['category'])
            return

        for element in type_arr[type_index]:
            try:
                self.driver.execute_script("window.scrollTo(0,0)")
            except TimeoutException:
                logger.info('Failed to scroll page to the beginning')
                logger.info('Restarting driver')
                self.restart_driver()
                self.driver.get(kwargs['url'])

            logger.info('Clicking out of notification')
            actions = ActionChains(self.driver)
            actions.move_by_offset(1, 1).click().perform()
            try:
                WebDriverWait(self.driver, self.wait_timeout).until(ec.element_to_be_clickable(element))
                element.click()
                self._iterate_all_product_type(type_index + 1, type_arr, scroll_retry=kwargs['scroll_retry'],
                                               url=kwargs['url'],
                                               category=kwargs['category'])
            except StaleElementReferenceException:
                logger.info('Element not attached to the page, renewing all elements')
                for row, attr in enumerate(self.driver.find_elements(By.CLASS_NAME, 'sku-prop-content')):
                    for col, ele in enumerate(attr.find_elements(By.XPATH, './*')):
                        type_arr[row][col] = ele
            except TimeoutException:
                logger.info(f'Element not clickable after waiting for {self.wait_timeout} seconds')

    def check_popup(self):
        try:
            logger.info("Checking for popup")
            WebDriverWait(self.driver, self.wait_timeout).until(
                ec.visibility_of_element_located((By.ID, 'baxia-dialog-content')))
            logger.info("Lazada popup detected")
            self.driver.switch_to.frame('baxia-dialog-content')
            logger.info("Switched to popup frame")
            slide_button = self.driver.find_element(By.CLASS_NAME, 'btn_slide')
            logger.info("Slide button found")
            actions = ActionChains(self.driver)
            actions.click_and_hold(slide_button).perform()
            v = 0
            a = 3
            for t in range(100):
                rng = uniform(0.9, 1.1)
                actions.move_by_offset(v + a * rng * t, rng * 5).perform()
            return True
        except TimeoutException:
            logger.info("Popup not found")
            return False
        except MoveTargetOutOfBoundsException:
            logger.info("Finished dragging")
            return True