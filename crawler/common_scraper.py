import json
import logging
import os
from abc import abstractmethod, ABC
from selenium.webdriver.chrome.options import Options

import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.chrome.service import Service

# from kafka import KafkaProducer, KafkaConsumer

logger = logging.getLogger(__name__)
BOOTSTRAP_SERVERS = ['viet:9092', 'jazzdung:9093', 'dungbruh:9094']
# BOOTSTRAP_SERVERS = ['localhost:9092', 'localhost:9093']


class CommonScraper(ABC):

    def __init__(self, num_page_to_scrape, data_dir, wait_timeout, retry_num, restart_num, is_headless, main_page,
                 info_topic, url_topic, consumer_group, consumer_id):
        self.restart_num = restart_num
        self.num_page_to_scrape = num_page_to_scrape
        self.data_dir = data_dir
        self.wait_timeout = wait_timeout
        self.retry_num = retry_num
        self.is_headless = is_headless
        self.main_page = main_page
        self.info_topic = info_topic
        self.url_topic = url_topic
        self.driver = self.start_driver()
        # self.url_producer = KafkaProducer(
        #     bootstrap_servers=BOOTSTRAP_SERVERS,
        #     # key_serializer=str.encode,
        #     value_serializer=lambda v: json.dumps(
        #         v, ensure_ascii=False).encode('utf-8'),
        #     batch_size=1000,
        #     linger_ms=5,
        #     acks=1,
        #     request_timeout_ms=1000
        # )
        # self.info_producer = KafkaProducer(
        #     bootstrap_servers=BOOTSTRAP_SERVERS,
        #     # key_serializer=str.encode,
        #     value_serializer=lambda v: json.dumps(
        #         v, ensure_ascii=False).encode('utf-8'),
        #     batch_size=1000,
        #     linger_ms=5,
        #     acks=1,
        #     request_timeout_ms=1000
        # )
        # self.url_consumer = KafkaConsumer(
        #     bootstrap_servers=BOOTSTRAP_SERVERS,
        #     value_deserializer=lambda v: v.decode('utf-8'),
        #     group_id="url_scraper",
        #     client_id=consumer_id
        # )
        # self.url_consumer.subscribe(self.url_topic)

    def get_main_page(self):
        self.driver.get(self.main_page)
        logger.info(
            "Main page opened, after finishing logging in, press any key to continue...")
        input()

    def start_driver(self):
        #Khởi tạo driver bằng undetected_chromedriver
        options = uc.ChromeOptions()
        options.set_capability("pageLoadStrategy", "none")
        if self.is_headless:
            options.headless = True

        if not os.path.exists("./profile"):
            os.mkdir("./profile")

        # driver = uc.Chrome(options=options, user_data_dir="./profile")
        driver = uc.Chrome(options=options)
        driver.set_script_timeout(10)
        driver.set_page_load_timeout(20)
        driver.maximize_window()
       
        #Khởi tạo driver bằng selenium
        # options = Options()
        # options.add_argument('--no-sandbox')
        # options.add_argument("--window-position=0,0")
        # options.add_argument('--no-verify')
        # service = Service(
        #     executable_path='/home/vaipe/anhtn/anhtn_test/dexscreener-reaction/utils/chromedriver')
        # driver = webdriver.Chrome(service=service, options=options)
        # driver.execute_cdp_cmd('Network.setUserAgentOverride', {
        #     "userAgent": "python 2.7", "platform": "Windows"})
        # agent = driver.execute_script("return navigator.userAgent")
        return driver

    def write_to_file(self, text, file_name):
        full_path = os.path.join(self.data_dir, file_name)
        with open(full_path, 'a') as f:
            f.write(text + '\n')

    def log_curr_url_num(self, category_path, num):
        full_path = os.path.join(category_path, "_CURRENT_URL")
        with open(full_path, 'w') as f:
            f.write(str(num))

    def get_curr_url_num(self, category_path):
        full_path = os.path.join(category_path, "_CURRENT_URL")
        if not os.path.exists(full_path):
            return 0

        with open(full_path, 'r') as f:
            num = int(f.read())
            return num

    def log_done_info(self, category_path):
        full_path = os.path.join(category_path, "_DONE")
        with open(full_path, 'w') as f:
            pass

    def check_done_info(self, category_path):
        full_path = os.path.join(category_path, "_DONE")
        return os.path.exists(full_path)

    @abstractmethod
    def get_product_urls(self):
        pass

    @abstractmethod
    def get_product_info(self):
        pass

    def restart_driver(self):
        self.driver.quit()
        self.driver = self.start_driver()
        logger.info('Driver restarted')

    def send_to_kafka(self, value, mode):
        if mode == 'url':
            self.url_producer.send(
                topic=self.url_topic,
                value=value
            )
        elif mode == 'info':
            self.info_producer.send(
                topic=self.info_topic,
                value=value
            )

    # def __del__(self):
    #     self.driver.quit()