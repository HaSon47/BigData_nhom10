import json
import logging
import os
from abc import abstractmethod, ABC
import requests
import random
import re
import time
from typing import Any, Callable, Dict, Optional, Set
# from kafka import KafkaProducer, KafkaConsumer
import seleniumwire.undetected_chromedriver as uc
from selenium_stealth import stealth
from selenium.common.exceptions import WebDriverException
# try:
#     from selenium_stealth import stealth
# except Exception:  # pragma: no cover
#     stealth = None
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
        self.proxy_list = self.get_proxy_list()
        self._last_proxy = None
        self.current_proxy = None
        self.current_proxy_auth = None
        self._proxy_failures = 0
        self._max_proxy_failures_before_rotate = 1
        
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

    def get_proxy_list(self):
        """Load proxies from proxy.txt.

        Expected line format:
            IP:Port:User:Pass

        Returns:
            List[dict]: [{ip, port, user, password}]
        """
        proxy_file = os.path.join(os.path.dirname(__file__), "proxy.txt")
        proxies = []

        if not os.path.exists(proxy_file):
            logger.warning("Proxy file not found: %s", proxy_file)
            return []

        try:
            with open(proxy_file, "r", encoding="utf-8", errors="replace") as f:
                for raw in f:
                    line = (raw or "").strip()
                    if not line or line.startswith("#"):
                        continue

                    parts = [p.strip() for p in line.split(":")]
                    if len(parts) != 4:
                        logger.warning("Skip proxy (invalid format): %r", line)
                        continue

                    ip, port, user, password = parts
                    if not ip or not port.isdigit():
                        logger.warning("Skip proxy (invalid host/port): %r", line)
                        continue

                    proxies.append(
                        {
                            "ip": ip,
                            "port": port,
                            "user": user,
                            "password": password,
                        }
                    )
        except Exception as e:
            logger.exception("Failed to read proxy file %s: %s", proxy_file, e)
            return []

        logger.info("Loaded %d proxies from %s", len(proxies), proxy_file)
        return proxies

    def _pick_proxy(self):
        if not self.proxy_list:
            return None

        if len(self.proxy_list) == 1:
            return self.proxy_list[0]

        # Avoid picking the exact same proxy consecutively when possible
        for _ in range(5):
            p = random.choice(self.proxy_list)
            if p != self._last_proxy:
                self._last_proxy = p
                return p
        p = random.choice(self.proxy_list)
        self._last_proxy = p
        return p

    def _pick_proxy_excluding(self, excluded_proxy_auth: Set[str]):
        if not self.proxy_list:
            return None

        candidates = []
        for p in self.proxy_list:
            proxy_auth = f"{p.get('user', '')}:{p.get('password', '')}@{p.get('ip', '')}:{p.get('port', '')}"
            if proxy_auth not in excluded_proxy_auth:
                candidates.append(p)

        if not candidates:
            return None

        # Avoid picking the exact same proxy consecutively when possible
        if len(candidates) == 1:
            return candidates[0]
        for _ in range(5):
            p = random.choice(candidates)
            if p != self._last_proxy:
                self._last_proxy = p
                return p
        p = random.choice(candidates)
        self._last_proxy = p
        return p

    @staticmethod
    def _looks_like_proxy_error(exc: Exception) -> bool:
        msg = (str(exc) or "").lower()

        # Selenium Wire / upstream proxy auth
        if "invalid proxy server credentials" in msg:
            return True
        if "proxy authentication" in msg or "authentication required" in msg:
            return True
        if "407" in msg and "proxy" in msg:
            return True

        # Typical Chrome net errors when proxy is dead/blocked
        if "err_proxy_connection_failed" in msg:
            return True
        if "err_tunnel_connection_failed" in msg:
            return True
        if "err_connection_closed" in msg or "err_connection_reset" in msg:
            return True

        # Selenium/W3C timeouts sometimes wrap proxy failures
        if "timed out" in msg and "proxy" in msg:
            return True

        return False

    def _run_with_proxy_rotation(
        self,
        fn: Callable[[], Any],
        *,
        action: str,
        max_attempts: int = 3,
        sleep_between: float = 0.2,
    ) -> Any:
        last_exc: Optional[Exception] = None

        for attempt in range(1, max_attempts + 1):
            try:
                return fn()
            except Exception as e:
                last_exc = e

                # Only rotate when we strongly suspect proxy/auth/connectivity issues
                if self.current_proxy and self._looks_like_proxy_error(e):
                    self._proxy_failures += 1
                    logger.warning(
                        "Proxy failure (%s/%s) while %s. Current proxy=%s. Error=%s",
                        attempt,
                        max_attempts,
                        action,
                        self.current_proxy_auth,
                        e,
                    )

                    if self._proxy_failures >= self._max_proxy_failures_before_rotate:
                        self._proxy_failures = 0
                        self.restart_driver()
                        time.sleep(sleep_between)
                        continue

                raise

        raise RuntimeError(f"Failed after {max_attempts} attempts while {action}") from last_exc

    def _wrap_driver_methods(self, driver):
        """Wrap selected webdriver methods to auto-rotate proxies on failure.

        LazadaScraper calls `self.driver.get()` / `refresh()` directly.
        Wrapping here avoids modifying individual scrapers.
        """
        try:
            orig_get = driver.get

            def get_wrapped(url: str):
                return self._run_with_proxy_rotation(
                    lambda: orig_get(url),
                    action=f"driver.get({url})",
                    max_attempts=3,
                )

            driver.get = get_wrapped

            orig_refresh = driver.refresh

            def refresh_wrapped():
                return self._run_with_proxy_rotation(
                    orig_refresh,
                    action="driver.refresh()",
                    max_attempts=3,
                )

            driver.refresh = refresh_wrapped
        except Exception:
            # If wrapping fails for any reason, continue with the raw driver.
            logger.debug("Failed to wrap driver methods", exc_info=True)
        return driver

    def start_driver(self, profile_path=None):
        """Start Chrome driver using Selenium Wire with an authenticated proxy.

        This follows the same approach as `test_selenium_wire.py`:
        - Use `seleniumwire.webdriver.Chrome`
        - Configure proxy with user/pass in URL
        - Disable WebRTC UDP routes to prevent IP leaks
        """
        chrome_options = uc.ChromeOptions()

        chrome_options.set_capability("pageLoadStrategy", "eager")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--lang=vi-VN")
        # Fallback: avoid HTTPS interstitials if MITM cert isn't trusted in the profile
        chrome_options.add_argument("--ignore-certificate-errors")
        # chrome_options.add_argument("--window-size=1400,900")

        if self.is_headless:
            chrome_options.add_argument("--headless=new")

        # Prevent WebRTC from leaking the real IP
        prefs = {
            "webrtc.ip_handling_policy": "disable_non_proxied_udp",
            "webrtc.multiple_routes_enabled": False,
            "webrtc.nonproxied_udp_enabled": False,
        }
        chrome_options.add_experimental_option("prefs", prefs)

        seleniumwire_options_base = {
            "verify_ssl": False,
            "connection_timeout": 20,
        }

        # If a proxy is bad (wrong user/pass, expired, blocked), Selenium Wire will surface
        # errors that often show up as a Chrome auth prompt to 127.0.0.1:<port> (local SW proxy).
        # We retry with another proxy instead of deleting it from the list.
        excluded: Set[str] = set()
        max_start_attempts = 1
        if self.proxy_list:
            max_start_attempts = min(len(self.proxy_list), 8)

        last_exc: Optional[Exception] = None
        selected = None
        proxy_auth = None
        driver = None

        for attempt in range(1, max_start_attempts + 1):
            selected = self._pick_proxy_excluding(excluded)
            seleniumwire_options = dict(seleniumwire_options_base)

            if selected:
                ip = selected["ip"]
                port = selected["port"]
                user = selected["user"]
                password = selected["password"]
                proxy_auth = f"{user}:{password}@{ip}:{port}"
                seleniumwire_options["proxy"] = {
                    "http": f"http://{proxy_auth}",
                    "https": f"http://{proxy_auth}",
                    "no_proxy": "localhost,127.0.0.1",
                }
                logger.info("Starting driver with proxy: %s (attempt %s/%s)", proxy_auth, attempt, max_start_attempts)
            else:
                proxy_auth = None
                logger.warning("Starting driver WITHOUT proxy (attempt %s/%s)", attempt, max_start_attempts)

            try:
                driver = uc.Chrome(
                    options=chrome_options,
                    seleniumwire_options=seleniumwire_options,
                )
                # Success
                self.current_proxy = selected
                self.current_proxy_auth = proxy_auth
                driver = self._wrap_driver_methods(driver)
                break
            except Exception as e:
                last_exc = e
                if selected and self._looks_like_proxy_error(e):
                    excluded.add(proxy_auth)
                    logger.warning("Failed to start driver with proxy %s: %s", proxy_auth, e)
                    continue
                raise

        if driver is None:
            raise RuntimeError("Could not start driver with available proxies") from last_exc

        if stealth is not None:
            stealth(
                driver,
                languages=["vi-VN", "vi", "en-US", "en"],
                vendor="Google Inc.",
                platform="Win32",
                webgl_vendor="Intel Inc.",
                renderer="Intel Iris OpenGL Engine",
                fix_hairline=True,
            )
        driver.set_script_timeout(120)
        driver.set_page_load_timeout(30)
        try:
            driver.maximize_window()
        except Exception:
            pass
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
        try:
            self.driver.quit()
        except Exception:
            pass
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