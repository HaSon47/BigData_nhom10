from crawler.shopee_scraper import ShopeeScraper

scraper = ShopeeScraper(num_page_to_scrape=1, is_headless=False, data_dir=r'C:\Users\Lenovo\Desktop\20251\BigData\code\BigData_nhom10\data\shopee')
# if login = run browser in the background
scraper.get_main_page()
scraper.quit()
#scraper.get_product_urls()