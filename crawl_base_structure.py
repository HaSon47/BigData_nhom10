import time
import json, os
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException

def get_cat1_list(n=10, headless=False, output_file="cat_1_list.json"):
    options = uc.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-blink-features=AutomationControlled")
    #options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")

    driver = uc.Chrome(options=options, version_main=142)
    driver.set_window_size(1920, 1080)

    try:
        print("[INFO] Opening Shopee homepage...")
        driver.get("https://shopee.vn/")
        WebDriverWait(driver, 15).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.home-category-list__category-grid"))
        )
        time.sleep(2)

        print("[INFO] Extracting categories...")
        cats = []
        elements = driver.find_elements(By.CSS_SELECTOR, "a.home-category-list__category-grid")

        for i, a in enumerate(elements[:n]):
            try:
                name_el = a.find_element(By.CSS_SELECTOR, "div.Qwqg8J")
                name = name_el.text.strip()
            except Exception:
                name = None
            href = a.get_attribute("href")
            cats.append({"cat_1_name": name, "cat_1_url": href})
            print(f"{i+1}. {name} -> {href}")

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(cats, f, ensure_ascii=False, indent=2)

        print(f"\n✅ Saved {len(cats)} categories to {output_file}")
        return cats

    finally:
        driver.quit()

def init_driver():
    options = uc.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-gpu")
    driver = uc.Chrome(options=options, version_main=142)
    return driver


def get_cat2_list(cat_1_name, cat_1_url, data_dir="./data/shopee"):
    driver = init_driver()
    driver.get(cat_1_url)
    time.sleep(3)

    # Click nút “Thêm” nếu có
    try:
        toggle_btn = driver.find_element(By.CLASS_NAME, "shopee-category-list__toggle-btn")
        driver.execute_script("arguments[0].scrollIntoView(true);", toggle_btn)
        time.sleep(0.5)
        # Click an toàn bằng JavaScript
        driver.execute_script("arguments[0].click();", toggle_btn)
        time.sleep(1)
    except NoSuchElementException:
        pass
    except ElementClickInterceptedException:
        pass

    # Lấy toàn bộ các cat_2
    sub_categories = driver.find_elements(By.CLASS_NAME, "shopee-category-list__sub-category")
    print(f"Found {len(sub_categories)} sub-categories in {cat_1_name}")

    # Tạo folder cat_1
    cat_1_dir = os.path.join(data_dir, cat_1_name)
    os.makedirs(cat_1_dir, exist_ok=True)

    for sub in sub_categories:
        cat_2_name = sub.text.strip()
        cat_2_url = sub.get_attribute("href")
        if not cat_2_name or not cat_2_url:
            continue

        cat_2_dir = os.path.join(cat_1_dir, cat_2_name)
        os.makedirs(cat_2_dir, exist_ok=True)

        data = {
            "category": f"{cat_1_name}/{cat_2_name}",
            "url": cat_2_url
        }

        with open(os.path.join(cat_2_dir, "base_url.txt"), "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    driver.quit()
    print(f"✅ Done: {cat_1_name}")



if __name__ == "__main__":
    #get_cat1_list(n=10)
    path_cat_1 = r'C:\Users\Lenovo\Desktop\20251\BigData\code\BigData_nhom10\cat_1_list.json'
    with open(path_cat_1, 'r', encoding='utf-8') as f:
        list_cat_1 = json.load(f)

    for cat_1 in list_cat_1:
        get_cat2_list(
            cat_1_name=cat_1['cat_1_name'],
            cat_1_url=cat_1['cat_1_url'],
            data_dir="./data/shopee"
        )
