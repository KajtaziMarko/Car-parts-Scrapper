import os
import json
import pandas as pd
from apify_shared.utils import json_dumps
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

URL = "https://www.bremboparts.com/europe/en"


def load_page(driver, wait):
    driver.get(URL)
    # Wait until the BrandCode element is present
    wait.until(EC.presence_of_element_located((By.ID, "BrandCode")))


def close_popup(driver, wait, section, t=10):
    # Wait until the close button is clickable in the given section and click it
    close_button = WebDriverWait(driver, t).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, f"{section} button.close"))
    )
    close_button.click()


def select_brand(driver, wait, brand):
    brand_input = wait.until(EC.element_to_be_clickable((By.ID, "BrandCode")))
    brand_input.click()
    xpath = f"//div[@class='item search-result']/span[text()='{brand}']"
    brand_elem = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
    brand_elem.click()


def select_model(driver, wait, model_name):
    model_input = wait.until(EC.element_to_be_clickable((By.ID, "ModelCode")))
    model_input.click()
    # Use a contains xpath to match the model name
    xpath = f"//div[@class='item search-result']/span[contains(., '{model_name}')]"
    model_elem = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
    model_elem.click()


def select_type(driver, wait, type_data):
    type_input = wait.until(EC.element_to_be_clickable((By.ID, "TypeCode")))
    type_input.click()
    # Use the engine displacement (first part of the name) for matching
    engine = type_data["name"].split()[0]
    xpath = (
        f"//div[contains(@class, 'row search-result') and "
        f".//span[contains(@class, 'type-name') and starts-with(normalize-space(text()), '{engine}')] and "
        f".//span[contains(@class, 'kw') and contains(normalize-space(text()), '{type_data['kw']}')] and "
        f".//span[contains(@class, 'cv') and contains(normalize-space(text()), '{type_data['cv']}')] and "
        f".//span[contains(@class, 'date') and contains(normalize-space(text()), '{type_data['date']}')]]"
    )
    type_elem = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
    type_elem.click()


def get_all_brands(driver, wait):
    load_page(driver, wait)
    brand_input = wait.until(EC.element_to_be_clickable((By.ID, "BrandCode")))
    brand_input.click()
    brand_container = wait.until(EC.visibility_of_element_located(
        (By.CSS_SELECTOR, 'div[data-type="brand"].white-exp.menu')
    ))
    brand_elements = brand_container.find_elements(By.CSS_SELECTOR, "div.item.search-result span.voice")
    brands = [elem.text.strip() for elem in brand_elements if elem.text]
    return brands


def get_all_models(driver, wait, brand):
    load_page(driver, wait)
    select_brand(driver, wait, brand)
    model_input = wait.until(EC.element_to_be_clickable((By.ID, "ModelCode")))
    model_input.click()
    model_container = wait.until(EC.visibility_of_element_located(
        (By.CSS_SELECTOR, 'div[data-type="model"].white-exp.menu')
    ))
    model_elements = model_container.find_elements(By.CSS_SELECTOR, "div.item.search-result span.voice")
    models = []
    seen = set()
    for elem in model_elements:
        full_text = elem.text.strip()
        try:
            # Extract the date from the inner span
            date = elem.find_element(By.TAG_NAME, "span").text.strip()
        except Exception:
            date = ""
        # Remove the date from the full text to get the model name
        if date and date in full_text:
            name = full_text.replace(date, "").strip()
        else:
            name = full_text
        identifier = (name, date)
        if identifier not in seen:
            seen.add(identifier)
            models.append({"model_name": name, "model_date": date})
    return models


def get_all_types(driver, wait, brand, model):
    load_page(driver, wait)
    select_brand(driver, wait, brand)
    select_model(driver, wait, model["model_name"])
    type_input = wait.until(EC.element_to_be_clickable((By.ID, "TypeCode")))
    type_input.click()
    rows = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.row.search-result")))
    types = []
    for row in rows:
        type_name = row.find_element(By.CSS_SELECTOR, "span.col.type-name").text.strip()
        kw = row.find_element(By.CSS_SELECTOR, "span.col.kw").text.strip()
        cv = row.find_element(By.CSS_SELECTOR, "span.col.cv").text.strip()
        date = row.find_element(By.CSS_SELECTOR, "span.col.date").text.strip()
        formatted = {"name": type_name, "kw": kw, "cv": cv, "date": date}
        types.append(formatted)
    return types


def click_search_and_get_url(driver, wait, brand, model, type_data):
    select_brand(driver, wait, brand)
    select_model(driver, wait, model["model_name"])
    select_type(driver, wait, type_data)
    current_url = driver.current_url
    search_button = wait.until(EC.element_to_be_clickable((By.ID, "SubmitType")))
    search_button.click()
    wait.until(EC.url_changes(current_url))
    return driver.current_url


def append_to_csv(file_name, row):
    """Appends a single row (a dict) to a CSV file. Writes header if file does not exist."""
    file_exists = os.path.exists(file_name)
    mode = 'a' if file_exists else 'w'
    df = pd.DataFrame([row])
    df.to_csv(file_name, mode=mode, index=False, header=not file_exists)


def main():
    options = webdriver.ChromeOptions()
    # Uncomment the following line to run in headless mode:
    # options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 10)

    # Load the initial page and close popups
    load_page(driver, wait)
    close_popup(driver, wait, "div.cookie-toast")
    close_popup(driver, wait, "div.wrapper")

    # Get list of brands and close any modal popup if present
    brands = get_all_brands(driver, wait)
    close_popup(driver, wait, "div.modal.link.show", 40)

    # Limit to the first three brands for testing
    # brands = brands[:2]
    
    # Remove any existing CSV files to start fresh
    for file in ["Data/brands.csv", "Data/models.csv", "Data/types.csv"]:
        if os.path.exists(file):
            os.remove(file)

    # Counters for unique IDs
    model_id_counter = 1
    type_id_counter = 1

    # Process each brand, model, and type while saving each step immediately
    for brand_index, brand in enumerate(brands, start=1):
        brand_id = brand_index
        brand_record = {"brand_id": brand_id, "brand_name": brand}
        append_to_csv("Data/brands.csv", brand_record)

        models = get_all_models(driver, wait, brand)
        for model in models:
            current_model_id = model_id_counter
            model_record = {
                "model_id": current_model_id,
                "brand_id": brand_id,
                "model_name": model["model_name"],
                "model_date": model["model_date"]
            }
            append_to_csv("Data/models.csv", model_record)

            types = get_all_types(driver, wait, brand, model)
            for type_data in types:
                # Reload the page and perform the search for each type
                load_page(driver, wait)
                url = click_search_and_get_url(driver, wait, brand, model, type_data)
                type_record = {
                    "type_id": type_id_counter,
                    "model_id": current_model_id,
                    "type_name": type_data["name"],
                    "kw": type_data["kw"],
                    "cv": type_data["cv"],
                    "date": type_data["date"],
                    "url": url
                }
                append_to_csv("Data/types.csv", type_record)
                type_id_counter += 1
            model_id_counter += 1

    print("Saved brands.csv, models.csv, and types.csv for the first three brands.")
    driver.quit()


if __name__ == "__main__":
    main()
