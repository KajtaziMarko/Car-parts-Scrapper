import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import time

URL = "https://www.bremboparts.com/europe/en"


def load_page(driver, wait):
    driver.get(URL)
    # Wait until the BrandCode element is present
    wait.until(EC.presence_of_element_located((By.ID, "BrandCode")))
    # time.sleep(2)


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


def select_model(driver, wait, model_data):
    model_input = wait.until(EC.element_to_be_clickable((By.ID, "ModelCode")))
    model_input.click()
    # Use a contains xpath to match the model name
    xpath = f"//div[@class='item search-result']/span[contains(., '{model_data["model_name"]}')]"
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

def input_brand(driver, wait, brand):
    brand_input = wait.until(EC.element_to_be_clickable((By.ID, "BrandCode")))
    brand_input.click()
    brand_input.clear()
    brand_input.send_keys(brand)

def input_model(driver, wait, model_data):
    formatted_model = f"{model_data["model_name"]} {model_data['model_date']}"
    model_input = wait.until(EC.element_to_be_clickable((By.ID, "ModelCode")))
    model_input.click()
    model_input.clear()
    model_input.send_keys(formatted_model)

def input_type(driver, wait, type_data):
    formatted_type = f"{type_data['name']} ({type_data['kw']} kW/{type_data['cv']} CV) {type_data['date']}"
    type_input = wait.until(EC.element_to_be_clickable((By.ID, "TypeCode")))
    type_input.click()
    type_input.clear()
    type_input.send_keys(formatted_type)

def input_and_select_brand(driver, wait, brand):
    input_brand(driver, wait, brand)
    select_brand(driver, wait, brand)

def input_and_select_model(driver, wait, model_data):
    input_model(driver, wait, model_data)
    select_model(driver, wait, model_data)

def input_and_select_type(driver, wait, type_data):
    input_type(driver, wait, type_data)
    select_type(driver, wait, type_data)


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
    # select_brand(driver, wait, brand)
    input_and_select_brand(driver, wait, brand)
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
    # select_brand(driver, wait, brand)
    input_and_select_brand(driver, wait, brand)
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
    # First, try to use the select functions.
    try:
        load_page(driver, wait)

        select_brand(driver, wait, brand)
        # input_and_select_brand(driver, wait, brand)

        # select_model(driver, wait, model["model_name"])
        input_and_select_model(driver, wait, model)

        # select_type(driver, wait, type_data)
        input_and_select_type(driver, wait, type_data)
    except Exception as select_exception:
        print("Select functions failed, trying input functions:", select_exception)
        # If the select approach fails, try the input functions.
        try:
            load_page(driver, wait)
            input_brand(driver, wait, brand)
            input_model(driver, wait, model)
            input_type(driver, wait, type_data)
        except Exception as input_exception:
            print("Input functions also failed:", input_exception)
            print("Brand:", brand, "Model:", model, "Type:", type_data)
            return ""

    # After successfully setting the values, click the search button.
    try:
        current_url = driver.current_url
        search_button = wait.until(EC.element_to_be_clickable((By.ID, "SubmitType")))
        search_button.click()
        wait.until(EC.url_changes(current_url))
        return driver.current_url
    except Exception as e:
        print("Error clicking search or waiting for URL change:", e)
        print("Brand:", brand, "Model:", model, "Type:", type_data)
        return ""


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

    # Ensure Data folder exists; if it does, adjust checkpoint data.
    if not os.path.exists("Data"):
        os.makedirs("Data")
    else:
        if os.path.exists("Data/brands.csv"):
            brands_csv = pd.read_csv("Data/brands.csv")
            if not brands_csv.empty:
                last_brand = brands_csv.iloc[-1]["brand_name"]
                # Remove rows in models and types that belong to the last brand.
                if os.path.exists("Data/models.csv"):
                    models_df = pd.read_csv("Data/models.csv")
                    # Get the last brand's id from brands.csv
                    last_brand_row = brands_csv[brands_csv["brand_name"] == last_brand]
                    if not last_brand_row.empty:
                        last_brand_id = last_brand_row.iloc[-1]["brand_id"]
                        # Find model_ids for rows that belong to the last brand.
                        models_to_remove = models_df[models_df["brand_id"] == last_brand_id]
                        model_ids_to_remove = models_to_remove["model_id"].tolist()
                        # Remove these rows and update models.csv
                        models_df = models_df[models_df["brand_id"] != last_brand_id]
                        models_df.to_csv("Data/models.csv", index=False)
                        # Now remove rows in types.csv whose model_id is in the removed list.
                        if os.path.exists("Data/types.csv"):
                            types_df = pd.read_csv("Data/types.csv")
                            types_df = types_df[~types_df["model_id"].isin(model_ids_to_remove)]
                            types_df.to_csv("Data/types.csv", index=False)

                # Remove the last brand from the csv
                brands_csv = brands_csv[brands_csv["brand_name"] != last_brand]
                brands_csv.to_csv("Data/brands.csv", index=False)

                # Adjust the brands list to resume from the last processed brand.
                try:
                    index = brands.index(last_brand)
                    brands = brands[index:]
                except ValueError as e:
                    print(e)

    # Initialize ID counters based on existing CSV files.
    if os.path.exists("Data/brands.csv"):
        brands_df = pd.read_csv("Data/brands.csv")
        if not brands_df.empty:
            next_brand_id = int(brands_df["brand_id"].max()) + 1
        else:
            next_brand_id = 1
    else:
        next_brand_id = 1

    if os.path.exists("Data/models.csv"):
        models_df = pd.read_csv("Data/models.csv")
        if not models_df.empty:
            next_model_id = int(models_df["model_id"].max()) + 1
        else:
            next_model_id = 1
    else:
        next_model_id = 1

    if os.path.exists("Data/types.csv"):
        types_df = pd.read_csv("Data/types.csv")
        if not types_df.empty:
            next_type_id = int(types_df["type_id"].max()) + 1
        else:
            next_type_id = 1
    else:
        next_type_id = 1

    # Process each brand, model, and type while saving each step immediately
    for brand in brands:
        brand_id = next_brand_id
        next_brand_id += 1
        brand_record = {"brand_id": brand_id, "brand_name": brand}
        append_to_csv("Data/brands.csv", brand_record)

        models = get_all_models(driver, wait, brand)
        for model in models:
            current_model_id = next_model_id
            next_model_id += 1
            model_record = {
                "model_id": current_model_id,
                "brand_id": brand_id,
                "model_name": model["model_name"],
                "model_date": model["model_date"]
            }
            append_to_csv("Data/models.csv", model_record)

            types = get_all_types(driver, wait, brand, model)
            for type_data in types:
                url = click_search_and_get_url(driver, wait, brand, model, type_data)
                type_record = {
                    "type_id": next_type_id,
                    "model_id": current_model_id,
                    "type_name": type_data["name"],
                    "kw": type_data["kw"],
                    "cv": type_data["cv"],
                    "date": type_data["date"],
                    "url": url
                }
                append_to_csv("Data/types.csv", type_record)
                next_type_id += 1

    print("Saved brands.csv, models.csv, and types.csv for the processed brands.")
    driver.quit()


if __name__ == "__main__":
    main()
