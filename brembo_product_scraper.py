from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup


mapped_titles = {
    'Brake discs': 'disc',
    'Brake pads': 'pad',
    'UPGRADE brake pads': 'upgradepad',
    'Brake hoses': 'hydraulic',
    'Clutch master cylinders': 'hydraulic',
    'Brake master cylinders': 'hydraulic',
    'Remanufactured calipers': 'caliper',
    'Central clutch release devices': 'hydraulic',
    'Clutch cylinders': 'hydraulic',
    'UPGRADE brake discs': 'upgradedisc',
    'Brake pad accessories': 'padaccessory',
    'Shoes': 'brakeshoe',
    'UPGRADE GT kit': 'upgradekit',
    'Brake wheel cylinders': 'hydraulic',
    'Shoe kit': 'brakeshoekit',
    'Brake proportioning valves': 'hydraulic',
    'Drums': 'drum',
    'LCV calipers': 'lcvcaliper',
    'Disc and Pad Kit': 'discpadkit',
    'UPGRADE GT Disc kit': 'upgradedisc',
    'Caliper': 'caliper',
    'LCV caliper bracket': 'lcvbracket',
    'Clutch pipes': 'hydraulic',
}


def get_url(code, title):
    base_url = "https://www.bremboparts.com/europe/en/catalogue"
    url = f"{base_url}/{mapped_titles.get(title, 'unknown')}/{code.replace(' ', '_')}"
    return url

def save_unique_products(input_csv: str):
    df = pd.read_csv(input_csv)

    if df.size < 826000:
        return

    df_unique = df.drop_duplicates(subset='code').reset_index(drop=True)
    df_unique['product_id'] = df_unique.index + 1
    df_unique['url'] = df_unique.apply(lambda row: get_url(row['code'], row['title']), axis=1)

    out_df = df_unique[['product_id', 'code', 'title', 'url',]]
    return out_df

def scrape_products_df(url):
    """
    Given a URL pointing to a Brembo disc product page, fetches the page and returns
    a pandas DataFrame with one row. Columns are each technical specification label
    (under "Technical specifications") and "technical_image_url". If a spec is missing, its value is NaN.
    """
    resp = requests.get(url)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    type_div = soup.find("div", class_="cluster-tag inline big")
    type_val = type_div.get("data-type") if type_div and type_div.has_attr("data-type") else None

    product_image_url = None
    main_img = soup.select_one(".product-detail .image img")
    if main_img and main_img.has_attr("src"):
        product_image_url = urljoin(url, main_img["src"])

    # Locate the <div class="technical-data"> block
    tech_block = soup.find("div", class_="technical-data")
    if tech_block is None:
        raise RuntimeError("Could not find the technical-data section on the page.")

    # Locate the specs container for labels and details
    specs_container = tech_block.find("div", class_="data")
    if specs_container is None:
        raise RuntimeError("Could not find the specs container inside technical-data.")

    specs = {}
    # Each spec is in <div class="item"> with children <div class="label"> and <div class="detail">
    for item in specs_container.find_all("div", class_="item"):
        label_div = item.find("div", class_="label")
        detail_div = item.find("div", class_="detail")
        if label_div and detail_div:
            label = label_div.get_text(strip=True)
            detail = detail_div.get_text(separator=" ", strip=True)
            specs[label] = detail

    # Find the image URL under technical-data
    technical_image_div = tech_block.find("div", class_="image")
    technical_image_url = None
    if technical_image_div:
        img_tag = technical_image_div.find("img")
        if img_tag and img_tag.has_attr("src"):
            technical_image_url = urljoin(url, img_tag["src"])

    row_data = {"type": type_val, **specs, "image_url": product_image_url, "technical_image_url": technical_image_url}
    df = pd.DataFrame([row_data])
    return df

def scrape_all_products_by_type(input_dataframe, output_csv: str, product_type: str):
    """
    Reads the input CSV, filters rows where title == "Brake discs", and scrapes each URL
    concurrently using threads. Returns a combined DataFrame of all scraped specs with
    product_id included, and saves it to output_csv.

    :param input_csv: Path to the CSV file containing 'product_id', 'title', and 'url' columns.
    :param output_csv: Path where the combined CSV should be saved.
    :param max_workers: Number of threads to use for concurrent scraping.
    """
    df = input_dataframe

    df_brake = df[df["title"] == product_type].reset_index(drop=True)

    df_brake["product_id"] = range(1, len(df_brake) + 1)

    results = []

    def worker(pid, code, url):
        print(f"Worker {pid} started. ({product_type})")
        df_result = scrape_products_df(url)
        df_result["product_id"] = pid
        df_result["code"] = code
        return df_result

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_pid = {
            executor.submit(worker, row["product_id"], row["code"], row["url"]): row["product_id"]
            for _, row in df_brake.iterrows()
        }
        for future in as_completed(future_to_pid):
            try:
                df_result = future.result()
            except Exception:
                continue
            results.append(df_result)

    if results:
        combined = pd.concat(results, ignore_index=True, sort=False)
        combined = combined.sort_values("product_id", ignore_index=True)
        cols = ["product_id", "code"] + [c for c in combined.columns if c not in ("product_id", "code")]
        combined = combined[cols]
        combined.to_csv(output_csv, index=False)
        return combined
    else:
        return pd.DataFrame()


def main():
    products_df = save_unique_products("Data/Products/product-relations.csv")
    for title in list(mapped_titles.keys()):
        file_name = f"{title.lower().replace(" ", "_")}.csv"
        scrape_all_products_by_type(products_df, f"Data/Products/{file_name}", title)

if __name__ == '__main__':
    main()