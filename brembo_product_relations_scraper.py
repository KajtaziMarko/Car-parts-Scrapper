import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import time
import requests

def robust_get(url, retries=3, base_sleep=0.2):
    for i in range(retries):
        try:
            resp = requests.get(url, timeout=20, headers={'User-Agent': 'Mozilla/5.0'})
            if resp.status_code == 200:
                return resp
            else:
                print(f"Error {resp.status_code} on {url} (try {i+1}/{retries})")
        except Exception as e:
            print(f"Exception on {url}: {e} (try {i+1}/{retries})")
        sleep_time = base_sleep * (2 ** i)
        print(f"Sleeping {sleep_time:.2f}s before retry...")
        time.sleep(sleep_time)
    return None

def extract_codes_from_url(full_url):
    resp = robust_get(full_url)
    if not resp:
        return []
    soup = BeautifulSoup(resp.text, 'html.parser')
    codes_per_group = []
    for group in soup.select('.products-group'):
        title_tag = group.select_one('.title .label')
        group_title = title_tag.text.strip() if title_tag else ''
        for code_div in group.select('.codes-list .code'):
            code = code_div.text.strip()
            if code:
                codes_per_group.append((code, group_title))

    if not codes_per_group:
        print(f"[WARN] No product groups found on {full_url}")

    return codes_per_group

def process_row(row, id_col):
    type_id = str(row[id_col])
    product_url = str(row['product_url'])
    base_url = "https://www.bremboparts.com"
    full_url = product_url if product_url.startswith('http') else base_url + product_url
    print(f"Processing: {full_url} ({id_col}={type_id})")
    code_title_pairs = extract_codes_from_url(full_url)

    if id_col == 'disp_id':
        return [{'disp_id': type_id, 'code': code, 'title': group_title} for code, group_title in code_title_pairs]

    return [{'type_id': type_id, 'code': code, 'title': group_title} for code, group_title in code_title_pairs]

def process_dataframe(df, id_col):
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_row, row, id_col): idx for idx, row in df.iterrows()}
        for future in as_completed(futures):
            try:
                results.extend(future.result())
            except Exception as e:
                print(f"Exception in worker: {e}")
    return results

def main(type_csv_path, displacement_csv_path, output_type_csv_path, output_bike_csv_path):
    types = pd.read_csv(type_csv_path)
    displacements = pd.read_csv(displacement_csv_path)

    type_results = process_dataframe(types, 'type_id')
    disp_results = process_dataframe(displacements, 'disp_id')

    scraped_type_ids = {r['type_id'] for r in type_results}
    scraped_disp_ids = {r['disp_id'] for r in disp_results}

    all_type_ids = set(types['type_id'].astype(str))
    all_disp_ids = set(displacements['disp_id'].astype(str))

    missing_type_ids = all_type_ids - scraped_type_ids
    missing_disp_ids = all_disp_ids - scraped_disp_ids

    print("Missing type_ids:", missing_type_ids)
    print("Missing disp_ids:", missing_disp_ids)

    if missing_type_ids:
        print("Retrying missing type_ids:", missing_type_ids)
        retry_types = types[types['type_id'].astype(str).isin(missing_type_ids)]
        retry_types_results = process_dataframe(retry_types, 'type_id')
        type_results.extend(retry_types_results)

    if missing_disp_ids:
        print("Retrying missing disp_ids:", missing_disp_ids)
        retry_disps = displacements[displacements['disp_id'].astype(str).isin(missing_disp_ids)]
        retry_disps_results = process_dataframe(retry_disps, 'disp_id')
        disp_results.extend(retry_disps_results)

    pd.DataFrame(type_results) \
        .sort_values(by="type_id") \
        .to_csv(output_type_csv_path, index=False, encoding='utf-8')
    print(f"Done! {len(type_results)} rows written to {output_type_csv_path}")

    pd.DataFrame(disp_results) \
        .sort_values(by="disp_id") \
        .to_csv(output_bike_csv_path, index=False, encoding='utf-8')
    print(f"Done! {len(disp_results)} rows written to {output_bike_csv_path}")

if __name__ == "__main__":
    main('Data/type.csv',
         'Data/bikeDisplacement.csv',
         'Data/Products/product-relations.csv',
         'Data/Products/bike-product-relations.csv')