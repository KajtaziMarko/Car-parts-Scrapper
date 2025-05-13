import os
import csv
import re
import requests
from functools import lru_cache

from numpy.matlib import empty
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor, as_completed
from apify_shared.utils import json_dumps


class BremboAPIClient:
    """
    HTTP client for interacting with the Brembo catalogue API.
    Handles session, CSRF token, and regional settings.

    Falls back to loading a local HTML file if network is unavailable.
    """
    def __init__(self, base_url: str, region: str, culture: str, country: str, offline_html: str = None):
        self.base_url = f"{base_url.rstrip('/')}/{region}/{culture}"
        self.session = requests.Session()
        # increase connection pool for performance
        adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50)
        self.session.mount('https://', adapter)
        self.offline_html = offline_html
        self.session.cookies.set(name="cnt", value=country, domain="www.bremboparts.com", path="/")
        self._initialize_session()

    def _initialize_session(self):
        html = None
        try:
            resp = self.session.get(self.base_url, timeout=10)
            resp.raise_for_status()
            html = resp.text
        except requests.RequestException:
            if not self.offline_html:
                raise RuntimeError("Cannot fetch home page and no offline HTML provided.")
            with open(self.offline_html, 'r', encoding='utf-8') as f:
                html = f.read()
        match = re.search(r'<input name="__RequestVerificationToken"[^>]*value="([^"]+)"', html)
        if not match:
            raise RuntimeError("CSRF token not found in HTML.")
        token = match.group(1)
        self.session.headers.update({
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Content-Type': 'application/json; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest',
            'RequestVerificationToken': token,
            'Referer': self.base_url,
        })

    def post_json(self, endpoint: str, payload: dict):
        url = self.base_url + endpoint
        resp = self.session.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        return resp.json()


class VehicleService:
    """
    Service for fetching brands, models, types, displacements, years, and product URLs.
    """
    ENDPOINTS = {
        'brands':      '/search/getsearchbrands',
        'models':      '/search/getsearchmodels',
        'types':       '/catalogue/search/getsearchtypes',
        'displacement':'/catalogue-bike/search/getsearchccms',
        'year':        '/catalogue-bike/search/getsearchyears',
        'search':      '/search/searchtype'
    }

    def __init__(self, client: BremboAPIClient):
        self.client = client

    @lru_cache(maxsize=None)
    def fetch_brands(self, vehicle: str) -> list:
        prefix = '/catalogue-bike' if vehicle == 'Bike' else '/catalogue'
        return self.client.post_json(prefix + self.ENDPOINTS['brands'], {'vehicleType': vehicle})

    @lru_cache(maxsize=None)
    def fetch_models(self, vehicle: str, brand_key: str) -> tuple:
        prefix = '/catalogue-bike' if vehicle == 'Bike' else '/catalogue'
        body = {'vehicleType': vehicle, 'modelYear': None}
        if vehicle == 'Bike':
            body['brandName'] = brand_key
        else:
            body['brandCode'] = brand_key
        return tuple(self.client.post_json(prefix + self.ENDPOINTS['models'], body))

    @lru_cache(maxsize=None)
    def fetch_types(self, model_code: str) -> tuple:
        return tuple(self.client.post_json(self.ENDPOINTS['types'], {'modelCode': model_code}))

    def fetch_displacement(self, brand_name: str, model_name: str, type_name: str) -> list:
        return self.client.post_json(
            self.ENDPOINTS['displacement'],
            {'brandName': brand_name, 'modelName': model_name, 'typeName': type_name}
        )

    @lru_cache(maxsize=None)
    def fetch_year(self, type_code: str) -> tuple:
        if not type_code:
            return ()
        return tuple(self.client.post_json(self.ENDPOINTS['year'], {'typeCode': type_code}))

    def fetch_product_url(self, vehicle: str, **kwargs) -> dict:
        prefix = '/catalogue-bike' if vehicle == 'Bike' else '/catalogue'
        try:
            return self.client.post_json(prefix + self.ENDPOINTS['search'], kwargs)
        except Exception:
            return {'url': ''}


def save_all_csvs(brands, models, types, displacements, years, out_dir='Data'):
    os.makedirs(out_dir, exist_ok=True)
    def write_csv(fname, hdrs, rows):
        with open(os.path.join(out_dir, fname), 'w', newline='', encoding='utf-8') as f:
            w = csv.writer(f)
            w.writerow(hdrs)
            w.writerows(rows)
    write_csv('brand.csv', ['brand_id','brand_name','brembo_brand_code','vehicle_type'], brands)
    write_csv('model.csv', ['model_id','brand_id','brembo_model_code','model_name','date_start','date_end'], models)
    write_csv('type.csv', ['type_id','model_id','type_name','brembo_type_code','date_start','date_end','kw','cv','product_url'], types)
    write_csv('bikeDisplacement.csv', ['disp_id','model_id','title','value','brembo_disp_code','product_url'], displacements)
    write_csv('bikeYear.csv', ['year_id','disp_id','year_value'], years)


def main():
    base_url = 'https://www.bremboparts.com'
    region = 'europe'
    culture = 'en'
    country = 'MK'
    vehicle_types = [('Car', 1), ('Truck', 2), ('Bike', 3)]

    client  = BremboAPIClient(base_url, region, culture, country)
    service = VehicleService(client)

    brands_data, models_data, types_data, disp_data, year_data = [],[],[],[],[]
    todos = []  # collect URL jobs
    b_id = m_id = t_id = d_id = y_id = 1

    # first gather all static data
    for vehicle,_ in vehicle_types:
        for b in service.fetch_brands(vehicle):
            brand_name = b.get('brandName') or b.get('title') or ''
            brand_code = b.get('brandCode') or brand_name
            brands_data.append((b_id, brand_name, brand_code, vehicle))

            for m in service.fetch_models(vehicle, brand_code if vehicle!='Bike' else brand_name):
                model_code = m.get('modelCode') or m.get('value') or m.get('title','')
                model_name = m.get('modelName') or m.get('title') or ''
                start = m.get('modelDateStart')
                end   = m.get('modelDateEnd')
                models_data.append((m_id, b_id, model_code, model_name, start, end))

                if vehicle=='Bike':
                    type_name = m.get('typeName') or model_name
                    disps = service.fetch_displacement(brand_name, m.get('modelName') or '', m.get('typeName') or '')
                    for d in disps:
                        d_code = d.get('typeCode') or ''
                        if not d_code: continue
                        d_title = d.get('title')
                        d_val   = d.get('value')

                        try:
                            years = service.fetch_year(d_code)
                        except requests.HTTPError as e:
                            print(f"[WARN] skipping years for {brand_name, model_name!r}: {e}")
                            years = ()
                        for y in years:
                            y_val = y.get('value')
                            year_data.append((y_id, d_id, y_val))
                            y_id+=1

                        try:
                            years[-1].get('value')
                        except:
                            continue

                        disp_data.append((d_id, m_id, d_title, d_val, d_code, ''))
                        todos.append((
                            vehicle,
                            {'brandName': brand_name,
                             'modelName': m.get('modelName') or '',
                             'typeName':  m.get('typeName') or '',
                             'ccm': d_val,
                             'typeCode': d_code,
                             'year': years[-1].get('value'),
                             'productTypes': ['All'],
                             'out': ('disp',d_id)}
                        ))
                        d_id+=1

                else:
                    for t in service.fetch_types(model_code):
                        t_code = t.get('typeCode') or t.get('value') or ''
                        if not t_code: continue
                        t_name = t.get('typeName') or t.get('title') or ''
                        t_start= t.get('typeDateStart'); t_end=t.get('typeDateEnd'); t_kw=t.get('kw'); t_cv=t.get('cv')
                        types_data.append((t_id, m_id, t_name, t_code, t_start, t_end, t_kw, t_cv, '')); todos.append((vehicle, {'brandCode':brand_code,'modelCode':model_code,'typeCode':t_code,'productTypes':['All'],'out':('type',t_id)})); t_id+=1
                m_id+=1
            b_id+=1
            print("Done with: ", brand_name, vehicle)

    # parallel fetch URLs
    def job(item):
        vehicle, payload = item
        out_type, out_id = payload.pop('out')
        res = service.fetch_product_url(vehicle, **payload) if vehicle!='Bike' else service.fetch_product_url(vehicle, **payload)

        # print(f"[DEBUG] payload={payload!r} → url={res.get('url','')!r}")
        return (out_type, out_id, res.get('url',''))

    with ThreadPoolExecutor(max_workers=20) as pool:
        for out_type,out_id,url in pool.map(job, todos):
            if out_type=='type': types_data[out_id-1] = types_data[out_id-1][:-1] + (url,)
            else: disp_data[out_id-1] = disp_data[out_id-1][:-1] + (url,)

    save_all_csvs(brands_data, models_data, types_data, disp_data, year_data)
    print(f"Done: {len(brands_data)} brands, {len(models_data)} models, {len(types_data)} types, {len(disp_data)} disp, {len(year_data)} years")

if __name__=='__main__':
    main()
