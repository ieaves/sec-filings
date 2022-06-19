
from bs4 import BeautifulSoup as bs
import requests
import itertools
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import os


class RequestSession:
    def __init__(self, email):
        self.headers = {
            'User-Agent': email,
        }

        retry_strategy = Retry(
            total=4,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        http = requests.Session()
        http.mount("https://", adapter)
        http.mount("http://", adapter)
        self.http = http

    def get(self, url):
        return self.http.get(url, headers=self.headers)


class SECArchiver:
    daily_index = 'https://www.sec.gov/Archives/edgar/daily-index'

    def daily_index_url(self, quarter, year):
        return f'{self.daily_index}/{year}/QTR{quarter}/'


def get_masterfile_list(base_url, session):
    resp = session.get(base_url)

    if resp.status_code in [403, 404]:
        # Does not exist or was not found
        return {}

    f = bs(resp.text, features="html.parser").find_all('a', href=True)
    master_list = {item['href']: f"{base_url}{item['href']}" for item in f if item['href'].startswith('master')}
    return master_list


def download_masters(year, quarter, session):
    archiver = SECArchiver()
    base_url = archiver.daily_index_url(quarter, year)
    output_path = os.path.join('masters', f'{year}', f'QTR{quarter}')

    master_files = get_masterfile_list(base_url, session)
    if master_files:
        os.makedirs(output_path, exist_ok=True)

    for file, url in master_files.items():
        filename = os.path.join(output_path, file)
        if os.path.exists(filename):
            continue

        resp = session.get(url)
        with open(filename, 'w') as f:
            f.write(resp.text)


def main():
    session = RequestSession('test@gmail.com')
    quarters = [1, 2, 3, 4]
    years = list(range(1993, 2023))
    for year, quarter in itertools.product(years, quarters):
        download_masters(year, quarter, session)


if __name__ == "__main__":
    main()


