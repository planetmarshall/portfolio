import os
import re
import requests
import logging

from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from alpha_vantage.timeseries import TimeSeries


def etf_sectors():
    url = 'https://www.hl.co.uk/shares/exchange-traded-funds-etfs'
    etf_search = requests.get(url)
    soup = BeautifulSoup(etf_search.text, 'html.parser')
    options = soup.find('select', id='sectorid').find_all('option', value=re.compile("[0-9]+"))
    return pd.DataFrame.from_records([{
        'sector_id': option.attrs["value"],
        'sector': option.text
    } for option in options]).set_index("sector_id")


def parse_factsheet(html_page):
    def extract_field(soup, expr):
        title = soup.find('th', text=expr)
        if title is None:
            title = soup.find('span', text=expr)
            if title is None:
                return None
            title = title.parent

        return title.parent.find('td').text.strip().lower()

    def percent_to_ratio(text):
        expr = re.compile("([0-9.\-]+)%")
        number = expr.search(text)
        if number is None:
            return np.nan
        return float(number.group(1)) * 0.01

    soup = BeautifulSoup(html_page, 'html.parser')
    info = {
        'launch_date': pd.to_datetime(extract_field(soup, re.compile("^Launch date")), errors='coerce'),
        'charge': percent_to_ratio(extract_field(soup, re.compile("^Ongoing Charge"))),
    }
    dividend = extract_field(soup, re.compile("^Income or accumulation"))
    if dividend is not None:
        info["dividend"] = dividend

    return info


def _etfs(table):
    for row in table.find_all('tr'):
        cells = row.find_all('td')
        if len(cells) == 6:
            factsheet_url = cells[1].a.attrs["href"]
            response = requests.get(factsheet_url)
            info = parse_factsheet(response.text)

            data = {
                'symbol': cells[0].text,
                'company': cells[1].a.text,
                'is_sophisticated': len(cells[2].contents) > 0,
                'lse': len(cells[3].contents) > 0,
                'name': cells[4].text,
                'factsheet': factsheet_url
            }
            data.update(info)
            yield data


def _offsets(search_results_table):
    page_cells = search_results_table.find_all("a", title=re.compile("^View page"))
    pages = set([int(cell.text) for cell in page_cells])
    return [(page - 1) * 50 for page in pages]


def _etfs_by_sector(sector_id, offset=None):
    url = 'https://www.hl.co.uk/shares/exchange-traded-funds-etfs/list-of-etfs'
    params = {
        'etf_search_input': '',
        'companyid': '',
        'sectorid': sector_id,
        'tab': 'prices'
    }
    if offset is not None:
        params['offset'] = offset

    sector_etfs = requests.get(url, params=params)
    soup = BeautifulSoup(sector_etfs.text, 'html.parser')
    return soup.find('table', summary='ETF search results')


def etfs_by_sector(sector_id):
    first_page = _etfs_by_sector(sector_id)
    records = list(_etfs(first_page))
    for offset in _offsets(first_page):
        records += list(_etfs(_etfs_by_sector(sector_id, offset)))

    return pd.DataFrame.from_records(records).set_index('symbol')




def historical(symbol, api_key=None):
    if api_key is None:
        api_key = os.getenv("ALPHAVANTAGE_API_KEY")

    ts = TimeSeries(key=api_key)