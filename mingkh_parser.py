# importing moduls
# requests and bs4 are needed to find the correct page and elements on the page
import requests
from bs4 import BeautifulSoup

# multiprocessing to fasten the data collection and time/random for masking the requests to not get booted by the server
from multiprocessing import Pool
import random
import time
# pandas to clean up data and output file
import pandas as pd

base_url = "https://dom.mingkh.ru"

user_agent_list = ['Mozilla/5.0 (Windows NT 6.1; WOW64; rv:77.0) Gecko/20190101 Firefox/77.0',
                   'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:77.0) Gecko/20100101 Firefox/77.0',
                   'Mozilla/5.0 (X11; Linux ppc64le; rv:75.0) Gecko/20100101 Firefox/75.0',
                   'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:39.0) Gecko/20100101 Firefox/75.0',
                   'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.10; rv:75.0) Gecko/20100101 Firefox/75.0',
                   'Mozilla/5.0 (X11; Linux; rv:74.0) Gecko/20100101 Firefox/74.0',
                   'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:61.0) Gecko/20100101 Firefox/73.0',
                   'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'
                   ]
user_agent = random.choice(user_agent_list)
headers = {
    'cookie': "_ym_uid=16522806051014068889; _ym_d=1652280605; PHPSESSID=0iu13f56j91656meb6tro3hbr0; _ym_isad=1",
    'authority': "dom.mingkh.ru",
    'accept': "*/*",
    'accept-language': "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    'referer': "https://dom.mingkh.ru/arhangelskaya-oblast/arhangelsk/421758",
    'user-agent': user_agent
}

# No useful API so soup does the main work for collecting info
r = requests.get(base_url)
gen_soup = BeautifulSoup(r.text, 'html.parser')

# combing all the regions to get urls in a list
s_regions = gen_soup.find_all(class_='col-md-3 col-sm-6 col-xs-6 list-unstyled')
region_urls = []
for rows in s_regions:
    for row in rows.find_all('a'):
        region_urls.append(base_url + row.get('href'))

# same combing with cities
city_urls = []
reg_r = requests.get('https://dom.mingkh.ru/city/')
reg_soup = BeautifulSoup(reg_r.text, 'html.parser')
r_cities = reg_soup.find_all(class_='list-unstyled list-inline')
for rows in r_cities:
    for row in rows.find_all('a'):
        city_urls.append(base_url + row.get('href'))

# function to ping the website and if any error it gives 5 seconds to refresh and tries again
def get_response(url):

    while 1:
        try:
            response = requests.get(url, headers=headers)
            if not response.status_code == 200:
                print(response.status_code)
                print(url)
                continue
            soup = BeautifulSoup(response.text, 'html.parser')
            return soup
        except BaseException as e:
            print(e)
            time.sleep(5)

# main function to get the info from a page on a house
def house_page_parser(url):
    soup = get_response(url)

    id_lat_lng = []
    house_id = soup.select('#houseid, #mapcenterlat, #mapcenterlng')
    for _ in range(3):
        try:
            id_lat_lng.append(house_id[_]['value'])
        except IndexError:
            id_lat_lng.append('None')
    id_df = pd.DataFrame(id_lat_lng).T
    id_df.index += 1
    id_df.columns = ['id', 'lat', 'lng']

    house = soup.find_all('dl', class_='dl-horizontal house')
    index = []
    info = []
    # cleaning up some duplicate data in the soup elements
    for row in house[0].find_all('dt'):
        if 'ыписк' in row.text:
            continue
        else:
            index.append(row.text.strip())
    for row in house[0].find_all('dd'):
        if row.text.startswith('Как получить'):
            continue
        else:
            info.append(row.text.strip().replace('\xa0\xa0\xa0На карте', ''))

    # combining dataframes to create one with general information
    general_df = pd.DataFrame(dict(zip(index, info)), index=[1]).drop('', axis=1)

    # searching through tables of soup for the additional information
    output = dict()
    last_category = ''
    tables = soup.find_all('tr')
    for t in tables:
        category = t.find('td', {'class': 'col-md-12 bg-gray'})
        if category is not None:
            last_category = category.text.strip()
            continue
        k = t.find('td', {'class': 'col-md-6 col-xs-8 word-wrap-force'})
        if k is None:
            k = t.find('td', {'class': 'col-md-6 col-xs-5'})
            if k is None:
                k = t.find('td', {'col-md-6 col-xs-5 word-wrap-force'})
                if k is None:
                    continue
                else:
                    v = t.find('td', {'col-md-6 col-xs-7 word-wrap-force'})
                    output[k.text.strip().split('\n')[0]] = v.text.strip().replace('&nbsp', ' ')
            else:
                v = t.find('td', {'class': 'col-md-6 col-xs-7'})
                output[last_category + '_' + k.text.strip()] = v.text.strip().replace('&nbsp', ' ')
        else:
            v = t.find('td', {'class': 'col-md-6 col-xs-4 word-wrap-force'})
            output[k.text.strip()] = v.text.strip().replace('&nbsp', ' ')

    additional_df = pd.DataFrame(output, index=[1])
    overhaul_df = pd.DataFrame()
    try:
        maint = soup.find('table', {'class': 'table table-hover table-striped'})
        maint_header = maint.find('thead')
        maint_header = maint_header.find_all('th')
        maint_body = maint.find('tbody')
        maint_body = maint_body.find_all('td')
        h_output = []
        m_output = []
        for h in maint_header:
            h_output.append(h.text.strip())
        for m in maint_body:
            m_output.append(m.text.strip())

        overhaul = {str(h_output): str(m_output)}
        overhaul_df = pd.DataFrame(overhaul, index=[1])
    except AttributeError:
        pass

    house_df = pd.concat([id_df, general_df, additional_df, overhaul_df], axis=1)
    house_df = house_df.loc[:, ~house_df.columns.duplicated()]
    return house_df

# function to create asynchronous requests to the website to fasten the data collection
def run_pool():
    with Pool(processes=10) as p:
        r = p.map_async(house_page_parser, df)
        return r.get()

# main code
if __name__ == '__main__':
    for reg in region_urls:
        split = reg.split('/')
        name = split[3]
        print(name)
        df = pd.read_csv(f'/Users/timk/Projects/MinZhkh/URLs/Region/{name}_urls.csv').drop('Unnamed: 0', axis=1)
        df = df['house_url'].values.tolist()
        print(len(df))
        house_output = run_pool()
        print(f'getting {name} house data - done')
        houses_df = pd.concat(house_output).reset_index(drop=True)
        houses_df.index = houses_df.index + 1
        houses_df.to_csv(f'/Users/timk/Projects/MinZhkh/House_Data/Region/{name}.csv')
