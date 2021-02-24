from scraper_api import ScraperAPIClient
from bs4 import BeautifulSoup
import concurrent.futures
from PIL import Image
import urllib3
import requests
import time
import json
import math
import csv
import re


# Items
def item_scraper(item):
# for item in items.keys():
    web = requests.get('https://liverpool.com.mx/tienda/pdp/' + item, headers=headers, proxies=proxies, verify=False)
    soup = BeautifulSoup(web.text, 'lxml')
    elements = soup.find('script', id='__NEXT_DATA__')
    elements = str(elements).strip('<script id="__NEXT_DATA__" type="application/json">')
    json_data = json.loads(str(elements))
    images = json_data['query']['data']['mainContent']['akamaiSkuImagesInfo']['skuImageMap']
    image_urls = re.findall(r'https://\w+.liverpool.com.mx/xl/\w+.jpg', str(images.values()))
    sku_name = json_data['query']['data']['mainContent']['endecaProductInfo']['contents'][0]['mainContent'][0]['record']['productDisplayName'][0]
    sku = json_data['query']['data']['mainContent']['endecaProductInfo']['contents'][0]['mainContent'][0]['record']['productId'][0]
    item_category = items[item]

    for i_url in image_urls:
        print(i_url)
        im = Image.open(requests.get(i_url, stream=True, headers=headers, proxies=proxies, verify=False).raw)
        width, height = im.size
        if width == 940 and height == 1215:
            validation = True
        else:
            validation = False

        if i_url == 'https://assetspwa.liverpool.com.mx/static/images/filler_REC.gif':
            broken = True
        else:
            broken = False

        csv_writer.writerow([item_category, sku_name, item, sku, i_url, validation, broken])
        print(sku_name + ' done.')


client = ScraperAPIClient('INSERT API KEY HERE')
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
proxies = {
  "http": "http://scraperapi:",
  "https": "http://scraperapi:"
}
start = time.perf_counter()
categories = {}
items = {}
rev_items = {}
difficult_cats = [
                '/tienda/american-eagle/catst9911665',
                '/tienda/banana-republic/catst4580701',
                '/tienda/calvin-klein/catst4580702',
                '/tienda/caterpillar/catst20529298',
                '/tienda/gap/catst4580704',
                '/tienda/hugo/catst9322811',
                '/tienda/jbe/catst18428520',
                '/tienda/levis/catst4580710',
                '/tienda/polo-ralph-lauren/catst4580712',
                '/tienda/that-s-it/catst18662068',
                '/tienda/gap/cat9540000',
                '/tienda/nike/catst7581293',
                '/tienda/adidas/catst7581398',
                '/tienda/puma/catst7581397',
                '/tienda/reebok/catst17013465',
                '/tienda/chopper/catst14469692',
]

with open('concurrent_image_test_2.csv', 'a') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(['category', 'item_name', 'item_url', 'sku', 'image_url', 'validation', 'broken'])

    # Category tree extraction
    tree_url = requests.get('https://www.liverpool.com.mx/getDepartments')
    tree = tree_url.json()
    for x in range(19):
        for i in range(len(tree['l1Categories'][x]['l2subcategories'])):
            node_length = tree['l1Categories'][x]['l2subcategories'][i]['childCategoryCount']
            for j in range(node_length):
                cat_url = tree['l1Categories'][x]['l2subcategories'][i]['l3subcategories'][j]['url']
                cat_name = tree['l1Categories'][x]['l2subcategories'][i]['l3subcategories'][j]['name']
                categories[cat_url] = cat_name

    print('Category tree extracted!')


# Cat sku url extraction
# def url_extraction(cat):
    error_counter = 0

    for cat in categories.keys():
        if cat not in difficult_cats:
            web = requests.get('https://www.liverpool.com.mx' + cat)
            soup = BeautifulSoup(web.text, 'lxml')
            element = soup.find('script', id='__NEXT_DATA__')
            element = str(element).strip('<script id="__NEXT_DATA__" type="application/json">')
            json_data = json.loads(str(element))
            # cat_tree = jsonData['query']['data']['mainContent']['contentItem']['endeca:assemblerRequestInformation']['endeca:dimensionValues']

            try:
                num_records = json_data['query']['data']['mainContent']['contentItem']['endeca:assemblerRequestInformation'][
                    'endeca:numRecords']
                records_per_page = \
                    json_data['query']['data']['mainContent']['contentItem']['endeca:assemblerRequestInformation'][
                        'endeca:recordsPerPage']
                clean_url = soup.find('link', attrs={'rel': "next"})['href']
                clean_url = re.findall(r'(https://www.liverpool.com.mx/tienda/.+/[\w-]+/)', clean_url)[0]
                total_pages = int(math.ceil(float(num_records)/float(records_per_page)))
                print(cat, total_pages)

                for page in range(1, total_pages + 1):
                    cat_webs = requests.get(clean_url + 'page-' + str(page))
                    soup = BeautifulSoup(cat_webs.text, 'lxml')
                    element = soup.find('script', id='__NEXT_DATA__')
                    element = str(element).strip('<script id="__NEXT_DATA__" type="application/json">')
                    json_data = json.loads(str(element))

                    try:
                        item_range = len(
                            json_data['query']['data']['mainContent']['contentItem']['contents'][0]['mainContent'][3][
                                'contents'][0]['records'])
                        for w in range(item_range):
                            product_name = \
                            json_data['query']['data']['mainContent']['contentItem']['contents'][0]['mainContent'][3]['contents'][
                                0]['records'][w]['productDisplayName'][0]
                            product_sku = \
                            json_data['query']['data']['mainContent']['contentItem']['contents'][0]['mainContent'][3]['contents'][
                                0]['records'][w]['productId'][0]

                            url_x = product_name.lower().replace(' ', '-') + '/' + product_sku
                            items[url_x] = cat
                            csv_writer.writerow([product_name, product_sku, url_x])

                        print('Category ' + str(cat) + ' page #' + str(page) + ' done.')


                    except IndexError:
                        try:
                            item_range = len(json_data['query']['data']['mainContent']['contentItem']['contents'][0]['mainContent'][1]['contents'][0]['records'])
                            for w in range(item_range):
                                product_name = json_data['query']['data']['mainContent']['contentItem']['contents'][0]['mainContent'][1]['contents'][0]['records'][w]['productDisplayName'][0]
                                product_sku = json_data['query']['data']['mainContent']['contentItem']['contents'][0]['mainContent'][1]['contents'][0]['records'][w]['productId'][0]

                                url_x = product_name.lower().replace(' ', '-') + '/' + product_sku
                                items[url_x] = cat
                                csv_writer.writerow([product_name, product_sku, url_x])
                            print('INDEXED Category ' + str(cat) + ' page #' + str(page) + ' done.')


                        except IndexError:
                            continue

                        except KeyError:
                            try:
                                item_range = len(json_data['query']['data']['mainContent']['contentItem']['contents'][0]['mainContent'][2]['contents'][0]['records'])
                                for w in range(item_range):
                                    product_name = json_data['query']['data']['mainContent']['contentItem']['contents'][0]['mainContent'][2]['contents'][0]['records'][w]['productDisplayName'][0]
                                    product_sku = json_data['query']['data']['mainContent']['contentItem']['contents'][0]['mainContent'][2][
                                        'contents'][0]['records'][w]['productId'][0]

                                    url_x = product_name.lower().replace(' ', '-') + '/' + product_sku
                                    items[url_x] = cat
                                    csv_writer.writerow([product_name, product_sku, url_x])
                                print('KEYED Category ' + str(cat) + ' page #' + str(page) + ' done.')

                            except IndexError:
                                continue


            except TypeError:
                error_counter += 1
                print('TYPE ERROR')
                continue
        else:
            print('Found a difficult cat!😾')

    print("WELL DONE!", error_counter)

    with concurrent.futures.ThreadPoolExecutor(max_workers=22) as executor:
        executor.map(item_scraper, items.keys())


finish = time.perf_counter()
print(f'Finished in {round(finish-start, 2)} second(s)')
