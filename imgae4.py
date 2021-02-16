# import concurrent.futures
from bs4 import BeautifulSoup
from PIL import Image
import requests
import json
import math
import csv
import re

categories = {}
items = {}
rev_items = {}
difficult_cats = ['/tienda/american-eagle/catst9911665',
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

with open('concurrent_image_test_1.csv', 'w') as csv_file:
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

                    print('Category ' + str(cat) + ' page #' + str(page) + ' done.')

                except IndexError:
                    try:
                        item_range = len(json_data['query']['data']['mainContent']['contentItem']['contents'][0]['mainContent'][1]['contents'][0]['records'])
                        product_name = json_data['query']['data']['mainContent']['contentItem']['contents'][0]['mainContent'][1]['contents'][0]['records'][w]['productDisplayName'][0]
                        product_sku = json_data['query']['data']['mainContent']['contentItem']['contents'][0]['mainContent'][1]['contents'][0]['records'][w]['productId'][0]

                        url_x = product_name.lower().replace(' ', '-') + '/' + product_sku
                        items[url_x] = cat
                        print('INDEXED Category ' + str(cat) + ' page #' + str(page) + ' done.')

                    except KeyError:
                        try:
                            item_range = len(json_data['query']['data']['mainContent']['contentItem']['contents'][0]['mainContent'][2]['contents'][0]['records'])
                            product_name = json_data['query']['data']['mainContent']['contentItem']['contents'][0]['mainContent'][2]['contents'][0]['records'][w]['productDisplayName'][0]
                            product_sku = json_data['query']['data']['mainContent']['contentItem']['contents'][0]['mainContent'][2][
                                'contents'][0]['records'][w]['productId'][0]

                            url_x = product_name.lower().replace(' ', '-') + '/' + product_sku
                            items[url_x] = cat
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
print(len(items.keys()))

# Items
def item_scraper(item):
#for item in items.keys():
    web = requests.get(item)
    soup = BeautifulSoup(web.text, 'lxml')
    element = soup.find('script', id='__NEXT_DATA__')
    element = str(element).strip('<script id="__NEXT_DATA__" type="application/json">')
    json_data = json.loads(str(element))
    images = json_data['query']['data']['mainContent']['akamaiSkuImagesInfo']['skuImageMap']
    image_urls = re.findall(r'https://\w+.liverpool.com.mx/xl/\w+.jpg', str(images.values()))
    sku_name = json_data['query']['data']['mainContent']['endecaProductInfo']['contents'][0]['mainContent'][0]['record']['productDisplayName'][0]
    sku = json_data['query']['data']['mainContent']['endecaProductInfo']['contents'][0]['mainContent'][0]['record']['productId'][0]
    item_category = items[item]
    items_category = categories[item_category]

    for i_url in image_urls:
        im = Image.open(requests.get(i_url, stream=True).raw)
        width, height = im.size
        if width == 940 and height == 1215:
            validation = True
        else:
            validation = False

        if i_url == 'https://assetspwa.liverpool.com.mx/static/images/filler_REC.gif':
            broken = True
        else:
            broken = False

        csv_writer.writerow([items_category, sku_name, item, sku, i_url, validation, broken])


"""
print('INITIATING CAT EXTRACTIONS...')
with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
    executor.map(url_extraction, categories.keys())

print("INITIATING IMAGE EXTRACTIONS...")

with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
    executor.map(item_scraper, items.keys())
"""