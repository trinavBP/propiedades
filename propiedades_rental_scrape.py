import requests
import csv
import re
import logging
import time
import numpy as np
import sys

# Increase CSV field size limit
maxInt = sys.maxsize
while True:
    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt/2)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Scraper API details
SCRAPER_API_KEY = 'eb87c7bc8c4d3874559f17e60c03a423'  # Add your API key here
BASE_URL = 'https://api.scraperapi.com'
MXN_TO_USD_CONVERSION_RATE = 20.34

RENT_URL_TEMPLATE = 'https://propiedades.com/df/renta?pagina={page_num}'
MAX_RETRIES = 3
RETRY_BACKOFF = 5
scraped_urls = set()

def fetch_page(page_num):
    retries = 0
    while retries < MAX_RETRIES:
        try:
            scraperapi_url = f'{BASE_URL}?api_key={SCRAPER_API_KEY}&url={RENT_URL_TEMPLATE.format(page_num=page_num)}'
            response = requests.get(scraperapi_url)
            if response.status_code == 200:
                return response.text
            else:
                logger.warning(f"Failed to fetch page {page_num}, status code: {response.status_code}")
        except requests.RequestException as e:
            logger.error(f"Error fetching page {page_num}: {e}")
        
        retries += 1
        time.sleep(RETRY_BACKOFF)
    return None

def get_total_pages(page_content):
    """Extract total number of pages from the first page"""
    try:
        page_info = re.search(r'(\d+)\s*resultados', page_content)
        if page_info:
            total_results = int(page_info.group(1))
            return (total_results + 23) // 24
        return 50
    except Exception as e:
        logger.error(f"Error getting total pages: {e}")
        return 50

def extract_property_data(page_content):
    properties = []
    url_pattern = re.compile(r'class="pcom-property-card-body-main-info-street" href="(https://propiedades\.com/inmuebles/.*?pos=\d+)"')
    image_pattern = re.compile(r'https%3A%2F%2Fpropiedadescom\.s3\.amazonaws\.com%2Ffiles%2F292x200%2F(.*?)\.jpg')
    rental_price_pattern = re.compile(r'"rental_price_real":(\d+)')
    size_pattern = re.compile(r'"size_m2":"(\d+)"')
    postal_code_pattern = re.compile(r'"postalCode" content="(\d+)"')
    street_address_pattern = re.compile(r'"streetAddress" content="(.*?)"')
    locality_pattern = re.compile(r'"addressLocality" content="(.*?)"')
    region_pattern = re.compile(r'"addressRegion" content="(.*?)"')
    amenity_pattern = re.compile(r'<div class="amenities-number">(\d+)<!--\s*-->\s*</div>')
    
    urls = url_pattern.findall(page_content)
    latitudes = re.findall(r'"latitude":"([0-9.-]+)"', page_content)
    longitudes = re.findall(r'"longitude":"([0-9.-]+)"', page_content)
    prices = [int(x) for x in rental_price_pattern.findall(page_content)]
    sizes = [int(x) for x in size_pattern.findall(page_content)]
    postal_codes = postal_code_pattern.findall(page_content)
    street_addresses = street_address_pattern.findall(page_content)
    localities = locality_pattern.findall(page_content)
    regions = region_pattern.findall(page_content)

    all_amenities = amenity_pattern.findall(page_content)
    amenities_grouped = []
    for i in range(0, len(all_amenities), 3):
        if i + 1 < len(all_amenities):
            amenities_grouped.append({
                'bedrooms': int(all_amenities[i]),
                'bathrooms': int(all_amenities[i + 1])
            })
        else:
            amenities_grouped.append({
                'bedrooms': None,
                'bathrooms': None
            })

    images = image_pattern.findall(page_content)
    images_per_property = [images[i:i+5] for i in range(0, len(images), 5)]

    total_properties = len(urls)
    
    for i in range(total_properties):
        image_urls = [f"https://propiedadescom.s3.amazonaws.com/files/292x200/{img}.jpg" for img in images_per_property[i]] if i < len(images_per_property) else [None]*5
        image_urls += [None] * (5 - len(image_urls))
        
        amenities = amenities_grouped[i] if i < len(amenities_grouped) else {'bedrooms': None, 'bathrooms': None}

        property_data = {
            'url': urls[i],
            'latitude': latitudes[i] if i < len(latitudes) else None,
            'longitude': longitudes[i] if i < len(longitudes) else None,
            'buy_price_mxn': 0,
            'buy_price_usd': 0,
            'rent_price_mxn': prices[i] if i < len(prices) else 0,
            'rent_price_usd': prices[i] / MXN_TO_USD_CONVERSION_RATE if i < len(prices) else 0,
            'size': sizes[i] if i < len(sizes) else None,
            'postal_code': postal_codes[i] if i < len(postal_codes) else None,
            'street_address': street_addresses[i] if i < len(street_addresses) else None,
            'locality': localities[i] if i < len(localities) else None,
            'region': regions[i] if i < len(regions) else None,
            'bedrooms': amenities['bedrooms'],
            'bathrooms': amenities['bathrooms'],
            'image_1': image_urls[0],
            'image_2': image_urls[1],
            'image_3': image_urls[2],
            'image_4': image_urls[3],
            'image_5': image_urls[4],
        }
        properties.append(property_data)

    prices_mxn = np.array([prop['rent_price_mxn'] for prop in properties if prop['rent_price_mxn']])
    sizes_m2 = np.array([prop['size'] for prop in properties if prop['size']])
    
    if len(prices_mxn) > 0 and len(sizes_m2) > 0:
        mean_price, std_price = prices_mxn.mean(), prices_mxn.std()
        mean_size, std_size = sizes_m2.mean(), sizes_m2.std()

        for prop in properties:
            prop['standardized_price'] = (prop['rent_price_mxn'] - mean_price) / std_price if prop['rent_price_mxn'] else None
            prop['standardized_size'] = (prop['size'] - mean_size) / std_size if prop['size'] else None
    else:
        for prop in properties:
            prop['standardized_price'] = None
            prop['standardized_size'] = None

    return properties

def save_properties_to_csv(properties, csv_filename):
    fieldnames = ['url', 'latitude', 'longitude', 'buy_price_mxn', 'buy_price_usd', 'rent_price_mxn', 'rent_price_usd',
                  'size', 'postal_code', 'street_address', 'locality', 'region', 'bedrooms', 'bathrooms', 
                  'image_1', 'image_2', 'image_3', 'image_4', 'image_5', 'standardized_price', 'standardized_size']
    with open(csv_filename, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        if file.tell() == 0:
            writer.writeheader()
        writer.writerows(properties)

def scrape_properties(max_pages=None, test_mode=False):
    if test_mode:
        max_pages = 2
        logger.info("Running in test mode - will only scrape 2 pages")
    
    csv_filename = 'propiedades_data_rental.csv'  # Changed filename for rental properties
    scraped_urls_this_run = set()
    
    # If the CSV file exists, remove it to start fresh
    try:
        if test_mode:  # Only remove file in test mode
            open(csv_filename, 'w').close()
            logger.info("Starting fresh CSV file for test mode")
    except Exception as e:
        logger.error(f"Error clearing CSV file: {e}")
    
    # Get total pages first
    page_content = fetch_page(1)
    if not page_content:
        logger.error("Could not fetch first page. Exiting.")
        return
        
    total_pages = get_total_pages(page_content)
    if max_pages:
        total_pages = min(total_pages, max_pages)
    
    logger.info(f"Starting scrape for {total_pages} pages")
    
    # Now scrape all pages including page 1
    for page_num in range(1, total_pages + 1):
        try:
            if page_num == 1:
                # We already have page 1 content
                pass
            else:
                page_content = fetch_page(page_num)
                
            if page_content:
                properties = extract_property_data(page_content)
                new_properties = [p for p in properties if p['url'] not in scraped_urls_this_run]
                if new_properties:
                    save_properties_to_csv(new_properties, csv_filename)
                    scraped_urls_this_run.update(p['url'] for p in new_properties)
                    logger.info(f"Scraped {len(new_properties)} properties from page {page_num}")
                else:
                    logger.info(f"No new properties found on page {page_num}")
                
                if page_num < total_pages:  # Don't sleep after last page
                    time.sleep(2)
            else:
                logger.error(f"Failed to fetch content for page {page_num}. Skipping this page.")
                time.sleep(5)
                
        except Exception as e:
            logger.error(f"Error processing page {page_num}: {e}")
            time.sleep(5)
            continue
    
    logger.info(f"Scraping completed. Total properties scraped: {len(scraped_urls_this_run)}")

if __name__ == '__main__':
    # For testing, run with test_mode=True
    # For full scrape, run with test_mode=False and optionally set max_pages
    scrape_properties(test_mode=False)  # Change to False for full scrape
    
    # Examples:
    # scrape_properties(test_mode=True)  # Test mode - 2 pages
    # scrape_properties(test_mode=False)  # Full scrape
    # scrape_properties(max_pages=10, test_mode=False)  # Scrape first 10 pages