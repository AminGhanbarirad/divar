import urllib.request
import requests
import sqlite3
import time
from bs4 import BeautifulSoup
import scrapy
from scrapy.utils.project import get_project_settings

class DivarSpider(scrapy.Spider):
    name = 'divar'
    
    # Custom settings for the spider
    custom_settings = {
    'LOG_LEVEL': 'DEBUG',                     # Set the log level to INFO
    'DOWNLOAD_DELAY': 1,                      # Add a delay of 2 seconds between requests
    'RETRY_ENABLED': True,                    # Enable retries
    'RETRY_TIMES': 3,                         # Number of times to retry a request in case of failure
    'RETRY_DELAY': 5,                         # Delay between retries (5 seconds)
    'DOWNLOAD_TIMEOUT': 10,                   # Set a timeout for downloading requests
    'CONCURRENT_REQUESTS': 1,                 # Limit concurrent requests to 1
    'CONCURRENT_REQUESTS_PER_DOMAIN': 1,      # Limit concurrent requests per domain to 1
    'AUTOTHROTTLE_ENABLED': True,             # Enable AutoThrottle
    'AUTOTHROTTLE_START_DELAY': 5.0,          # Initial delay for AutoThrottle (5 seconds)
    'AUTOTHROTTLE_TARGET_CONCURRENCY': 1.0,   # Target concurrency for AutoThrottle
    'AUTOTHROTTLE_MAX_DELAY': 60.0,           # Maximum delay for AutoThrottle (60 seconds)
    'AUTOTHROTTLE_DEBUG': False,              # Set to True for AutoThrottle debugging
    'DOWNLOAD_IMAGES': False,                 # Avoid downloading images
    'MEDIA_ALLOW_REDIRECTS': False            # Avoid redirects for media (images, videos)
}

    

    def start_requests(self):
        conn = sqlite3.connect('divar.sqlite')
        cur = conn.cursor()
        
        #####################################################################################################
        #####################################################################################################
        cur.execute('SELECT token_C FROM tokens_T WHERE description_C IS NULL AND spam_title_C = 0 AND fake_price_flag_C = 0 AND store_C = 0')
        #####################################################################################################
        #####################################################################################################

        tokens = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()

        for token in tokens:
            url = f'https://divar.ir/v/-/{token}'
            yield scrapy.Request(url=url, callback=self.parse, meta={'token': token})

    def parse(self, response):
        token = response.meta.get('token')
        description = response.css('div p.kt-description-row__text--primary::text').extract_first()
        
        conn = sqlite3.connect('divar.sqlite')
        cur = conn.cursor()
        cur.execute('UPDATE tokens_T SET description_C=? WHERE token_C=?', (description, token))
        conn.commit()

        #####################################################################################################
        #####################################################################################################
        migration_description_flag_C_udate_query = '''
        UPDATE tokens_T
        SET migration_description_flag_C = CASE
            WHEN description_C like "%مهاجرت%" OR description_C like "%علت سفر%" OR description_C like "%دلیل سفر%" 
            OR title_C like "%مهاجرت%" OR title_C like "%علت سفر%" OR title_C like "%دلیل سفر%"
            THEN 1
            ELSE 0
        END
        '''
        cur.execute(migration_description_flag_C_udate_query)
        conn.commit()
        #####################################################################################################
        #####################################################################################################

        conn.close()

        self.log(f'Processing token {token}: Description updated successfully.')

def insert_data(cursor, data):
    cursor.execute('INSERT INTO tokens_T(token_C, title_C, status_C, price_C, store_C) VALUES (?, ?, ?, ?, ?)',
                   (data['token'], data['title'], data['top_description_text'], data['middle_description_text'], data['store']))

def main():
    conn = sqlite3.connect('divar.sqlite')
    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS tokens_T')
    cur.execute('CREATE TABLE tokens_T(id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE , token_C TEXT , title_C TEXT , status_C TEXT , price_C TEXT , store_C TEXT , description_C TEXT)')
    
    main_url = 'https://divar.ir/s/iran/refrigerator-freezer'
    html = urllib.request.urlopen(main_url).read()
    soup = BeautifulSoup(html, 'html.parser')
    tags = soup('article')
    
    for tag in tags[:24]:
        token = tag.get('token', None)
        title = tag.h2.get_text(strip=True)
        status = tag.find('div', class_='kt-post-card__description').get_text(strip=True)
        price = tag.find('div', class_='kt-post-card__description').find_next_sibling().get_text(strip=True)
        store_look = tag.find('span', class_='kt-post-card__red-text')
        store = store_look.get_text(strip=True) if store_look else 0
        
        print('token =' , token, 'title =' ,title , 'status :' , status , 'price :' , price)

        insert_data(cur, {
            'token': token,
            'title': title,
            'top_description_text': status,
            'middle_description_text': price,
            'store': store
        })
        conn.commit()





        
    last_post_date = int(input('************Enter last_post_date value: '))
    ##https://s100.divarcdn.com/statics/2023/08/bootstrap-5672.91eb43ac.desktop.js
    #t.lastPostDate
    limit = int(input('*********************Enter limit value: '))
    total_time_start = time.time()


    scroll_api_url = 'https://api.divar.ir/v8/web-search/1/refrigerator-freezer'
    #headers = {'Content-Type': 'application/json'}
    json_schema = {
        "category": {"value": "refrigerator-freezer"},
        "cities": [
            "1",
            "2",
            "3",
            "4",
            "5",
            "6",
            "7",
            "8",
            "9",
            "10",
            "11",
            "12",
            "13",
            "14",
            "15",
            "16",
            "17",
            "18",
            "19",
            "20",
            "21",
            "22",
            "23",
            "24",
            "25",
            "26",
            "27",
            "28",
            "29",
            "30",
            "31",
            "32",
            "33",
            "34",
            "35",
            "36",
            "37",
            "38",
            "39",
            "314",
            "316",
            "317",
            "318",
            "602",
            "660",
            "662",
            "663",
            "664",
            "665",
            "671",
            "706",
            "707",
            "708",
            "709",
            "710",
            "743",
            "744",
            "745",
            "746",
            "747",
            "748",
            "749",
            "750",
            "751",
            "752",
            "753",
            "754",
            "756",
            "759",
            "760",
            "761",
            "762",
            "763",
            "764",
            "765",
            "766",
            "767",
            "768",
            "769",
            "770",
            "771",
            "772",
            "773",
            "774",
            "775",
            "776",
            "777",
            "778",
            "779",
            "780",
            "781",
            "782",
            "783",
            "784",
            "785",
            "786",
            "787",
            "788",
            "789",
            "790",
            "791",
            "792",
            "793",
            "794",
            "795",
            "796",
            "797",
            "798",
            "799",
            "800",
            "802",
            "803",
            "804",
            "805",
            "806",
            "807",
            "808",
            "809",
            "810",
            "811",
            "812",
            "813",
            "814",
            "815",
            "816",
            "817",
            "818",
            "819",
            "820",
            "821",
            "822",
            "823",
            "824",
            "825",
            "826",
            "827",
            "828",
            "829",
            "830",
            "831",
            "832",
            "833",
            "834",
            "835",
            "836",
            "837",
            "838",
            "839",
            "840",
            "841",
            "842",
            "843",
            "844",
            "845",
            "846",
            "847",
            "848",
            "849",
            "850",
            "851",
            "852",
            "853",
            "854",
            "855",
            "856",
            "857",
            "858",
            "859",
            "860",
            "861",
            "862",
            "863",
            "864",
            "865",
            "866",
            "867",
            "868",
            "869",
            "870",
            "871",
            "872",
            "873",
            "874",
            "1683",
            "1684",
            "1686",
            "1687",
            "1688",
            "1689",
            "1690",
            "1691",
            "1692",
            "1693",
            "1694",
            "1695",
            "1696",
            "1697",
            "1698",
            "1699",
            "1700",
            "1701",
            "1702",
            "1703",
            "1706",
            "1707",
            "1708",
            "1709",
            "1710",
            "1711",
            "1712",
            "1713",
            "1714",
            "1715",
            "1716",
            "1717",
            "1718",
            "1719",
            "1720",
            "1721",
            "1722",
            "1723",
            "1724",
            "1725",
            "1726",
            "1727",
            "1728",
            "1729",
            "1730",
            "1731",
            "1732",
            "1733",
            "1734",
            "1735",
            "1736",
            "1737",
            "1738",
            "1739",
            "1740",
            "1741",
            "1742",
            "1743",
            "1744",
            "1745",
            "1746",
            "1747",
            "1748",
            "1749",
            "1750",
            "1751",
            "1752",
            "1753",
            "1754",
            "1755",
            "1756",
            "1757",
            "1758",
            "1759",
            "1760",
            "1761",
            "1762",
            "1763",
            "1764",
            "1765",
            "1766",
            "1767",
            "1768",
            "1769",
            "1770",
            "1771",
            "1772",
            "1773",
            "1774",
            "1775",
            "1776",
            "1777",
            "1778",
            "1779",
            "1780",
            "1781",
            "1782",
            "1783",
            "1784",
            "1785",
            "1786",
            "1787",
            "1788",
            "1789",
            "1790",
            "1791",
            "1792",
            "1793",
            "1794",
            "1795",
            "1796",
            "1797",
            "1798",
            "1799",
            "1800",
            "1801",
            "1802",
            "1803",
            "1804",
            "1805",
            "1806",
            "1807",
            "1808",
            "1809",
            "1810",
            "1811",
            "1812",
            "1813",
            "1814",
            "1815",
            "1816",
            "1817",
            "1818",
            "1819",
            "1820",
            "1821",
            "1822",
            "1823",
            "1824",
            "1825",
            "1826",
            "1827",
            "1828",
            "1829",
            "1830",
            "1831",
            "1832",
            "1833",
            "1834",
            "1835",
            "1836",
            "1837",
            "1839",
            "1840",
            "1841",
            "1842",
            "1843",
            "1844",
            "1845",
            "1846",
            "1847",
            "1848",
            "1849",
            "1850",
            "1851",
            "1852",
            "1853",
            "1854",
            "1855",
            "1856",
            "1858",
            "1859",
            "1860",
            "1861",
            "1862",
            "1863",
            "1864",
            "1865",
            "1866",
            "1867",
            "1868",
            "1869",
            "1870",
            "1871",
            "1872",
            "1873",
            "1874",
            "1875",
            "1876"
        ],
        "goods-business-type": {"value": "all"}
    }

    page = 1
    scroll_time_total = 0

    while page < limit :
        
        payload = {
            "json_schema": json_schema,
            "last-post-date": last_post_date,
            "page": page
        }
        
        start_time = time.time()
        response = requests.post(scroll_api_url, json=payload)
        end_time = time.time()
        scroll_time = end_time - start_time
        scroll_time_total += scroll_time

        data = response.json()
        last_post_date = data['last_post_date']

        for widget in data['web_widgets']['post_list']:
            token = widget['data']['token']
            title = widget['data']['title']
            status = widget['data']['top_description_text']
            price = widget['data']['middle_description_text']
            if 'red_text' in widget['data'] and widget['data']['red_text']:
                store = widget['data']['red_text']
            else: store = 0            

            print('token =' , token, 'title =' ,title , 'status :' , status , 'price :' , price)
            
            insert_data(cur, {
                'token': token,
                'title': title,
                'top_description_text': status,
                'middle_description_text': price,
                'store': store
            })
            conn.commit()
      
        total_time_elapsed = time.time() - total_time_start
        print(f'***************Scroll {page+1}/{limit} took :{end_time - start_time:.2f} seconds')
        print(f'**************Total time elapsed :{total_time_elapsed:.2f} seconds')
        print(f'Total time taken for all scrolls :{scroll_time_total:.2f} seconds')
        print(f'**********************Mean speed :{page*24/total_time_elapsed:.2f} tokens per seconds')
        print(f'***************************speed :{24/scroll_time:.2f} tokens per seconds')

        page += 1


def cleaning_and_validation():
    
    conn = sqlite3.connect("divar.sqlite")
    cur = conn.cursor()

    title_spam_detection_column_creation = '''
        ALTER TABLE tokens_T ADD COLUMN spam_title_C INTEGER
    '''
    cur.execute(title_spam_detection_column_creation)
    print("title_spam_detection_column_creation : created")

    conn.commit()

    title_spam_detection_C_update_query = '''
        UPDATE tokens_T
        SET spam_title_C = CASE
            WHEN title_C like "%خرید%" OR title_C like "%خراب%" OR title_C like "%مسافرت%" -- Add other conditions
            THEN 1
            ELSE 0
        END
    '''
    cur.execute(title_spam_detection_C_update_query)
    print("title_spam_detection_C_update_query - Rows Affected:", cur.rowcount)
    conn.commit()
    
    status_C_update_query = '''
        UPDATE tokens_T
        SET status_C = CASE status_C
            WHEN "نو" THEN 1
            WHEN "در حد نو" THEN 2
            WHEN "کارکرده" THEN 3
            WHEN "نیازمند تعمیر" THEN 4
        END
        WHERE status_C IN ("نو", "در حد نو", "کارکرده", "نیازمند تعمیر")
    '''
    cur.execute(status_C_update_query)
    print("status_C_update_query - Rows Affected:", cur.rowcount)
    conn.commit()
    
    store_C_update_query_1 = '''
        UPDATE tokens_T SET store_C = 1 WHERE store_C IN ("فروشگاه ", "فروشگاه", "فوری در فروشگاه")
    ''' 
    cur.execute(store_C_update_query_1)
    print("store_C_update_query_1 - Rows Affected:", cur.rowcount)

    store_C_update_query_2 = '''
        UPDATE tokens_T SET store_C = 0 WHERE store_C != 1 ----------------- AND store_C != "فوری "
    '''
    ##فعلا  فوری  رو 0 میکنم
    cur.execute(store_C_update_query_2)
    print("store_C_update_query_2 - Rows Affected:", cur.rowcount)
    conn.commit()

    price_C_cleaning_query_1 = '''
        UPDATE tokens_T
        SET price_C = CAST(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(price_C,
        ',',''),'۰','0'),'۱','1'),'۲','2'),'۳','3'),'۴','4'),'۵','5'),'۶','6'),'۷','7'),'۸','8'),'۹','9')
        , 'تومان'       , '') AS INTEGER)
        WHERE price_C != "توافقی"
    '''
    cur.execute(price_C_cleaning_query_1)
    print("price_C_cleaning_query - Rows Affected:", cur.rowcount)
    conn.commit()
        
    price_C_cleaning_query_2 = '''
        UPDATE tokens_T
        SET price_C = CAST(0 AS INTEGER)
        WHERE price_C = "توافقی"
    '''
    cur.execute(price_C_cleaning_query_2)
    print("price_C_cleaning_query_2 - Rows Affected:", cur.rowcount)
    conn.commit()

    price_C_cleaning_query_3 = '''
        UPDATE tokens_T
        SET price_C = 0
        WHERE price_C IS NULL
    '''
    cur.execute(price_C_cleaning_query_3)
    print("price_C_cleaning_query_3 - Rows Affected:", cur.rowcount)
    conn.commit()


    migration_description_flag_C_column_creation = '''
        ALTER TABLE tokens_T ADD COLUMN migration_description_flag_C INTEGER
    '''
    cur.execute(migration_description_flag_C_column_creation)
    print("migration_description_flag_C_column_creation : created")
    conn.commit()
        
    migration_description_flag_C_udate_query = '''
        UPDATE tokens_T
        SET migration_description_flag_C = CASE
            WHEN description_C like "%مهاجرت%" OR description_C like "%علت سفر%" OR description_C like "%دلیل سفر%" 
            OR title_C like "%مهاجرت%" OR title_C like "%علت سفر%" OR title_C like "%دلیل سفر%"
            THEN 1
            ELSE 0
        END
    '''
    cur.execute(migration_description_flag_C_udate_query)
    print("migration_description_flag_C_udate_query - Rows Affected:", cur.rowcount)
    conn.commit()


    fake_price_flag_C_column_creation = '''
        ALTER TABLE tokens_T ADD COLUMN fake_price_flag_C INTEGER
    '''
    cur.execute(fake_price_flag_C_column_creation)
    print("fake_price_flag_C_column_creation : created")
    conn.commit()

    patterns = [str(i) * j for i in range(1, 10) for j in range(1, 11)]

    fake_price_flag_C_query = '''
        UPDATE tokens_T
        SET fake_price_flag_C = CASE
            WHEN price_C IN ({})
            OR price_C like "%1234%"   --------- IN ( '1234', '12345', '123456', '1234567', '12345678', '123456789', '1234567890')
            OR price_C = 0
            THEN 1
            ELSE 0
        END
    '''.format(','.join(['?'] * len(patterns)))

    cur.execute(fake_price_flag_C_query, patterns)
    print("fake_price_flag_C_update_query - Rows Affected:", cur.rowcount)
    conn.commit()

    conn.close()



if __name__ == "__main__":
    main()
    cleaning_and_validation()
    from scrapy.crawler import CrawlerProcess
    process = CrawlerProcess(get_project_settings())
    process.crawl(DivarSpider)
    process.start()
