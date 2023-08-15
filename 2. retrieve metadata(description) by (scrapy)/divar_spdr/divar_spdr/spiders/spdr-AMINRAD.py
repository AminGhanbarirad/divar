import scrapy
import sqlite3

class DivarSpider(scrapy.Spider):
    name = 'divar'
    
    # Custom settings for the spider
    custom_settings = {
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
        cursor = conn.cursor()
        #################################################################
        cursor.execute('SELECT token_C FROM tokens_T WHERE token_C IS NULL') #where ...
        #################################################################
        tokens = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        
        for token in tokens:
            url = f'https://divar.ir/v/-/{token}'
            yield scrapy.Request(url=url, callback=self.parse, meta={'token': token})

    def parse(self, response):
        token = response.meta.get('token')
        
        description = response.css('div p.kt-description-row__text--primary::text').extract_first()
        
        conn = sqlite3.connect('divar.sqlite')
        cursor = conn.cursor()
        cursor.execute('UPDATE tokens_T SET description_C=? WHERE token_C=?', (description, token))
        conn.commit()
        conn.close()

        self.log(f'description for token {token} updated successfully.')

# Run the spider
if __name__ == '__main__':
    from scrapy.crawler import CrawlerProcess
    process = CrawlerProcess()
    process.crawl(DivarSpider)
    process.start()
