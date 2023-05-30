import scrapy
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from scrapy.http import HtmlResponse
from selenium.webdriver.chrome.options import Options
import time
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import datetime
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import os
from fake_useragent import UserAgent

ua = UserAgent()
fake_user_agent = ua.random
home = os.path.expanduser("~")
## Setup chrome options
chrome_options = Options()
chrome_options.add_argument("--headless") # Ensure GUI is off
chrome_options.add_argument("--no-sandbox")

def dataFormatter(data,date_field):    
                if data.css("td:nth-child(20)::text").get() is not None:    
                    return{
                        "company_name": data.css("td:nth-child(2) a::text").get(),
                        "date": date_field.strftime('%Y/%m/%d'),
                        "confidence": data.css("td:nth-child(3)::text").get(),
                        "open_price": data.css("td:nth-child(4)::text").get(),
                        "highest_price": data.css("td:nth-child(5)::text").get(),
                        "lowest_price": data.css("td:nth-child(6)::text").get(),
                        "closing_price": data.css("td:nth-child(7)::text").get(),
                        "VWAP": data.css("td:nth-child(8)::text").get(),
                        "total_traded_quantity": data.css("td:nth-child(9)::text").get(),
                        "Previous_closing": data.css("td:nth-child(10)::text").get(),
                        "total_traded_value": data.css("td:nth-child(11)::text").get(),
                        "total_trades": data.css("td:nth-child(12)::text").get(),
                        "difference": data.css("td:nth-child(13)::text").get(),
                        "range": data.css("td:nth-child(14)::text").get(),
                        "difference_percentage": data.css("td:nth-child(15)::text").get(),
                        "range_percentage": data.css("td:nth-child(16)::text").get(),
                        "VWAP_percentage": data.css("td:nth-child(17)::text").get(),
                        # "120_days": data.css("td:nth-child(18)::text").get(),
                        # "180_days": data.css("td:nth-child(19)::text").get(),
                        "year_high": data.css("td:nth-child(20)::text").get(),
                        "year_low": data.css("td:nth-child(21)::text").get()
                    }
                    
                        
                
                else:
                    return{
                            "company_name": data.css("td:nth-child(2) a::text").get(),
                            "date": date_field.strftime('%Y/%m/%d'),
                            "confidence": data.css("td:nth-child(3)::text").get(),
                            "open_price": data.css("td:nth-child(4)::text").get(),
                            "highest_price": data.css("td:nth-child(5)::text").get(),
                            "lowest_price": data.css("td:nth-child(6)::text").get(),
                            "closing_price": data.css("td:nth-child(7)::text").get(),
                            "VWAP": data.css("td:nth-child(8)::text").get(),
                            "total_traded_quantity": data.css("td:nth-child(9)::text").get(),
                            "Previous_closing": data.css("td:nth-child(10)::text").get(),
                            "total_traded_value": data.css("td:nth-child(11)::text").get(),
                            "total_trades": data.css("td:nth-child(12)::text").get(),
                            "difference": data.css("td:nth-child(13)::text").get(),
                            "range": data.css("td:nth-child(14)::text").get(),
                            "difference_percentage": data.css("td:nth-child(15)::text").get(),
                            "range_percentage": data.css("td:nth-child(16)::text").get(),
                            "VWAP_percentage": data.css("td:nth-child(17)::text").get(),
                            # "120_days": data.css("td:nth-child(18)::text").get(),
                            # "180_days": data.css("td:nth-child(19)::text").get(),
                            "year_high": data.css("td:nth-child(18)::text").get(),
                            "year_low": data.css("td:nth-child(19)::text").get(),
                        
                    }                

class NepseSpider(scrapy.Spider):
    name = "nepse"
    allowed_domains = ["www.sharesansar.com"]
    # start_urls = ["https://www.sharesansar.com/today-share-price"]
    custom_settings = {
        "FEEDS": {
            "nepse_data.csv":{
                "format":"csv",
            }
        }
    }
    
    def start_requests(self):        
        url = "https://www.sharesansar.com/today-share-price"
        yield scrapy.Request(url=url, callback=self.parse) 

    def parse(self, response):
        driver = webdriver.Edge(executable_path=r"stockmarket\msedgedriver.exe")
        driver.get("https://www.sharesansar.com/today-share-price")
        time.sleep(5)
        #get todays date only
        date_field = datetime.date.today()
        # date_field = "2023-01-04"
        # date_field = datetime.datetime.strptime(date_field, '%Y-%m-%d').date()
        for i in range(0,365):
            yield from self.scraper(driver,date_field)      
            date_field = date_field - datetime.timedelta(days=1)
        driver.close()
        driver.quit()
        
    def scraper(self,driver,date_field):
        search = driver.find_element(By.CSS_SELECTOR,"div.form-group>input#fromdate")
        ##clear the date field
        search.clear()
        ##fill the date field
        search.send_keys(date_field.strftime('%Y/%m/%d'))
        search.send_keys(Keys.ENTER)
        wait = WebDriverWait(driver, 10)
        element = wait.until(EC.invisibility_of_element_located((By.CLASS_NAME, "dow")))      
        ##click on search button
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "btn_todayshareprice_submit")))
        search_button = driver.find_element(By.ID,"btn_todayshareprice_submit")
        search_button.click()
        
        time.sleep(5)
        response = HtmlResponse(url=driver.current_url, body=driver.page_source, encoding='utf-8')
        #wait for the reposne to load
        wait = WebDriverWait(driver, 15)
        try:
            for data in response.css("tbody tr"):
                formatted_data=dataFormatter(data,date_field)     
                yield formatted_data    
        except:
            print("nothing found")
            pass
        
            
class nepseTodaySpider(scrapy.Spider):
    name = "nepse-today"
    allowed_domains = ["www.sharesansar.com"]
    start_urls = ["https://www.sharesansar.com/today-share-price"]
    custom_settings = {
        'FEED_URI': f"{home}/airflow/data/nepsetoday.csv",
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8',
        'FEED_EXPORT_OVERWRITE': True,
        'FEED_EXPORT_FIELDS': ['company_name','date','confidence','open_price','lowest_price','highest_price','closing_price','VWAP','total_traded_quantity','Previous_closing','total_traded_value','total_trades','difference','range','difference_percentage','range_percentage','VWAP_percentage','year_high','year_low'],
               
    }
    
    def start_requests(self):        
        url = "https://www.sharesansar.com/today-share-price"
        yield scrapy.Request(url=url, callback=self.parse) 

    def parse(self, response):
        homedir = os.path.expanduser("~")
        webdriver_service = Service(f"{homedir}/chromedriver/stable/chromedriver")

        # Choose Chrome Browser
        driver = webdriver.Chrome(service=webdriver_service, options=chrome_options)
        driver.get("https://www.sharesansar.com/today-share-price")
        time.sleep(5)
        #get todays date only
        date_field = datetime.date.today()
        # date_field = datetime.datetime.strptime(date_field, '%Y-%m-%d').date()        
        yield from self.scraper(driver,date_field)      
        driver.close()
        driver.quit()
        
    def scraper(self,driver,date_field):    
        search = driver.find_element(By.CSS_SELECTOR,"div.form-group>input#fromdate")
        ##clear the date field
        search.clear()
        ##fill the date field
        search.send_keys(date_field.strftime('%Y/%m/%d'))
        search.send_keys(Keys.ENTER)
        wait = WebDriverWait(driver, 10)
        element = wait.until(EC.invisibility_of_element_located((By.CLASS_NAME, "dow")))      
        ##click on search button
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "btn_todayshareprice_submit")))
        search_button = driver.find_element(By.ID,"btn_todayshareprice_submit")
        search_button.click()
        time.sleep(5)    
        response = HtmlResponse(url=driver.current_url, body=driver.page_source, encoding='utf-8')
        #wait for the reposne to load
        wait = WebDriverWait(driver, 15)
        try:
            for data in response.css("tbody tr"):
                formatted_data=dataFormatter(data,date_field)     
                yield formatted_data           
        except:
            print("nothing found")
            pass
class NepseTodayRealtime(scrapy.Spider):
    name = "nepse-today-realtime"
    allowed_domains = ["https://www.nepalstock.com"]
    start_urls = ["https://www.nepalstock.com/today-price"]
    custom_settings = {
        "USER_AGENT": fake_user_agent,
        'FEED_URI': f"{home}/airflow/data/nepsetodayRealtime.csv",
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8',
        'FEED_EXPORT_OVERWRITE': True,
        'FEED_EXPORT_FIELDS': ["company_name","date",'time', "open_price","highest_price",
                            "lowest_price",
                            "total_traded_quantity",
                            "total_traded_value",
                            "total_trades",
                            "LTP",
                            "previous_closing",
                            "average_traded_price",
                            "year_high",
                            "year_low",]
    }
    def start_requests(self):        
        url = "https://www.nepalstock.com/today-price"
        yield scrapy.Request(url=url, callback=self.parse,
     headers={'User-Agent': self.settings['USER_AGENT']}
                             ) 

    def parse(self, response):
        chrome_options1 = Options()
        chrome_options.add_argument("--no-sandbox")
        driver = webdriver.Chrome(options=chrome_options1)
        driver.get("https://www.nepalstock.com/today-price")
        time.sleep(5)
        #get todays date only
        date_field = datetime.date.today()
        # convert date to / format
        date_field = date_field.strftime('%m/%d/%Y')
        # date_field = "2023-01-04"
        # date_field = datetime.datetime.strptime(date_field, '%Y-%m-%d').date()        
        yield from self.scraper(driver,date_field)      
        driver.close()
        driver.quit()
        
    def scraper(self,driver,date_field):        
        response = HtmlResponse(url=driver.current_url, body=driver.page_source, encoding='utf-8')
        #wait for the reposne to load
        wait = WebDriverWait(driver, 15)
        #wait for the search box to load        
        wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR,"body > app-root > div > main > div > app-today-price > div > div.box__filter.d-flex.flex-column.flex-md-row.justify-content-between.align-items-md-center > div.box__filter--wrap.mb-md-0.mb-3.d-flex.flex-column.flex-lg-row.align-items-md-center.justify-content-begin > div:nth-child(1) > div > input")) 
        )
        search = driver.find_element(By.CSS_SELECTOR,"body > app-root > div > main > div > app-today-price > div > div.box__filter.d-flex.flex-column.flex-md-row.justify-content-between.align-items-md-center > div.box__filter--wrap.mb-md-0.mb-3.d-flex.flex-column.flex-lg-row.align-items-md-center.justify-content-begin > div:nth-child(1) > div > input")
        search.clear()
        #wait for the search box to load
        wait.until(EC.visibility_of(search))
        search.send_keys(date_field)
        
        optionSelector = driver.find_element(By.CSS_SELECTOR,"body > app-root > div > main > div > app-today-price > div > div.box__filter.d-flex.flex-column.flex-md-row.justify-content-between.align-items-md-center > div.box__filter--wrap.mb-md-0.mb-3.d-flex.flex-column.flex-lg-row.align-items-md-center.justify-content-begin > div:nth-child(3) > select > option:nth-child(6)")
        ##wait for the option to load
        wait.until(EC.visibility_of(optionSelector))
        optionSelector.click()
        #wait for the page to load
        time.sleep(5)
        ##click on filter button 
        filterButton =driver.find_element(By.CSS_SELECTOR,"body > app-root > div > main > div > app-today-price > div > div.box__filter.d-flex.flex-column.flex-md-row.justify-content-between.align-items-md-center > div.box__filter--wrap.mb-md-0.mb-3.d-flex.flex-column.flex-lg-row.align-items-md-center.justify-content-begin > div.box__filter--btns.mt-md-3.mt-xl-0 > button.box__filter--search")
        
        wait.until(EC.visibility_of(filterButton))
        filterButton.click()
        wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, 'tbody tr')))
        response = HtmlResponse(url=driver.current_url, body=driver.page_source, encoding='utf-8')
        ##current time 
        current_time = datetime.datetime.now()
        #only hr and min
        current_time = current_time.strftime("%H:%M")
        try:
            for data in response.css("tbody tr"):
                    yield {
                        
                            "company_name": data.css("td:nth-child(2) a::text").get(),
                            "date": date_field,
                            "time":current_time,
                            "open_price": data.css("td:nth-child(4)::text").get(),
                            "highest_price": data.css("td:nth-child(5)::text").get(),
                            "lowest_price": data.css("td:nth-child(6)::text").get(),
                            "total_traded_quantity": data.css("td:nth-child(7)::text").get(),
                            "total_traded_value": data.css("td:nth-child(8)::text").get(),
                            "total_trades": data.css("td:nth-child(9)::text").get(),
                            "LTP": data.css("td:nth-child(10)>span::text").get(),
                            "previous_closing": data.css("td:nth-child(11)::text").get(),
                            "average_traded_price": data.css("td:nth-child(12)::text").get(),
                            "year_high": data.css("td:nth-child(13)::text").get(),
                            "year_low": data.css("td:nth-child(14)::text").get(),
                        
                    }                    
        except:
            print("nothing found")
            pass
    
    
