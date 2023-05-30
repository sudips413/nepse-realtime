from stockmarket.spiders.nepse import nepseTodaySpider
from scrapy.crawler import CrawlerProcess
import pandas as pd
import os
def main():
    print("scraping started")
    process = CrawlerProcess()
    process.crawl(nepseTodaySpider)
    process.start()
    
def csvMerger():
    nepseToday=pd.read_csv("data/nepse_today.csv")
    nepseExceptToday=pd.read_csv("data/nepse_data.csv")
    # get the date of the last row of nepseToday
    lastDate=nepseToday.iloc[-1]["date"]
    #now delete the nepseExceptToday rows date equal to lastDate
    nepseExceptToday=nepseExceptToday[nepseExceptToday["date"]!=lastDate]
    #now merge the two dataframes in descending order of date
    df=pd.concat([nepseToday,nepseExceptToday],axis=0)
    df=df.sort_values(by="date",ascending=False)
    df.to_csv("data/nepse_data.csv",index=False)
    try:
        #now delete the nepseToday.csv file
        os.remove("data/nepse_today.csv")
    except:
        pass
    
    
    
    
if __name__ == "__main__":
    print("scraping started from main.py")
    main()
    