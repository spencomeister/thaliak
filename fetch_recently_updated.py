import requests
import json
import pymysql.cursors
import datetime
import time
import asyncio
import aiohttp

async def main():
    results = []
    tasks = []
    semaphore = asyncio.Semaphore(100)
    
    for i in fetch_recently_updated_items():
        for w in load_worlds():
            task = asyncio.create_task(fetch_market_values(i, w["WorldName"], semaphore))
            tasks.append(task)
    
    responces = await asyncio.gather(*tasks)
    
    for result in responces:
        await insert_market_values_mysql(result)

# ret: json in list
def fetch_recently_updated_items():
    url = "https://universalis.app/api/v2/extra/stats/least-recently-updated?DcName=Japan&entiries=100"
    headers = {"content-type" : "application/json"}
    r = requests.get(url, headers=headers)
    j = r.json()
    data =[]
    for item in j['items']:
        data.append(item['itemID'])

    return data


# ret: json in list
async def fetch_market_values(item_id, world, semaphore):
    ret = []
    async with semaphore:
        async with aiohttp.ClientSession() as session:
            url = "https://universalis.app/api/v2/" + world + "/" + str(item_id)
            async with session.get(url) as resp:
                data = await resp.json()
                ret.append(
                    {
                        "ItemID"        :   data["itemID"]
                    ,   "WorldOrDc"     :   data["worldName"]
                    ,   "IsHighQuality" :   False
                    ,   "AveragePrice"  :   data["currentAveragePriceNQ"]
                    ,   "MinPrice"      :   data["minPriceNQ"]
                    ,   "MaxPrice"      :   data["maxPriceNQ"]
                    ,   "LastUploadTime":   data["lastUploadTime"] / 1000
                    }
                )
                ret.append(
                    {
                        "ItemID"        :   data["itemID"]
                    ,   "WorldOrDc"     :   data["worldName"]
                    ,   "IsHighQuality" :   True
                    ,   "AveragePrice"  :   data["currentAveragePriceHQ"]
                    ,   "MinPrice"      :   data["minPriceHQ"]
                    ,   "MaxPrice"      :   data["maxPriceHQ"]
                    ,   "LastUploadTime":   data["lastUploadTime"] / 1000
                    }
                )
                
    return ret

async def insert_market_values_mysql(list_json):
    cfg = load_config()
    conn = pymysql.connect(
        host        = cfg['mysql']['host'],
        user        = cfg['mysql']['user'],
        password    = cfg['mysql']['password'],
        database    = cfg['mysql']['database'],
        cursorclass = pymysql.cursors.DictCursor
    )
    
    with conn:
        with conn.cursor() as cur:
            # RegistDatetime and UpdateDatetime are not listed because they insert 
            # the default value "current datetime" specified when the table is generated.
            sql =   "INSERT IGNORE INTO universalis_market(" \
                    "ID, WorldOrDc, IsHighQuality, AveragePrice, MinPrice, MaxPrice, LastUploadTime" \
                    ") values (%s, %s, %s, %s, %s, %s, %s);"
            
            for c in list_json:
                cur.execute(sql, (
                    c['ItemID'], c['WorldOrDc'], c['IsHighQuality'], c['AveragePrice'], c['MinPrice'], c['MaxPrice'],
                    datetime.datetime.fromtimestamp(c['LastUploadTime'], datetime.timezone(datetime.timedelta(hours=9)))
                ))
            
            conn.commit()
            

def load_config():
    return json.load(open('config.json', 'r'))

def load_worlds():
    return json.load(open('worlds.json', 'r'))

if __name__=="__main__":
    # for debugging: processing time
    time_start = time.perf_counter()
    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

    # for debugging: processing time(2)
    time_end = time.perf_counter()
    time_perf = time_end - time_start
    print(time_perf)
