import ccxt
import json
import time
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import schedule
import configparser
import traceback
# ==============================================================================
# Config 

config = configparser.ConfigParser()

#rename your config file
config.read('################.ini')

apikey = config['FTX']['api_key']
secret_key = config['FTX']['secret_key']
acc = config['FTX']['account']

# ==============================================================================
# Configuration

Capital = config.getint('CONFIG', 'Capital')
Ceiling = config.getfloat('CONFIG', 'Ceiling')
Floor = config['CONFIG'].getint('Floor')
Gap = config.getfloat('CONFIG', 'Gap')

# ==============================================================================
# Global Variable Setting

pair = 'XRPBULL/USDT'
Coin = 'XRPBULL'
RealMoney = 'USDT'
digits = 2
Level = int((Ceiling-Floor)/Gap)

# ==============================================================================
# Connect line Notify
def linenoti():

    url = 'https://notify-api.line.me/api/notify'
    #Add Token Here
    token = '#############################################'
    headers = {'content-type': 'application/x-www-form-urlencoded','Authorization': 'Bearer '+token}

    msg = f'Account : {acc}'
    msg1 = f'Symbol : {pair}'
    msg2 = f'Error : {traceback.format_exc()}'
    sendline = requests.post(url, headers=headers, data = {'message' : '\n' + msg + '\n' + msg1 + '\n' + msg2})
# ==============================================================================
# Connect Server

ftx = ccxt.ftx({
    'apiKey': apikey,
    'secret': secret_key,
    'enableRateLimit': True
})

if acc == "0":
    acc = "Main Account"
else:
    ftx.headers = {
        'FTX-SUBACCOUNT': acc,
    }
# ==============================================================================
# Connect GGS
scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]

#rename your API GGS
creds = ServiceAccountCredentials.from_json_keyfile_name("##################", scope)

client = gspread.authorize(creds)

#renane sheet
sheet = client.open("#########################")

#rename worksheet
worksheet = sheet.worksheet("######################")

# ==============================================================================
# Update Record
def getUpdateRecord(since = None):
    params = {
    'order': 'asc',
    }
    df = pd.DataFrame(ftx.fetch_my_trades(pair,since=since,limit=200,params=params),columns=('id','datetime','symbol','side','takerOrMaker','amount','price','cost','fee'))
    return df

def convertDataFrameListForSheet(dataFrameList):
    data = []
    for i in dataFrameList:
        con = i[8]
        i[8] = float(con["cost"])
        i.append(con["currency"])
        data.append(i)
    return data

def updateSheet():
    ws = worksheet.get_all_values()
    since = None
    if len(ws) > 1:
        since = ftx.parse8601(ws[-1][1])
        secdata = getUpdateRecord(since).values.tolist()
        del secdata[0]
    else:
        secdata = getUpdateRecord(since).values.tolist()
    if secdata:
        row = len(ws)+1
        if len(ws) < 1:
            row = 2
        worksheet.append_rows(convertDataFrameListForSheet(secdata),table_range=f'A${row}')
    # check if have more
    ws = worksheet.get_all_values()
    since = ftx.parse8601(ws[-1][1])
    secdata = getUpdateRecord(since).values.tolist()
    del secdata[0]
    if secdata:
        updateSheet()

# ==============================================================================
# Check Order

def getCheckOrder():

    try:
        Order = []
        j = ftx.fetch_open_orders(pair)
        for i in j:
            Order.append(float(i['info']['price']))
        return Order
    except ccxt.NetworkError as e:
        getCheckOrder()
    except ccxt.ExchangeError as e:
        getCheckOrder()
    except Exception as e:
        getCheckOrder()


# ==============================================================================
# ใช้ในการดึงข้อมูลราคา PRODUCT ที่เราสนใจจะเทรด

def getPrice():
    try:
        r1 = json.dumps(ftx.fetch_ticker(pair))
        dataPriceBTC = json.loads(r1)
        jsonPrice = dataPriceBTC['info']['price']
        BidAsk = ftx.fetchOrderBook(pair)
        dfBidAsk = pd.DataFrame(BidAsk)
        dfBidAskList = dfBidAsk.values.tolist()
        bid = dfBidAskList[0][0]
        ask = dfBidAskList[0][1]
        return dataPriceBTC['last'], jsonPrice, bid, ask
    except ccxt.NetworkError as e:
        getPrice()
    except ccxt.ExchangeError as e:
        getPrice()
    except Exception as e:
        getPrice()

# ==============================================================================
# get Wallet
def getWallet(currency):
    try:
        wallet = ftx.fetch_balance()
        wallet_total = wallet[currency]['total']
        wallet_free = wallet[currency]['free']
        wallet_used = wallet[currency]['used']
        pdusdv = pd.DataFrame(wallet)
        dd = pdusdv.values.tolist()
        wallet_usdv = dd[0][0][0]['usdValue']
        return wallet_total, wallet_free, wallet_used, wallet_usdv
    except ccxt.NetworkError as e:
        getWallet(currency)
    except ccxt.ExchangeError as e:
        getWallet(currency)
    except Exception as e:
        getWallet(currency)

# ==============================================================================
# Buy
def getBuy(BuyAmount, Bid):
    try:
        ftx.create_order(pair, 'limit', 'buy', BuyAmount, Bid,{'postOnly' : True})
    except ccxt.ExchangeError as e:
        pass
    except ccxt.InsufficientFunds as e:
        pass

# ==============================================================================
# Sell Order
def getSell(SellAmount, Ask):
    try:
        ftx.create_order(pair, 'limit', 'sell', SellAmount, Ask,{'postOnly' : True})
    except ccxt.ExchangeError as e:
        pass
    except ccxt.InsufficientFunds as e:
        pass
# ==============================================================================
# Main Function
def CreatZone():

    Zone = []
    for i in range(Level):
        i = Ceiling-(i*Gap)
        Zone.append(round(i, digits))
    return Zone

def Grid():

    Zone = CreatZone()
    Price = getPrice()
    Order = getCheckOrder()
    lastOrder = ftx.fetch_my_trades(pair)[-1]
    BuyOrder = []
    SellOrder = []
    for i in Zone:
        if i > Price[1] and i not in Order and i != lastOrder['info']['price'] and lastOrder['info']['side'] != 'sell':
            SellOrder.append(i)
        elif i < Price[1] and i not in Order and i != lastOrder['info']['price'] and lastOrder['info']['side'] != 'buy':
            BuyOrder.append(i)
    return BuyOrder, SellOrder

def StartSell():

    sGrid = Grid()[1]
    Fund = Capital/Level
    sGrid.sort()
    if not sGrid:
        pass
    else:
        s = sGrid[0]
        for i in sGrid:
            aciont = round(Fund/(s-Gap), digits)
            getSell(aciont, s)
            return

def StartBuy():

    bGrid = Grid()[0]
    Fund = Capital/Level
    if not bGrid:
        pass
    else:
        b = bGrid[0]
        for i in bGrid:
            aciont = round(Fund/b, digits)
            getBuy(aciont, b)
            return

def Main():

    StartSell()
    StartBuy()

def RunProgram():

    print('==================================')
    print(time.strftime('%a, %d %b %Y %H:%M:%S', time.localtime()))
    print(f'Status : Working..................' )
    print(f'This Account : {acc}' )
    print(f'Now Port Value : {round((getWallet(Coin)[0]*getPrice()[1] + getWallet(RealMoney)[0]), digits)}')
    print(f'Now Order Have : {len(getCheckOrder())}')

# ==============================================================================
# Run Bot
schedule.every(1).hour.do(updateSheet)
schedule.every(30).minutes.do(RunProgram)

while (True):
    try:
        schedule.run_pending()
        Main()
        time.sleep(5)
    except Exception as err:
        linenoti()
        print(err)
        print(traceback.format_exc())
        time.sleep(300)
# ==============================================================================
