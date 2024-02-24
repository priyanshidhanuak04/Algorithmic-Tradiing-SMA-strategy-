from datetime import datetime, timedelta
import subprocess
import pandas as pd

headers=["DateTime", "Ticker", "ExpiryDT", "Strike", "F&O", "Option", "Volume", "Open", "High", "Low", "Close", "OpenInterest"]
# df =pd.read_csv("FINNIFTY-I.NFO_2020-01-03.csv", header=None).reset_index(drop=True)
# df.columns=headers
# print(df.head(10))
ExeDF=pd.DataFrame(columns=["Symbol", "Pos", "Date", "Strike", "ExpiryDT", "Option", "EnTime", "SPrice", "ExTime","BPrice"])

#Setting Parameters:
start_time = pd.to_datetime("9:15:00").time()
end_time = pd.to_datetime("15:15:00").time() 
candeltime = 5
UnavailDateList = []
instruments='NIFTY'
quantity = 50
Start_DT = pd.to_datetime('2023-09-01')
End_DT = pd.to_datetime('2023-09-30')
fileyear = End_DT.year
ExpiryFILE = "NIFTYData_202309.xls" # for the year 2023 
# ExpiryFILE = "NIFTYData_20230626.xls" #only 2022 data

period1  = 10
sma1 = f'SMA{period1}'

period2  = 30
sma2 = f'SMA{period2}'

"""______________________________________________________________________FILE NAMES____________________________________________________________________________________"""

#SMA10_SMA30_resample_and_signal_future_CE_PE_2022.csv
#SMA10_SAM30_Filetred_resample_and_signal_future_CE_PE_2022.csv
#SMA10_SMA30_Trade_option_CE_PE_2022.csv

r̥r̥
"""____________________________________________________________FOR YEAR 2022 !!!!!_________________________________________________________________________________________________________"""
    
i=0
def query(**kwargs):
    """
    :param instrument: String
    :param expry_dt: Datetime
    :param strike: numpy int
    :param option_type: CE  PE
    :param start_date: In Datetime
    :param end_date: In Datetime
    """

    global ticker, UnavailDateList

    start_date = kwargs['start_date'].strftime("%Y-%m-%d") + 'T' + "09:15:00"
    end_date = kwargs['end_date'].strftime("%Y-%m-%d") + 'T' + "15:30:00"
    if kwargs['f_o'] == 'O':
        ticker = (kwargs['instrument'] + kwargs['expiry_dt'].strftime("%d%b%y") + str(kwargs['strike']) + kwargs[
            'option_type']).upper() + '.NFO'  # nfo FOR OHLCV
    elif kwargs['f_o'] == 'F':
        ticker = kwargs['instrument'] + '-I' + '.NFO'  #+kwargs['start_date'].strftime("%Y-%m-%d")

    print(ticker, start_date, end_date)
    try:
        subprocess.call(["/home/admin/query_client/query_ohlcv", ticker, start_date, end_date])

        # df = pd.read_csv(f"~/query_client/{ticker}.csv", parse_dates=['__time'])

        df = pd.read_csv(f"{ticker}.csv", header=None, low_memory=False).reset_index(drop=True)

        # print(df.head())

        df.columns = ['DateTime', 'Ticker', 'ExpiryDT', 'Strike', 'FnO', 'Option', 'Volume',
                    'Open', 'High', 'Low', 'Close', 'OI']
        # df['Time'] = pd.to_datetime((df['DateTime'])).apply(lambda x: x.time())

        df['Time'] = pd.to_datetime((df['DateTime'])).dt.strftime("%H:%M:%S")

        df["Date"] = pd.to_datetime((df['DateTime'])).dt.strftime("%Y-%m-%d")
        subprocess.call(['unlink', ticker + '.csv'])  # This deletes the file from storage after reading it to memory
        
        # print(df.tail())
        return df
    
    except Exception as e:

        print("Exception occured",e)
        df=pd.DataFrame()
        date = kwargs['start_date'].strftime("%Y-%m-%d")
        if date not in UnavailDateList:
            UnavailDateList.append(date)
        return df



def get_expiry(date):

    ExpDf = pd.read_excel(ExpiryFILE)

    ExpDf["DataTime"] = pd.to_datetime(ExpDf["DataTime"])

    date=pd.to_datetime(date)

    mask = ExpDf["DataTime"] >= date
    
    # Find the index of the first occurrence of True in the mask
    next_greater_index = mask.idxmax()

    # Select the row with the next greater date
    next_greater_date_row = ExpDf.loc[next_greater_index]

    return next_greater_date_row["DataTime"]

"""_______________________________________________________________________________________________________________________________________________________________________-"""

def resample_future_data_fn():

    # future = pd.read_csv("future_2022.csv")

    future = query(f_o='F', instrument=instruments, start_date=Start_DT,
                      end_date=End_DT, STime="09:15:00")


    future['Timestamp'] = pd.to_datetime(future['DateTime'])
     # Convert 'DateTime' to datetime format
    future.set_index('DateTime', inplace=True)  # Set 'DateTime' as the index
    future_data = future.set_index('Timestamp').resample(f'{candeltime}T').agg({

        'Date': 'first',
        'Time': 'first',
        'Ticker': 'first',
        'Volume': 'sum',
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last'
    })
    future_data.reset_index(inplace=True)
    future_data = future_data.dropna(subset=['Date'])
    # future_data.to_csv(f"resampled_future_CE_PE_{fileyear}.csv")
    
  
    return future_data, future
"""________________________________________________________________________________________________________________________________________________"""

def calculate_SMA_with_signals(df, future):
    df[sma1] = df["Close"].rolling(window=10).mean()
    df[sma2] = df["Close"].rolling(window=30).mean()

    # Assuming df is your DataFrame
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df.set_index('Timestamp', inplace=True)
    future_data = df.between_time(start_time, end_time)

    # DataFrame index  reset 
    future_data = future_data.reset_index(drop=True)
  
    # Create 'signal' and 'ATMSP' columns if they don't exist
    if 'signal' not in future_data.columns:
        future_data['signal'] = ''
    if 'close_hit_signal' not in future_data.columns:
        future_data['close_hit_signal'] = ''    
    if 'ATMSP' not in future_data.columns:
        future_data['ATMSP'] = 0

    for index, row in future_data.iterrows():
        future_highhit = None
        future_lowhit = None
        if index > 0:
            if  (row[sma1] > row[sma2])and (future_data[sma1][index - 1] < future_data[sma2][index - 1]) and (pd.to_datetime(row['Time']).time() < end_time):
                future_data.at[index, 'signal'] = 'Bullish'
                if row['Close'] > row[sma1]:
                    future_data.at[index, 'close_hit_signal'] = 'CloseHIT'
                    future_highhit = future[(future['Date'] == row['Date']) & (future['Time'] > row['Time']) & (future['High']>row['High'])].reset_index(drop=True)
                    if not future_highhit.empty:
                        future_data.at[index, 'ATMSP'] = round(future_highhit['Low'][0]/100)*100
                        future_data.at[index, 'EntryTime'] = future_highhit['Time'][0]
          
            elif (row[sma1] < row[sma2]) and (future_data[sma1][index - 1] > future_data[sma2][index - 1]) and (pd.to_datetime(row['Time']).time() < end_time):
                future_data.at[index, 'signal'] = 'Bearish'
                if row['Close'] < row[sma1]:
                    future_data.at[index, 'close_hit_signal'] = 'CloseHIT'
                    future_lowhit = future[(future['Date'] == row['Date']) & (future['Time'] > row['Time']) & (future['Low']<row['Low'])].reset_index(drop=True)
                    if not future_lowhit.empty:
                        future_data.at[index, 'ATMSP'] = round(future_lowhit['High'][0]/100)*100
                        future_data.at[index, 'EntryTime'] = future_lowhit['Time'][0]
                     

            elif (pd.to_datetime(row['Time']).time() == end_time):
                future_data.at[index, 'signal'] = 'Sell'

        else:
            print("Not enough data for calculation.")      

    future_data.to_csv(f"{sma1}_{sma2}_resample_and_signal_future_CE_PE_{fileyear}.csv")        

    future_data = future_data[(future_data['ATMSP'] != 0) | (future_data['signal'] == 'Sell')].reset_index(drop=True)
  
    future_data = future_data.sort_values(by=['Date', 'EntryTime'], ascending=[True, True])
    # Filter rows with alternative signs
    future_data = future_data[future_data['signal'] != future_data['signal'].shift(1)]    
        # Reset the index
    future_data.reset_index(inplace=True)
    
    # Drop the index column
    future_data = future_data.drop(columns=['index'])
    
    future_data.to_csv(f"{sma1}_{sma2}_Filetred_resample_and_signal_future_CE_PE_{fileyear}.csv")
    
    return future_data

"""_______________________________________________________________________________________________________________________________________________"""

def option_trade_execution(row, next_row, option, next_signal):

    Option_file = pd.DataFrame()
    new_data = {}
    # Extract relevant information from the current row
    
    buy_atmsp = row['ATMSP']
    buy_date = row['Date']
    buy_time = row['EntryTime']

    # Calculate the expiry date based on the buy date
    expiry_date = get_expiry(buy_date)
    # Check if all required information is available
    if all([buy_atmsp, buy_date, buy_time, expiry_date]):
        # Fetch option data using the query function
        Option_file = query(f_o='O', instrument=instruments, expiry_dt=expiry_date, strike=buy_atmsp, option_type=option, start_date=pd.to_datetime(pd.to_datetime(buy_date).date()), end_date=pd.to_datetime(pd.to_datetime(buy_date).date()))
        # Check if option data is not empty
        if not Option_file.empty:
            # Extract option data at the same datetime as the buy time
            option_buy = Option_file[(Option_file['Time']) == buy_time]
            option_buyprice = None  

            if not option_buy.empty:
                option_buyprice = option_buy['Close'].values[0]
                # Check if the next row is available and has the expected signal
                if next_row is not None and next_row['signal'] in {next_signal, 'Sell'}:
                    # Calculate the sell time based on the next row's time
                    sell_time = next_row['EntryTime']
                    if next_row['signal'] == 'Sell':
                        sell_time = next_row['Time']
                
                    # Extract option data at the same datetime as the sell time
                    option_sell = Option_file[(Option_file['Time']) == sell_time]
                    option_sellprice = None   # Replace with a suitable default value

                    # Check if option data at the same datetime is not empty
                    if not option_sell.empty:
                        option_sellprice = option_sell['Close'].values[0]
                    # Create a dictionary with the trade details
                    new_data = {
                            'Strategy' : f"CE_PE_{sma1}_{sma2}_{fileyear}",
                            'Date': buy_date,
                            'Transaction': 'Buy',
                            'Symbol': instruments,
                            'Qty':quantity,
                            'Option': option,
                            'Strike' : buy_atmsp,
                            'ExpiryDt': expiry_date,
                            'EnTime': buy_time,
                            'BPrice': option_buyprice,
                            'ExTime': sell_time,
                            'SPrice':option_sellprice,
                            }
                                        
    return new_data
"""__________________________________________________________________________________________________________________________________________________"""

def main(future_data):    

    new_data_list = []
    # Iterate through each row in the future_data DataFrame
    for index in range(1, len(future_data)):
        row = future_data.iloc[index]
        prev_row = future_data.iloc[index - 1]
        next_row = future_data.iloc[index + 1] if index + 1 < len(future_data) else None
        
        # Check if the current signal is 'Bullish'
        if row['signal'] == 'Bullish' and prev_row['signal'] in {'Bearish', 'Sell'}:
            # Call the option_trade_execution function for Bearish signal
            new_data_ce = option_trade_execution(row, next_row, option = 'CE', next_signal= "Bearish")
            if new_data_ce:  # Check if new_data_ce is not an empty dictionary
                new_data_list.append(new_data_ce)
        # Check if the current signal is 'Bearish' and the time is before the specified end time
        elif row['signal'] == 'Bearish' and prev_row['signal'] in {'Bullish', 'Sell'}:

            # Call the option_trade_execution function for Bullish signal
            new_data_pe = option_trade_execution(row, next_row, option = 'PE', next_signal= "Bullish")
            if new_data_pe:  # Check if new_data_pe is not an empty dictionary
                new_data_list.append(new_data_pe)    

        else:
      
            print("Option data is empty.")

    return new_data_list 


df, future = resample_future_data_fn()
sigdf = calculate_SMA_with_signals(df, future)
tradelist = main(sigdf)
tradedf = pd.DataFrame(tradelist)
tradedf = tradedf.dropna(subset=['Date'])
tradedf.to_csv(f"{sma1}_{sma2}_Trade_option_CE_PE_{fileyear}.csv")

"""____________________________________________________________________________________________________________________________________________________"""
