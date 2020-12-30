import os
import requests
import pypyodbc as odbc             #pip install pypyodbc
from urllib.parse import urljoin
from bs4 import BeautifulSoup       #pip install beautifulsoup4
import pandas as pd                 #pip install pandas

#On line 28, I am downloading the files in C drive, Do change as per you preference
#Filling 0 in PPWAP column for NULL values as 0 is never 
# occuring in the PPWAP column of the database 

#Depends on your settings...
DRIVER = 'SQL Server'
SERVER_NAME = 'AMIT-THINKPAD\SQLEXPRESS'
DATABASE_NAME = 'BASEDATA'

#To filter out other prefixes
def matc(string):
    if 'dwbfpricesukplace' in string or 'dwbfpricesireplace' in string:
        return True
    return False

url = 'https://promo.betfair.com/betfairsp/prices'

#Its a fix that the script would download the files into this folder and then import it back

#Choose your preferred location to download the csv
folder_location = r'C:\webscraping'
if not os.path.exists(folder_location):os.mkdir(folder_location)

response = requests.get(url)
soup= BeautifulSoup(response.text, "html.parser")   


def connection_string(driver, server_name, database_name):
    conn_string = f"""
        DRIVER={{{driver}}};
        SERVER={server_name};
        DATABASE={database_name};
        Trust_Connection=yes;
    """
    return conn_string

try:
    conn = odbc.connect(connection_string(DRIVER, SERVER_NAME, DATABASE_NAME))
except odbc.DatabaseError as e:
    print("Database error")
    print(str(e.value[1]))
except odbc.Error as e:
    print("Connection Error:")
    print(str(e.value[1]))

cursor = conn.cursor()

sql_insert ='''
    INSERT INTO bf_hist_data_place
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
'''

################## Here you Enter the date #################                                    DATE
#if it is 05-01-2010, It gets converted to int 20110805
Date = input("Enter DATE (DD-MM-YYYY) : ")
Date = "".join(Date.split('-'))
Date = int(Date[4:] + Date[2:4] + Date[:2])

for link in soup.select("a[href$='.csv']"):
    ######### Fdate is extracted from the name of the csv file ######
    Fdate = str(link['href'])[-12:-4]
    Fdate = "".join(Fdate.split('-'))
    Fdate = int(Fdate[4:] + Fdate[2:4] + Fdate[:2])
    
    ############# Here the condition is being checked ############                              Condition
    if matc(link['href']) and Fdate <= Date and Fdate >= 20100101:                              #Between Date and 01-01-2010
    
        filename = os.path.join(folder_location,link['href'].split('/')[-1])
        
        with open(filename, 'wb') as f:
            f.write(requests.get(urljoin(url,link['href'])).content)
                   
        df = pd.read_csv(filename)
        rc = df.shape[0]
        
        for i in range(rc):
            df['EVENT_DT'].iloc[i] = df['EVENT_DT'].iloc[i] + ":00"
        
        df['EVENT_DT'] = pd.to_datetime(df['EVENT_DT']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df.fillna(0, inplace=True)
        records = df.values.tolist()


        try:
            
            cursor.executemany(sql_insert, records)
            cursor.commit();
        except Exception as e:
            cursor.rollback()
            print(str(e))
        finally:
            print("Done")

        #To clear up temporarily downloaded CSVs
        os.remove(filename)


print("ALL DONE")
cursor.close()
conn.close()
