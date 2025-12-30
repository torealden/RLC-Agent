from bs4 import BeautifulSoup
from selenium.webdriver.common.keys import Keys
import numpy as np
import pandas as pd
import re
from sqlalchemy import create_engine
import os
import requests 
import pymysql

class lcfs():
    def __init__(self,serv_info):
        self.serv_info = serv_info
        self.url = 'https://ww3.arb.ca.gov/fuels/lcfs/lrtqsummaries.htm'


    def get_lcfs_csv(self,url,sheetname):
        header_index_start = []
        header_index_end = []
        res = requests.get(url)
        soup = BeautifulSoup(res.content, 'lxml')
        #print(soup)
        #l = soup.find(href=re.compile(r'^https://ww3.arb.ca.gov/fuels/lcfs/dashboard/quarterlysummary/quarterlysummary_*'))
        l = soup.find(href=re.compile(r'^/fuels/lcfs/dashboard/quarterlysummary/quarterlysummary_*'))
        df = pd.read_excel("https://ww3.arb.ca.gov"+l['href'],sheet_name=sheetname,skiprows=[0])
        df = df.transpose()
        
        for count,element in enumerate(df.iloc[0]):
            if(element=='Fuel - Feedstock'):
                header_index_start.append(count)
            elif(element=='Alternative Jet Fuel'):
                header_index_end.append(count)
                
        header = df.iloc[0]
        header = header[header_index_start[0]:header_index_end[0]]
        header = header.apply(lambda x: re.sub('[^0-9a-zA-Z]+', '_', str(x)))
        header = header.apply(lambda x: re.sub('^High_Solids_Anaerobic_Digestion_HSAD_Food_Waste_Waste_Water', 'H_S_A_D_W_W', str(x)))
        header = header.apply(lambda x: re.sub('^Fuel_Feedstock', 'Quarter', str(x)))

        #df = df.rename(columns = header)
        df.drop(df.head(2).index,inplace=True)

        lcfs_feedstock = df[df.columns[header_index_start[0]:header_index_end[0]]].dropna(how='all')
        lcfs_feedstock.columns =  header
        
        lcfs_credit = df[df.columns[header_index_start[1]:header_index_end[1]]].dropna(how='all')
        lcfs_credit.columns = header
        
        lcfs_deficit = df[df.columns[header_index_start[2]:header_index_end[2]]].dropna(how='all')
        lcfs_deficit.columns = header
    

        
        lcfs_feedstock = lcfs_feedstock.reset_index(drop=False)
        lcfs_feedstock = lcfs_feedstock.rename(columns={"index": "Year"})
        lcfs_feedstock = self.format_df(lcfs_feedstock)
        

        

        lcfs_credit = lcfs_credit.reset_index(drop=False)
        lcfs_credit = lcfs_credit.rename(columns={"index": "Year"})
        lcfs_credit = self.format_df(lcfs_credit)
        

        lcfs_deficit = lcfs_deficit.reset_index(drop=False)
        lcfs_deficit = lcfs_deficit.rename(columns={"index": "Year"})
        lcfs_deficit = self.format_df(lcfs_deficit)
        
        #table_name = ['lcfs_feedstock','lcfs_credit','lcfs_deficit']

        #update_table(lcfs_feedstock,table_name[0])
        #update_table(lcfs_credit,table_name[1])
        #update_table(lcfs_deficit,table_name[2])
        
        return {'lcfs_feedstock':lcfs_feedstock,
                'lcfs_credit':lcfs_credit,
                'lcfs_deficit':lcfs_deficit}

        
        
        
        
    def format_df(self,df):
        
        index = []
        pd_date = {}
        
        for count,element in enumerate(df['Year']):
            if(isinstance(element,int)):
                index.append(count+1)

        for i in index:
            pd_date[i] = df.iloc[i-1]['Year']
            
        keys = np.array(list(pd_date.keys()))
        for index, row in df.iterrows():
            try:
                date_index = np.amin(keys[keys > index])
                #print"index:{} date: {}".format(index, pd_date[date_index])
                df.loc[index,'Year'] = str(pd_date[date_index])
            except Exception as e:
                print(e)
            
        #df = df.drop(columns=['Total Volume'])  
        #df  = df.set_index(['Year','Quarter'])
        #convert Quarter to month
        df['Month']= df.Quarter.apply(lambda x: str((int(x.strip('Q'))-1)*3 + 1)) 
        #df["date"] = pd.Series(str(df['Month']) +"/1/"+str(df["Year"]), index =df.index)
        df['date'] = df[['Month','Year']].apply(lambda row: '/1/'.join(row.values.astype(str)), axis=1)
        df["date"] = pd.to_datetime(df["date"],format = '%m/%d/%Y').dt.date
        df = df.drop(columns=['Quarter','Month'])
        df = df.set_index(['date'])
        return df

    def update_table(self,dataframe,table_name,serv_info):
        # Obtain connection string information from the portal
        engine = create_engine("mysql+pymysql://{}:{}@{}/thejacobsen".format(serv_info[1],
                                                                            serv_info[2],
                                                                            serv_info[0]))
        print("Updating ",table_name)
        dataframe.to_sql(table_name, engine, if_exists='replace')
        print(table_name," Updated")

        
    def run_lcfs(self):
        tables = self.get_lcfs_csv(self.url,2)
        for table_name in tables:
            self.update_table(tables[table_name],table_name,self.serv_info)
            


if __name__ == "__main__":
    serv_info = ['jakescrape.mysql.database.azure.com','jakeadmin@jakescrape','Gvc$35lkaaPq!']
    selection = [True,False,False]
    lcfs = lcfs(serv_info)
    lcfs.run_lcfs()