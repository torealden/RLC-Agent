import pandas as pd
import re
import requests 
from bs4 import BeautifulSoup
from io import StringIO
import html5lib
from collections import defaultdict
from datetime import datetime
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy import Table
from sqlalchemy import Column
from sqlalchemy import MetaData
from sqlalchemy.dialects.mysql import DOUBLE as Double
from sqlalchemy.dialects.mysql import DATE as Date
import sqlalchemy
from sqlalchemy.dialects.mysql import insert
import pymysql

class eia():
    def __init__(self,serv_info):

        self.serv_info = serv_info

        self.eia_url = 'https://www.eia.gov/biofuels/biodiesel/production/'

        self.pd_header = [0,2,4,0,0]

        self.pd_date = {}


    def get_eia_csv(self,url,sheet=0):
        eia_frames = {}
        res = requests.get(url)
        soup = BeautifulSoup(res.content, 'lxml')
        link_list = soup.find_all('a', {'class': re.compile(r'ico_xls')})
        
        table_name = [a['href'] for a in soup.find_all('a', {'class': re.compile(r'ico_xls')})]
        eia_csv = [url+a  for a in table_name]
        
        for index,val in enumerate(eia_csv):
            try:
                eia_frames[table_name[index]] = pd.read_excel(val,sheet_name=sheet, header=self.pd_header[index])
            except:
                print("didn't convert xls to dataframe",val)
        
        return eia_frames,table_name

    def get_eia_total(self,url):
        d = 0
        t = 0
        res = requests.get(url)
        soup = BeautifulSoup(res.content, 'lxml')
        link_list = soup.find_all('div', {'class': re.compile(r'main_col')})
        for txt in link_list:
            x = re.findall(r"([^.]*?million pounds of feedstocks[^.]*\.)",txt.text) 
            for val in x:
                val = val.replace(",",'').replace('.','').split()
                filter_val =filter(lambda x: x.isdigit(),val) 
                total = int(next(filter_val))
                date = val[-2][0:3] +" 1 "+val[-1]
                date = datetime.strptime(date, '%b %d %Y').date()
                d = date
                t = total 
    
        return d,t

    def format_feedstock(self,sheet=0):
        eia_data_list,eia_table_name = self.get_eia_csv(self.eia_url,sheet)
        df = eia_data_list[eia_table_name[2]]
        cols = [c for c in df.columns if c.lower()[:7] != 'unnamed']
        df = df[cols]
        df.drop(df.tail(12).index,inplace=True)
        #df.drop(df.head(1).index,inplace=True)
        df = df.dropna(how='all')
        df.drop(df.Period[df.Period.astype(str).str.contains('Total', regex=True)].index,inplace=True)
        #df.Period[df.Period.astype(str).str.contains('2', regex=True)]
        
        df = df.reset_index(drop=True)
        indexes = df.index.values[np.array(df.index.values%13 == 0)]

        for i in indexes:
            self.pd_date[i] = df.iloc[i].Period
            
        keys = np.array(list(self.pd_date.keys()))

        for index, row in df.iterrows():
            try:
                date_index = np.amax(keys[keys < index])
                #print"index:{} date: {}".format(index, date_index)
                df.loc[index,'Year'] = self.pd_date[np.amax(keys[keys < index])]
            except:
                pass


        df.drop(df.Period[df.Period.astype(str).str.contains('[0-9]', regex=True)].index,inplace=True)
        df.Year = df.Year.astype(int)
        df.Year = df.Year.astype(str)
        df.Period = df.Period.apply(lambda x: x[0:3])
        df = df.reset_index(drop=True)
        df["date"] = pd.Series(df['Period'] +"/1/"+df["Year"], index =df.index)
        df["date"] = pd.to_datetime(df["date"],format = '%b/%d/%Y')
        df.drop(columns=['Period','Year'], axis=1, inplace=True)
        
        return df
            
    def format_production(self):
        eia_data_list,eia_table_name = self.get_eia_csv(self.eia_url)
        df = eia_data_list[eia_table_name[1]]
        cols = [c for c in df.columns if c.lower()[:7] != 'unnamed']
        df = df[cols]
        df.drop(df.tail(12).index,inplace=True)
        #df.drop(df.head(1).index,inplace=True)
        df = df.dropna(how='all')
        df.drop(df.Period[df.Period.astype(str).str.contains('Total', regex=True)].index,inplace=True)
        #df.Period[df.Period.astype(str).str.contains('2', regex=True)]
        
        df = df.reset_index(drop=True)
        indexes = df.index.values[np.array(df.index.values%13 == 0)]

        for i in indexes:
            self.pd_date[i] = df.iloc[i].Period

        keys = np.array(list(self.pd_date.keys()))
        
        for index, row in df.iterrows():
            try:
                date_index = np.amax(keys[keys < index])
                #print"index:{} date: {}".format(index, date_index)
                df.loc[index,'Year'] = self.pd_date[np.amax(keys[keys < index])]
            except:
                pass


        df.drop(df.Period[df.Period.astype(str).str.contains('[0-9]', regex=True)].index,inplace=True)
        df.Year = df.Year.astype(int)
        df.Year = df.Year.astype(str)
        df.Period = df.Period.apply(lambda x: x[0:3])
        df = df.reset_index(drop=True)
        df["date"] = pd.Series(df['Period'] +"/1/"+df["Year"], index =df.index)
        df["date"] = pd.to_datetime(df["date"],format = '%b/%d/%Y')
        df.drop(columns=['Period','Year'], axis=1, inplace=True)

        return df  

    def all_total(self):
        date_list = {}
        df = self.format_feedstock()
        date = df.date.tolist()
        date = [str(i.year)+"_"+'%02d' % i.month for i in date]
        for i in date:
            url = 'https://www.eia.gov/biofuels/biodiesel/production/archive/{}/{}/biodiesel.php'.format(i[0:4],i)
            d,t = self.get_eia_total(url)
            date_list[d] = t
            
        df = pd.DataFrame(date_list.items(),columns=['date','total'])
        df = df.set_index('date')
        return df
        

    def feedstock(self):
        veg_oil = self.format_feedstock()
        veg_oil = veg_oil.rename(columns={"Other":"other_sunflower_oil"})
        veg_oil = veg_oil.set_index('date')
        anm_oil = self.format_feedstock(1)
        anm_oil = anm_oil.rename(columns={"Other":"other_lard","Other.1":"other_grease","Other.2":"other"})
        anm_oil = anm_oil.set_index('date')
        anm_oil.drop(columns=['Alcohol','Catalysts'], axis=1, inplace=True)
        prod = self.format_production()
        prod = prod.set_index('date')
        total = self.all_total()

        result = pd.concat([veg_oil, anm_oil], axis=1, sort=False)
        result = result.rename(columns={'Canola oil':'canola_oil','Corn oil':'corn_oil','Cottonseed oil':'cottonseed_oil',
                                    'Palm oil':'palm_oil','Soybean oil':'soybean_oil','Poultry':'poultry_fat',
                                    'Tallow':'tallow','White grease':'white_grease','Yellow grease':'yellow_grease',
                                    'Algae':'algae'})
        
        p = re.compile('[W()]')
        s = re.compile('[(s)]')
        result = result.loc[:,~result.columns.duplicated()]
        result['other_sunflower_oil']=result.other_sunflower_oil.apply(lambda x: np.where(p.match(str(x)) ,2.00,x))
        result['cottonseed_oil']=result.cottonseed_oil.apply(lambda x: np.where(p.match(str(x)) ,1.00,x))
        result['palm_oil']=result.palm_oil.apply(lambda x: np.where(p.match(str(x)) ,1.00,x))
        result=result.replace('(s)',1)
        result=result.replace('W',np.nan)
        
        #get a list of date with only 1 null value
        all_dates = result.index.tolist()
        #get subtract sum of all columns from total feedstock
        column_average = result.mean(axis=0,skipna=True).sort_values(ascending = True)
        
        #for d in range(result.shape[0]):
        for d,row in result.iterrows():
            for i,v in column_average.items():
                
                
                c_total = np.array(result.loc[d,:].to_numpy(),dtype=np.float32)
                save_c = c_total
                null_values = c_total[np.isnan(c_total)]
                num_null = len(null_values)
                #print(c_total,null_values,num_null)
                c_total = c_total[~np.isnan(c_total)]       
                a_total = np.array(total.loc[d.date(),:].to_numpy(),dtype=np.float32)
                total_diff = a_total.sum() - c_total.sum()
                
                if (total_diff > v) and (num_null>1) :
                    #result[i] = result[i].apply(lambda x: v if pd.isnull(x) else x )
                    if pd.isna(result.loc[d,i]):
                        #print(save_c,"i added value")
                        result.loc[d,i] = v
                else:
                    #result[i] = result[i].apply(lambda x: total_diff if pd.isnull(x) else x)
                    if pd.isna(result.loc[d,i]):
                        #print(save_c,'i added diff of total')
                        result.loc[d,i] = total_diff
                        
                #print(d.date(),c_total.sum(),a_total)
                    
        checker = np.array(result.iloc[0,:],dtype=np.float32)
        print(checker,np.sum(checker),total.iloc[0,:])
        
        result = pd.concat([result,total], axis=1, sort=False)
        
        return result

    def production(self):
        prod = self.format_production()
        prod = prod.set_index('date')

        prod = prod.rename(columns={'B100 production':'b100_production','Sales of B100':'sales_of_b100',
                                    'Sales of B100 included in biodiesel blends':'sales_of_b100_included_in_biodiesel_blends',
                                    'Ending stocks of B100':'ending_stocks_of_b100','B100 stock change':'b100_stock_change'})
        

        return prod

    def feed_prod(self):
        feed = self.feedstock()
        prod = self.production()
        
        f_n_p = pd.concat([feed,prod], axis=1, sort=False)
        
        return f_n_p

    def update_table(self,dataframe,tableName,serv_info):
        # Obtain connection string information from the portal
        engine = create_engine("mysql+pymysql://{}:{}@{}/thejacobsen".format(serv_info[1],
                                                                            serv_info[2],
                                                                            serv_info[0]),pool_recycle=1800)
        conn = engine.connect()
        

        print("Updating ", tableName)
        #dataframe.to_sql('eia_feedstock', engine, if_exists='append')
        meta = MetaData()
        meta.reflect(bind=engine)
        #table = meta.tables[tableName]
        table = Table(tableName, meta, autoload=True, 
            autoload_with=engine)
        
        
        print(dataframe)
        df = dataframe.reset_index(level=0)
        df['date'] = df['date'].dt.date
        
        insert_values = df.to_dict(orient='records')
        
        for row in insert_values:
            insert_statement = insert(table).values(row)
            upsert_statement = insert_statement.on_duplicate_key_update(row)
            conn.execute(upsert_statement)
        conn.close()
        print( tableName,"Updated")

            
        
        
            

    def run_eia(self):

        feedstock_table = self.feedstock() # get the full dateframe with all columns, (s) & W haven't been formatted
        production_table = self.production()
        #dataframe = feed_prod()
        self.update_table(feedstock_table,"eia_feedstock",self.serv_info)
        self.update_table(production_table,'eia_production',self.serv_info)

if __name__ == "__main__":
    serv_info = ['jakescrape.mysql.database.azure.com','jakeadmin@jakescrape','Gvc$35lkaaPq!']
    selection = [True,False,False]
    eia =eia(serv_info)
    eia.run_eia()