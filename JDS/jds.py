from kivy.app import App
from kivy.uix.widget import Widget
from kivy.uix.label import Label
from kivy.uix.gridlayout import GridLayout
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.pagelayout import PageLayout
from kivy.uix.progressbar import ProgressBar
from kivy.uix.checkbox import CheckBox
from kivy.properties import StringProperty
from kivy.lang import Builder
import time
from kivy.clock import Clock
import threading
from kivymd.app import MDApp
import sys
import requests
import ast
from datetime import datetime
import pandas as pd
from collections import defaultdict
import time
import sqlalchemy as sa
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import re
from sqlalchemy import create_engine
import os
from io import StringIO
import html5lib
from sqlalchemy import Table
from sqlalchemy import Column
from sqlalchemy import MetaData
from sqlalchemy.dialects.mysql import DOUBLE as Double
from sqlalchemy.dialects.mysql import DATE as Date
import sqlalchemy
from sqlalchemy.dialects.mysql import insert
import pymysql
import json

from kivy.core.window import Window
Window.size = (400, 550)



class gtt():
    def __init__(self,serv_info,gtt_info):

        self.serv_info = serv_info

        self.username = gtt_info[0]
        self.password = gtt_info[1]
        self.start_date = gtt_info[2]
        self.end_date = gtt_info[3]

        token_url = 'https://www.globaltradetracker.com/api/rest/gettoken?userid={0}&password={1}'.format(self.username,self.password)

        r = requests.get(token_url)
        self.token = r.text
                                                                            
        self.import_codes = self.read_table('hs_codes',self.serv_info)
        self.export_codes = self.read_table('hs_codes',self.serv_info)
        self.countries = self.read_table('gtt_countries', self.serv_info)
        self.hs_list = []
        self.hs_sub_list = defaultdict(list)
        self.build_sub_dic()


    def construct_table(self,dataframe):
        # Obtain connection string information from the portal
        engine = create_engine("mysql+pymysql://{}:{}@{}/thejacobsen".format(self.serv_info[1],
                                                                            self.serv_info[2],
                                                                            self.serv_info[0]))
        print("Updating gtt Table")
        #column names = ['date','flowtype','reporter','partner','hs_code','quantity','unit']
        dataframe.to_sql('gtt', engine, if_exists='replace',
                        dtype={
                            'ind':    sa.types.VARCHAR(length=255),
                            'flowtype': sa.types.VARCHAR(length=255),
                            'reporter': sa.types.VARCHAR(length=255),
                            'partner':  sa.types.VARCHAR(length=255),
                            'hs_code':  sa.types.VARCHAR(length=255),
                            'commodity':  sa.types.VARCHAR(length=255)}
                        )
        print("gtt Table Updated")

    def read_table(self,table_name,serv_info):
        # Obtain connection string information from the portal
        engine = create_engine("mysql+pymysql://{}:{}@{}/thejacobsen".format(serv_info[1],
                                                                            serv_info[2],
                                                                            serv_info[0]))
        print("reading" ,table_name)
        df = pd.read_sql_table(table_name, engine, index_col='index')
        return df


    def build_sub_dic(self):
        for i in self.get_subs():
            self.hs_sub_list[i['country']] = i['hsCodes']


    def build_url(self,token,flowType,reporter,hscode,periodType='M',startDate='2000-01',endDate='2019-04'):
        url = '''
                https://www.globaltradetracker.com/api/rest/getreport?token={}&impexp={}&periodtype={}&from={}&to={}
                &reporter={}&accumulatereportergroups=true&accumulatepartner
                groups=true&source=DEFAULT&hscode={}&hslevel=0&currency=&unit=&decimalscale=&customconversionrules=true&decimalscale=3&format=j
                son
                '''.format(token,flowType,periodType,self.start_date,self.end_date,reporter,hscode) 
        #print url
        return url 


    def get_subs(self):
        sub_url = 'https://www.globaltradetracker.com/api/rest/getreport/subscriptions?token={}&periodType=M'.format(self.token)
        sub_get = requests.get(sub_url)
        subs = ast.literal_eval(sub_get.text)
        
        return subs
    

    def filter_hs_list(self,country,hs_list):
        hs_codes = []
        for i in hs_list:
            for j in self.hs_sub_list[country]:
                if j in i:
                    hs_codes.append(i)
        hs_code = ",".join(hs_codes[1:])
        hs_code = hs_codes[0]+"%2C"+hs_code
        return hs_code


    def get_import(self,country):
        hs_code = self.filter_hs_list(country,self.import_codes.hs_code.astype(str))
        try:
            url = self.build_url(self.token,'I',country,hs_code)
            list_imp = requests.get(url).text
            l_imp = json.loads(list_imp)
            for imp in l_imp:               
                date = datetime.strptime(str(imp['period'][0])+"/"+str(imp['period'][1])+'/1','%Y/%m/%d').date()
                flowType = imp['flowType']
                reporter = imp['reporter']['code']
                partner = imp['partner']['name']
                code = imp['commodity']['code']
                quantity = imp['quantity1']['number']
                unit = imp['quantity1']['unit']

                if unit == 'MT':
                    quantity = quantity*1000

                index = str(imp['period'][0])+str(imp['period'][1])+flowType+imp['reporter']['code']+imp['partner']['code']+code
                index = index.replace('(','').replace(')','')
                index = index.replace('-','')
                
                l= [date,flowType,reporter,partner,code,quantity,index]
                if len(l)>0:
                    print(l)
                    self.hs_list.append(l)
                    #print hs_list
                else:
                    print(url)

        except Exception as e:
            print(str(e))
            url = self.build_url(self.token,'I',country,hs_code)
            list_imp = requests.get(url).text
            print(list_imp)
            print("GTT subscription not found")

    def get_export(self,country):
        hs_code = self.filter_hs_list(country,self.export_codes.hs_code.astype(str))
        try:
            url = self.build_url(self.token,'E',country,hs_code)
            list_exp = requests.get(url).text
            l_exp = json.loads(list_exp)
            for exp in l_exp:
                date = datetime.strptime(str(exp['period'][0])+"/"+str(exp['period'][1])+'/1','%Y/%m/%d').date()
                flowType = exp['flowType']
                reporter = exp['reporter']['code']
                partner = exp['partner']['name']
                code = exp['commodity']['code']
                quantity = exp['quantity1']['number']
                unit = exp['quantity1']['unit']

                if unit == 'MT':
                    quantity = quantity*1000


                index = str(exp['period'][0])+str(exp['period'][1])+flowType+exp['reporter']['code']+exp['partner']['code']+code
                index = index.replace('(','').replace(')','')
                index = index.replace('-','')

                #print reporter,partner,code,commodity,quantity
                l = [date,flowType,reporter,partner,code,quantity,index]
                if len(l)>0:
                    print(l)
                    self.hs_list.append(l)
                    #print hs_list
        except Exception as e:
            print(str(e))
            url = self.build_url(self.token,'E',country,hs_code)
            list_exp = requests.get(url).text
            print(list_exp)
            print("GTT subscription not found")

    def create_dataframe(self):
        checksum = pd.concat([self.import_codes,self.export_codes]).drop_duplicates()
        start_time = time.time()
        print(start_time)
        for i in self.countries.code.values:
            self.get_import(i)
            self.get_export(i)

        print("--- %s seconds ---" % (time.time() - start_time))
        print("data set size:{}".format(len(self.hs_list)))
        header = ['date','flowtype','reporter','partner','hs_code','quantity','ind']
        data = pd.DataFrame(self.hs_list,columns=header)
        data['commodity'] = data.hs_code.apply(lambda x:checksum[checksum.hs_code==int(x)].commodity.values[0])
        data.set_index('ind',inplace=True)
        data = data.groupby(['ind','date','flowtype','reporter','partner','hs_code','commodity'],as_index=True).sum()
        print(data.head())
        
        self.construct_table(data)
        return data

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
            




class JDSView(BoxLayout):
    
    stop = threading.Event()


    def update(self):

        scraper = threading.Thread(target=self.update_loop)
        scraper.daemon = True
        scraper.start()



    def update_loop(self):

        db_s = self.ids['d_server'].text
        db_u = self.ids['d_username'].text 
        db_p = self.ids['d_password'].text


        g_u = self.ids['g_username'].text 
        g_p = self.ids['g_password'].text
        g_start = self.ids['start_date'].text 
        g_end = self.ids['end_date'].text

        serv_info = [db_s,db_u,db_p]
        gtt_info= [g_u,g_p,g_start,g_end]

        self.ids['status_update'].value = 0
        if self.ids['GTT'].active:
            self.button_state.text = "Updating GTT"
            gtt_go =  gtt(serv_info,gtt_info)
            gtt_go.create_dataframe()
            self.ids['status_update'].value = self.ids['status_update'].value + .25

        if self.stop.is_set():
            return

        if self.ids['LCFS'].active:
            self.button_state.text = "Updating LCFS"
            lcfs_go = lcfs(serv_info)
            lcfs_go.run_lcfs()
            self.ids['status_update'].value = self.ids['status_update'].value + .25

        if self.stop.is_set():
            return

        if self.ids['EIA'].active:
            self.button_state.text = "Updating EIA"
            eia_go =eia(serv_info)
            eia_go.run_eia()
            self.ids['status_update'].value = self.ids['status_update'].value + .25

        if self.stop.is_set():
            return

        if self.ids['EMTS'].active:
            self.button_state.text = "EMTA is not active"
            self.ids['status_update'].value = self.ids['status_update'].value + .25

        self.ids['status_update'].value = 1
        
    


        if self.stop.is_set():
            return

class JDSApp(MDApp):
    def __init__(self, **kwargs):
        self.title = "The Jacobsen Data Scrapper"
        super().__init__(**kwargs)


    def on_stop(self):
        self.root.stop.set()
    def build(self):
        jds = JDSView()
        return jds





if __name__ == "__main__":
    JDSApp().run()