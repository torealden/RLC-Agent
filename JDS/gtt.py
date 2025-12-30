import requests
import ast
from datetime import datetime
import pandas as pd
from collections import defaultdict
import time
from sqlalchemy import create_engine
import sqlalchemy as sa
#pd.set_option('display.precision',9)
import pymysql

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
        print("Updating Table")
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
        print("Table Updated")

    def read_table(self,table_name,serv_info):
        # Obtain connection string information from the portal
        engine = create_engine("mysql+pymysql://{}:{}@{}/thejacobsen".format(serv_info[1],
                                                                            serv_info[2],
                                                                            serv_info[0]))
        print("reading" ,table_name)
        df = pd.read_sql_table(table_name, engine, index_col='index')
        return df

    def get_subs(self):
        sub = []
        sub_url = 'https://www.globaltradetracker.com/api/rest/getreport/subscriptions?token={}&periodType=M'.format(self.token)
        sub_get = requests.get(sub_url)
        subs = ast.literal_eval(sub_get.text)
        
        return subs
    

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
            l_imp = ast.literal_eval(list_imp)
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
            l_exp = ast.literal_eval(list_exp)
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
        
        #self.construct_table(data)
        return data


if __name__ == "__main__":

    serv_info = ['jakescrape.mysql.database.azure.com','jakeadmin@jakescrape','Gvc$35lkaaPq!']
    selection = [True,False,False]
    gtt =  gtt(serv_info,selection)
    gtt.create_dataframe()




