import psycopg2
import pandas as pd
from myconst import *
from dbcon import * 
from models import *
from module import *

class Calculator():

    def __init__(self, room_cnt = False,use_cache = False, regions_info_file  = 'regions/regions.info') -> None:
        self.room_cnt_flag = room_cnt
        self.get_db_connections()
        self.make_building_class_name_dict()
        self.regions_info = read_json(regions_info_file)

        self.city = ""
        # cache_path = 'cache/db_data_cached.csv'
        # cache_path = 'cache/historical_cian_last_advert_only_geo.csv'

        # if room_cnt:
        #     cache_path = 'cache/db_data_rooms_cached.csv'
        
        # if use_cache:
        #     self.db_data = pd.read_csv(cache_path,index_col = 0)
        #     print('[INIT] Load cached data')
        # else : 
        #     self.db_data = self.load_data()
        #     print('[INIT] Data loaded successfuly')
        #     self.db_data = self.preprocess_data(self.db_data)
        #     print('[INIT] Data preprocessed successfuly')
        #     self.db_data.to_csv(cache_path)
            
    def get_geo_data(self,city_name):
        import os.path
        path_name = self.regions_info[city_name]['path_name']
        path = f'regions/{path_name}/geo_data.csv'
        if os.path.isfile(path):
            return  pd.read_csv(path)
        else :
            return None

    def load_market_data(self,city_name):
        path_name = self.regions_info[city_name]['path_name']
        path = f'regions/{path_name}/market_data.csv'

        return  pd.read_csv(path)


    def get_db_connections(self): 
        try  : 
            self.ETL_CON = psycopg2.connect(ETL_DSN)
            print('[INIT] ETL DB connection successful')
        except Exception as e:
            print(e)

        try  : 
            self.SSVD_CON = psycopg2.connect(SSVD_DSN)
            print('[INIT] SSVD connection successful')
        except Exception as e:
            print(e)


    def check_connections(self):
        try:
            cur = self.ETL_CON.cursor()
            cur.execute('SELECT 1')
            cur = self.SSVD_CON.cursor()
            cur.execute('SELECT 1')
        except :
            self.get_db_connections()
        

    def get_polygons(self,city_name):
        from shapely import wkt
        from geopandas import GeoDataFrame


#         sel_sql = POLYGONS_SQL

#         df_polygons = pd.read_sql_query(sel_sql,self.ETL_CON)
        df_polygons = self.get_geo_data(city_name)
        
        if df_polygons is None :
            return None

        df_polygons['geometry'] = df_polygons['wkt'].apply(wkt.loads)
        gdf_polygons = GeoDataFrame(df_polygons, crs = 'epsg:4326')

        return gdf_polygons

    
    def load_data(self):
        self.check_connections()
        data = pd.read_sql_query(CIAN_LOAD_SQL,self.SSVD_CON)
        
        return data

    
    def preprocess_data(self, data):
        from shapely import wkt
        from geopandas import GeoDataFrame, points_from_xy, sjoin

        data.dropna(subset=['price_sqm_amt','room_cnt'], inplace=True)


        geo_data = GeoDataFrame(data, crs = 'epsg:4326',
                                        geometry=points_from_xy(data.realty_longitude, 
                                                                    data.realty_latitude))
        gdf_polygons = self.get_polygons()
        gdf_quarters = sjoin(geo_data, gdf_polygons)
        
        key_fields_list = OBJ_KEY_FIELDS.copy()
        if self.room_cnt_flag :
            key_fields_list.append('room_cnt')
        
        pivot_quarters = gdf_quarters.pivot_table(['price_sqm_amt'], 
                                            key_fields_list, 
                                            aggfunc = ['median', 'count'])   
        pivot_quarters = pivot_quarters.reset_index(drop = False)
        
        key_fields_list += ['median_price_sqm_amt','count_price_sqm_amt']
        
        pivot_quarters.columns = key_fields_list
        
        return pivot_quarters
    
    def make_building_class_name_dict(self):
        self.check_connections()
        self.code2cls_name = pd.read_sql_query(DCT_BUILDING_CLS_SQL,self.SSVD_CON).set_index('building_class_type').to_dict()['building_class_name']
        self.cls_name2code = {v:k for k,v in self.code2cls_name.items()}

    def get_obj_info(self, obj_id):
        from geopandas import GeoDataFrame, points_from_xy, sjoin
        
        self.check_connections()
        obj_info_query = OBJ_INFO_SQL.format(obj_id)
        df_obj = pd.read_sql_query(obj_info_query,self.ETL_CON)
     
        if df_obj.empty:
            
            return pd.DataFrame()
        df_obj['building_class_type'] = df_obj['building_class_type'].replace(DICT_CSL_CODE_EISGS2CIAN)
        df_obj['building_class_name'] = df_obj['building_class_type'].replace(self.code2cls_name)

    
        city_name = df_obj['region_name'].values[0]
        
        geo_data = GeoDataFrame(df_obj, crs = 'epsg:4326',
                                geometry=points_from_xy(df_obj.realty_longitude, 
                                                            df_obj.realty_latitude))
        gdf_polygons = self.get_polygons(city_name)

        if gdf_polygons is None : 
            df_obj['district'] = None
            df_obj['subdistrict'] = None

            return df_obj.loc[0,OBJ_INFO_FIELDS]

        gdf_quarters = sjoin(geo_data, gdf_polygons)
        gdf_quarters = gdf_quarters[OBJ_INFO_FIELDS]
        
        return gdf_quarters.iloc[0]
    

    def get_market_price_old(self,building_class_cd,district_name,room_cnt = -1):
       
        assert not self.room_cnt_flag or ( self.room_cnt_flag and room_cnt > 0) ,f'[WARN] Expected room_cnt value > 0, got {room_cnt}'
            
        if self.room_cnt_flag and room_cnt > 0 :
            match_df = self.db_data.loc[(self.db_data['building_class_type'] == building_class_cd)
                                        & (self.db_data['NAME'].str.contains(district_name))
                                        & (self.db_data['room_cnt']  == room_cnt)]
        if not self.room_cnt_flag or match_df.shape[0] ==0 :
            if room_cnt > 0:
                print('[WARN] Ignoring the entered room_cnt value!')

            match_df = self.db_data.loc[(self.db_data['building_class_type'] == building_class_cd)
                                        & (self.db_data['NAME'].str.contains(district_name))]
        if match_df.shape[0] ==0 :
            print('[WARN] Ignoring the building_class_type value!')
            match_df = self.db_data.loc[self.db_data['NAME'].str.contains(district_name)]
        if match_df.shape[0] ==0 :
            print('[WARN] Nothing found !')
        
        return match_df

    def get_market_data(self,city_name,ao_name=None,raion_name=None,cls_name=None, transport_accessibility = None,    ):
        today_date = datetime.datetime.today().strftime("%Y-%m-%d")
        # print(today_date,ao_name,raion_name,cls_name)
        
        if self.city != city_name:
            self.city = city_name
            self.market_data = self.load_market_data(city_name)
            # print(self.market_data.columns)

        # if data_filter:
        qq = "advert_category_code == 3 \
            and commissioning_date <= @today_date"
        # if data_filter['city']
        if ao_name and 'district' in self.market_data.columns:
            qq+= " and district == @ao_name "

        if raion_name  and 'subdistrict' in self.market_data.columns :
            qq+= " and subdistrict == @raion_name "

        if cls_name:
            qq+= " and building_class_name ==  @cls_name "
        # else :
        #     qq = "advert_category_code == 3 \
        #         and commissioning_date <= @today_date\
        #         and building_class_name ==  @cls_name\
        #         and ABBREV_AO == @ao_name"
        
        market_data = self.market_data.query(qq)
        print(qq)
        # print('empty data -- ',market_data.empty)
        if market_data.empty:
            return market_data
        
        return price_line(market_data)
    
    def get_price_of_similar_objects(self,obj_id,room_cnt = -1):
        
        obj_info = self.get_obj_info(obj_id)

        return self.get_market_price_old(obj_info['building_class_type'],obj_info['NAME'],room_cnt)

    
    def get_current_price(self, obj_id):
        
        price_df =  self.get_price_dynamics(obj_id).iloc[[0]]
        
        return price_df
    

    def get_price_dynamics(self, obj_id):
        self.check_connections()
        
        query = OBJ_PRICE_DYNAMICS_SQL.format(obj_id)
        price_dynamics_df =  pd.read_sql_query(query,self.ETL_CON)
        
        return price_dynamics_df
    

    def get_commissioning_date(self,obj_id):
        self.check_connections()
        obj_commissioning_date_sql = COMMISSIONING_DATE_SQL.format(obj_id)
        commissioning_date =  pd.read_sql_query(obj_commissioning_date_sql,self.ETL_CON)
        
        return commissioning_date.iloc[0][0]
    
    
    def get_comissionin_effect(self,building_class_cd,district_name):
        self.check_connections()
        df_secondary_market_raw =  self.db_data.query('advert_category_code == 1')
        df_cian_primary_market_raw =  self.db_data.query('advert_category_code == 3')
        df_eisgs_primary_market_raw =  pd.read_sql_query(EISGS_MARKET_DATA_SQL,self.ETL_CON)
    
    
    def make_forecast_by_obj_id(self,obj_id,forecast_period,price = -1):
        market_df =  self.get_price_of_similar_objects(obj_id)
        actual_date = None
        if price>0 :
            current_price = price 
        else :
            price_df = self.get_current_price(obj_id)
            current_price = price_df['avg_price_sqm'][0]
            actual_date = price_df['report_month_dt'][0]
            print(f'[NOTE] Actual price for {actual_date} is {current_price}')
        commis_date = self.get_commissioning_date(obj_id)
        
        return self._make_forecast(market_df,current_price,commis_date,forecast_period,actual_date=actual_date)
    
    
    def make_forecast_for_custom_obj_old(self, building_class_cd,district_name,price,commis_date,forecast_period,room_cnt = -1,actual_date = None):
        
        market_df = self.get_market_price_old(building_class_cd,district_name,room_cnt)
        return self._make_forecast(market_df,price,commis_date,forecast_period,room_cnt,actual_date)
    
    
    def _make_forecast(self,market_data: pd.DataFrame,price,commis_date,forecast_period,room_cnt = -1,actual_date = None ):
        from datetime import date,timedelta
        self.check_connections()
       
        assert market_data.shape[0]==1, f'[WARN] Unable to match such objects! Got {market_data.shape[0]} lines in market data, expected 1.'
        
        
        market_price = market_data['median_price_sqm_amt'].iloc[0]

        actual_date = actual_date if actual_date else date.today().replace(day = 1) - timedelta(days=1)

        current_price = price 
        
        if current_price > market_price:
            print(f'current price =\t{current_price}\nmarket price =\t{market_price}\nNot stonks!')
            return 0
        
        macro = DummyMacroModel(start_market_price = market_price,grow_rate = 1.013)
        commis_period = (commis_date - actual_date).days//30

        
        commis_effect = float(pd.read_sql_query(COMMISSIONING_EFFECT_SQL,self.ETL_CON)['comissioning_effect'][0])

        micro = CommisEffectLinearMicroModel(current_price,commis_effect,commis_period,macro)
#         micro = MacroLikeMicroModel(current_price,commis_effect,commis_period,market_entry_period = 6,macro = macro)
        
        print('Stonks!')
        
        stonks_data = pd.DataFrame(index = range(forecast_period))
        stonks_data['date'] = [add_months(actual_date,n)  for n in range(forecast_period)]
        stonks_data['market'] = [macro(n)  for n in range(forecast_period)]
        stonks_data['price'] = [micro(n)  for n in range(forecast_period)]
        
        annum_percent = percentage_per_annum(stonks_data['price'].iloc[0],stonks_data['price'].iloc[-1],forecast_period)
        
        print(f'You got {annum_percent:.2f}% per annum')
        
        return stonks_data 

    def _make_forecast_new(self,market_data:pd.DataFrame,current_price,forecast_period,):
        self.check_connections()
        path_name = self.regions_info[self.city]['path_name']
        folder_path = f'regions/{path_name}/'
        macro = MacroMLModel(market_data,forecast_period,models_folder_path= folder_path)
        
        return macro.market_forecast_df
        

    def make_forecast_custom(self,current_price,commiss_dt,period=12,city_name = None,ao_name=None,raion_name=None,class_name=None):
        if city_name is None:
            print('[ERR] city is None!')
            return None

        city_name = None if city_name == '- Не выбран -' else city_name
        names_dict = {
            'city_name':city_name
            ,'ao_name':ao_name
            ,'raion_name':raion_name
            ,'class_name':class_name
        }
        for key in names_dict:
            if names_dict[key] == '- Не выбран -':
                names_dict[key] = None

        market_data = self.get_market_data(city_name,ao_name,raion_name,class_name)
        
        # пока выбрасываем август
        market_data = market_data[:-1]


        if len(market_data)>0:
            diff = diff_month(market_data.index[-1],market_data.index[0])
            new_idx = [add_months(market_data.index[0],i) for i in range(diff+1)]
            market_data = market_data.reindex(new_idx).interpolate()
       

        if len(market_data)<6:
            print(f'Found {len(market_data)} months')
            return pd.DataFrame(columns=['price_sqm_amt','price_sqm_forecast','price_sqm_obj_forecast'])

        # print(market_data)
        # commiss_dt = commiss_dt.replace(day = 1)
        before_commiss_period = diff_month(commiss_dt, market_data.index[-1])+1
        forecast_period = max(period,before_commiss_period) 

        object_history = pd.DataFrame(columns=['price_sqm_obj_history'],index= market_data.index)
        object_advantage = 1 
        print(current_price)
        if type(current_price) is list :
            object_history.iloc[-len(current_price):,0] =  current_price[-len(object_history):]
            current_price =  current_price[-1]
            k = (object_history['price_sqm_obj_history']/market_data["price_sqm_amt"]).median()
            if k>1:
                object_advantage*=k

        forecast_data =  self._make_forecast_new(market_data,current_price,forecast_period)

        
        concat_market_and_forecast_df = pd.concat([market_data,forecast_data])
        concat_market_and_forecast_df = concat_market_and_forecast_df[~concat_market_and_forecast_df.index.duplicated(keep='first')]
        concat_market_and_forecast_df.loc[market_data.index[-1],'price_sqm_forecast'] =  market_data.iloc[-1,0]

        
        concat_market_and_forecast_df.loc[market_data.index[-1],'price_sqm_obj_forecast'] =  current_price

        before_commiss_forecast_index = concat_market_and_forecast_df.loc[market_data.index[-1]:commiss_dt].index

        delta =  market_data.iloc[-1,0]*object_advantage - current_price
        delta_series = pd.Series(index= before_commiss_forecast_index)
        delta_series[0] =  delta
        delta_series[-1] =  0
        delta_series =  delta_series.interpolate()

        concat_market_and_forecast_df.loc[commiss_dt:,'price_sqm_obj_forecast'] = \
                        concat_market_and_forecast_df.loc[commiss_dt:,'price_sqm_forecast']*object_advantage
        concat_market_and_forecast_df.loc[before_commiss_forecast_index,'price_sqm_obj_forecast'] = \
                         (concat_market_and_forecast_df.loc[before_commiss_forecast_index,'price_sqm_forecast']-delta_series)*object_advantage
        concat_market_and_forecast_df = concat_market_and_forecast_df.iloc[:len(market_data)+period]
        
    
        if not object_history.isnull().values.all():
            concat_market_and_forecast_df.loc[object_history.index,'price_sqm_obj_history'] =  object_history
    
    
        return concat_market_and_forecast_df