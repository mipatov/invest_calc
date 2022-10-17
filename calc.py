import pandas as pd
import psycopg2

from dbcon import *
from models import *
from module import *
from myconst import *


class Calculator():

    def __init__(self, room_cnt=False, use_cache=False, regions_info_file='regions/regions.info') -> None:
        self.room_cnt_flag = room_cnt
        try:
            self.make_building_class_name_dict()
        except:
            print("[WARN] Something wrong. Mb cant connect to db")
        self.regions_info = read_json(regions_info_file)

        self.city = ""


    def get_geo_data(self, city_name):
        import os.path
        path_name = self.regions_info[city_name]['path_name']
        path = f'regions/{path_name}/geo_data.csv'
        if os.path.isfile(path):
            return pd.read_csv(path)
        else:
            return None

    def load_market_data(self, city_name):
        path_name = self.regions_info[city_name]['path_name']
        path = f'regions/{path_name}/market_data.csv'

        return pd.read_csv(path, index_col=0)

    
    def connect_db_etl(self):
        try:
            self.ETL_CON = psycopg2.connect(ETL_DSN)
            print('[INIT] ETL DB connection successful')
        except Exception as e:
            print(e)
            

    def connect_db_ssvd(self):
        try:
            self.SSVD_CON = psycopg2.connect(SSVD_DSN)
            print('[INFO] SSVD connection successful')
        except Exception as e:
            print(e)   

    def close_db_etl(self):
        try:
            self.ETL_CON.close()         
            print('[INFO] ETL connection closed')
        except Exception as e:
            print(e) 

    def close_db_ssvd(self):
        try:
            self.SSVD_CON.close()         
            print('[INFO] SSVD connection closed')
        except Exception as e:
            print(e)   

    def get_db_connections(self):
        try:
            self.ETL_CON = psycopg2.connect(ETL_DSN)
            print('[INIT] ETL DB connection successful')
        except Exception as e:
            print(e)

        try:
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
        except:
            self.get_db_connections()

    def get_polygons(self, city_name):
        from geopandas import GeoDataFrame
        from shapely import wkt

#         sel_sql = POLYGONS_SQL
#         df_polygons = pd.read_sql_query(sel_sql,self.ETL_CON)
        df_polygons = self.get_geo_data(city_name)

        if df_polygons is None:
            return None

        df_polygons['geometry'] = df_polygons['wkt'].apply(wkt.loads)
        gdf_polygons = GeoDataFrame(df_polygons, crs='epsg:4326')

        return gdf_polygons

    def load_data(self):
        self.connect_db_etl()
        data = pd.read_sql_query(CIAN_LOAD_SQL, self.SSVD_CON)
        self.close_db_etl()
        return data

    def preprocess_data(self, data):
        from geopandas import GeoDataFrame, points_from_xy, sjoin
        from shapely import wkt

        data.dropna(subset=['price_sqm_amt', 'room_cnt'], inplace=True)

        geo_data = GeoDataFrame(data, crs='epsg:4326',
                                geometry=points_from_xy(data.realty_longitude,
                                                        data.realty_latitude))
        gdf_polygons = self.get_polygons()
        gdf_quarters = sjoin(geo_data, gdf_polygons)

        key_fields_list = OBJ_KEY_FIELDS.copy()
        if self.room_cnt_flag:
            key_fields_list.append('room_cnt')

        pivot_quarters = gdf_quarters.pivot_table(['price_sqm_amt'],
                                                  key_fields_list,
                                                  aggfunc=['median', 'count'])
        pivot_quarters = pivot_quarters.reset_index(drop=False)

        key_fields_list += ['median_price_sqm_amt', 'count_price_sqm_amt']

        pivot_quarters.columns = key_fields_list

        return pivot_quarters

    def make_building_class_name_dict(self):
        self.connect_db_ssvd()
        self.code2cls_name = pd.read_sql_query(DCT_BUILDING_CLS_SQL, self.SSVD_CON).set_index(
            'building_class_type').to_dict()['building_class_name']
        self.cls_name2code = {v: k for k, v in self.code2cls_name.items()}
        self.close_db_ssvd()


    def get_obj_info(self, obj_id):
        from geopandas import GeoDataFrame, points_from_xy, sjoin

        self.connect_db_etl()
        obj_info_query = OBJ_INFO_SQL.format(obj_id)
        df_obj = pd.read_sql_query(obj_info_query, self.ETL_CON)

        self.close_db_etl()

        if df_obj.empty:

            return pd.DataFrame()
        df_obj['building_class_type'] = df_obj['building_class_type'].replace(
            DICT_CSL_CODE_EISGS2CIAN)
        df_obj['building_class_name'] = df_obj['building_class_type'].replace(
            self.code2cls_name)

        city_name = df_obj['region_name'].values[0]

        geo_data = GeoDataFrame(df_obj, crs='epsg:4326',
                                geometry=points_from_xy(df_obj.realty_longitude,
                                                        df_obj.realty_latitude))
        gdf_polygons = self.get_polygons(city_name)

        if gdf_polygons is None:
            df_obj['district'] = None
            df_obj['subdistrict'] = None

            return df_obj.loc[0, OBJ_INFO_FIELDS]

        gdf_quarters = sjoin(geo_data, gdf_polygons)
        gdf_quarters = gdf_quarters[OBJ_INFO_FIELDS]

        return gdf_quarters.iloc[0]

    
    def find_housing_complex_cian(self, city_name, search_name):
        from fuzzywuzzy import process

        if self.city != city_name:
            self.city = city_name
            self.market_data = self.load_market_data(city_name)

        hc_df = self.market_data.query('advert_category_code == 3')[
            ['housing_complex_name', 'commissioning_date']].drop_duplicates()

        results = process.extract(search_name, hc_df.housing_complex_name)

        return hc_df.loc[[r[-1] for r in results]]

    
    def get_housing_complex_price_dynamics(self, city_name, hc_name, commissioning_date):

        if self.city != city_name:
            self.city = city_name
            self.market_data = self.load_market_data(city_name)

        return price_line(self.market_data.query('advert_category_code == 3 and housing_complex_name == @hc_name and commissioning_date == @commissioning_date'), counts=True)

    
    def get_market_data(self, city_name, ao_name=None, raion_name=None, cls_name=None,
                        transport_accessibility=None,  exclude_hc_name=None, exclude_hc_comiss_dt=None,
                        today_date=None, threshold=6):
        today_date = today_date if today_date else datetime.datetime.today().strftime("%Y-%m-%d")

        if self.city != city_name:
            self.city = city_name
            self.market_data = self.load_market_data(city_name)

        qq = "advert_category_code == 3 \
            and commissioning_date <= @today_date\
            and last_date > '2021-04-03'\
            and last_date < '2022-08-01'"

        if ao_name and 'district' in self.market_data.columns:
            qq += " and district == @ao_name "

        if raion_name and 'subdistrict' in self.market_data.columns:
            qq += " and subdistrict == @raion_name "

        if cls_name:
            qq += " and building_class_name ==  @cls_name "

        if exclude_hc_name and exclude_hc_comiss_dt and exclude_hc_comiss_dt < datetime.datetime.today():
            qq += " and housing_complex_name !=  @exclude_hc_name "
            qq += " and housing_complex_name !=  @exclude_hc_comiss_dt "

        market_data = self.market_data.query(qq)
        price_line_df = price_line(market_data)

        if price_line_df is not None and not price_line_df.empty:
            idx = price_line_df.index
            if diff_month(idx[-1], idx[0]) > len(idx):
                new_idx = [add_months(idx[0], i)
                           for i in range(diff_month(idx[-1], idx[0])+1)]
                price_line_df = price_line_df.reindex(new_idx).interpolate()

        if price_line_df is None or len(price_line_df) < threshold:
            print('[WARN] No such building class in this area. Calculate market from other classes and areas of same level')

            path_name = self.regions_info[city_name]['path_name']
            path = f'regions/{path_name}/relations.json'
            relations_dict = read_json(path)

            market_data = price_convert(
                self.market_data, relations_dict, class_name=cls_name, district=ao_name, subdistrict=raion_name)

            price_line_df = price_line(market_data)

            if price_line_df is not None and not price_line_df.empty:
                idx = price_line_df.index
                if diff_month(idx[-1], idx[0]) > len(idx):
                    new_idx = [add_months(idx[0], i)
                               for i in range(diff_month(idx[-1], idx[0])+1)]
                    price_line_df = price_line_df.reindex(
                        new_idx).interpolate()

        if price_line_df is None or len(price_line_df) < threshold:
            print('[WARN] No data in this place! Get data from next level area.')
            market_data = price_convert(
                self.market_data, relations_dict, class_name=cls_name, district=ao_name)
            price_line_df = price_line(market_data)

            if price_line_df is not None and not price_line_df.empty:
                idx = price_line_df.index
                if diff_month(idx[-1], idx[0]) > len(idx):
                    new_idx = [add_months(idx[0], i)
                               for i in range(diff_month(idx[-1], idx[0])+1)]
                    price_line_df = price_line_df.reindex(
                        new_idx).interpolate()

        return price_line_df

    def get_current_price(self, obj_id):

        price_df = self.get_price_dynamics(obj_id).iloc[[0]]

        return price_df

    def get_price_dynamics(self, obj_id):
        self.connect_db_etl()

        query = OBJ_PRICE_DYNAMICS_SQL.format(obj_id)
        price_dynamics_df = pd.read_sql_query(query, self.ETL_CON)
        self.close_db_etl()
        
        return price_dynamics_df


    def _make_forecast(self, market_data: pd.DataFrame, forecast_period,):
        path_name = self.regions_info[self.city]['path_name']
        folder_path = f'regions/{path_name}/'
        macro = MacroMLModel(market_data, forecast_period,
                             models_folder_path=folder_path)

        return macro.market_forecast_df

    def make_forecast_custom(self, current_price, commiss_dt, period=12, city_name=None, ao_name=None, raion_name=None, class_name=None, hc_name=None, indexes = {}):
        if city_name is None:
            print('[ERR] city is None!')
            return None

        # print(indexes)

        

        threshold = 6
        market_data = self.get_market_data(
            city_name, ao_name, raion_name, class_name, exclude_hc_name=hc_name, exclude_hc_comiss_dt=commiss_dt, threshold=threshold,
            today_date='2022-08-01')

        # пока выбрасываем август
        market_data = market_data.loc[:'2022-07-01']

        trend_market_data = market_data.copy()
        trend_market_data.price_sqm_amt = lowess_trend(
            market_data.price_sqm_amt.values)
        trend_market_data = trend_market_data.rename(
            columns={'price_sqm_amt': 'price_sqm_trend'})

        if market_data is None or len(market_data) < threshold:
            print(f'Found {len(market_data)} months')
            return pd.DataFrame(columns=['price_sqm_amt', 'price_sqm_forecast', 'price_sqm_obj_forecast'])

        before_commiss_period = diff_month(commiss_dt, market_data.index[-1])+1
        forecast_period = max(period, before_commiss_period)

        object_history = pd.DataFrame(
            columns=['price_sqm_obj_history'], index=market_data.index)
        object_advantage = 1

        if type(current_price) is pd.Series:
            print(current_price)
            print(current_price.reindex(market_data.index).to_list())
            object_history['price_sqm_obj_history'] = current_price.reindex(
                market_data.index).to_list()
            current_price = current_price.iloc[-1]
            k = (object_history['price_sqm_obj_history'].iloc[-threshold:] /
                 market_data["price_sqm_amt"]).median()
            if k > 1:
                object_advantage *= k

        forecast_data = self._make_forecast(trend_market_data, forecast_period)

        indexes_coef = 1
        if len(indexes)>0:
            path_name = self.regions_info[city_name]['path_name']
            path = f'regions/{path_name}/relations.json'
            relations_dict = read_json(path)
            infrastructure_relations = relations_dict['INFRASTRUCTURE_INDEX_RELATION']
            this_district_infrastructure = str(int(relations_dict['DISTRICT_MEDIAN_INFRASTRUCTURE_INDEX'][ao_name]))
            this_obj_infrastructure = str(int(indexes['infrastructure_index']))
            indexes_coef = infrastructure_relations[this_district_infrastructure][this_obj_infrastructure]

            print('this_obj_infrastructure',this_obj_infrastructure)
            print('this_district_infrastructure',this_district_infrastructure)
            print('indexes_coef',indexes_coef)
            
        object_advantage*=indexes_coef

        concat_market_and_forecast_df = pd.concat(
            [market_data, trend_market_data, forecast_data], axis=1)
        concat_market_and_forecast_df = concat_market_and_forecast_df[~concat_market_and_forecast_df.index.duplicated(
            keep='first')]

        concat_market_and_forecast_df.loc[commiss_dt:, 'price_sqm_obj_forecast'] = \
            concat_market_and_forecast_df.loc[commiss_dt:,'price_sqm_forecast']*object_advantage

        before_commiss_forecast_index = concat_market_and_forecast_df.loc[
            market_data.index[-1]:commiss_dt].index

        if before_commiss_period > 0:
            # print('current_price',current_price,'object_advantage',object_advantage)
            delta = trend_market_data.iloc[-1, 0]*object_advantage - current_price
            delta_series = pd.Series(index=before_commiss_forecast_index)
            delta_series[0] = delta
            delta_series[-1] = 0
            delta_series = delta_series.interpolate()

            concat_market_and_forecast_df.loc[before_commiss_forecast_index, 'price_sqm_obj_forecast'] = \
                (concat_market_and_forecast_df.loc[before_commiss_forecast_index,
                 'price_sqm_forecast']*object_advantage-delta_series)

        concat_market_and_forecast_df.loc[market_data.index,
                                          'price_sqm_forecast'] = None
        concat_market_and_forecast_df.loc[market_data.index[-1],
                                          'price_sqm_forecast'] = trend_market_data.iloc[-1, 0]

        concat_market_and_forecast_df = concat_market_and_forecast_df.iloc[:len(market_data)+period]

        if not object_history.isnull().values.all():
            object_history = object_history.reindex(market_data.index).interpolate()
            concat_market_and_forecast_df.loc[object_history.index,'price_sqm_obj_history'] = object_history

        return concat_market_and_forecast_df

    def validate_market(self, city_name=None, ao_name=None, raion_name=None, class_name=None, hc_name=None, validate_period=6):
        if city_name is None:
            print('[ERR] city is None!')
            return None

        validate_period = 6
        prev_today_date = add_months(
            datetime.datetime.today(), -validate_period)
        

        print('city : ', city_name, ' ao : ', ao_name,
              ' raion : ', raion_name, ' class : ', class_name)
        market_data = self.get_market_data(
            city_name, ao_name, raion_name, class_name, today_date=prev_today_date.strftime("%Y-%m-%d"))

        # пока выбрасываем август
        market_data = market_data.loc[:'2022-07-01']

        trend_market_data = market_data.copy()
        trend_market_data.price_sqm_amt = lowess_trend(
            market_data.price_sqm_amt.values)
        trend_market_data = trend_market_data.rename(
            columns={'price_sqm_amt': 'price_sqm_trend'})


        prev_market_data = trend_market_data[:-validate_period]

        threshold = 6

        if len(prev_market_data) < threshold:
            print(
                f'Found only {len(prev_market_data)} months of prev_market_data ')
            return pd.DataFrame(columns=['price_sqm_amt', 'price_sqm_forecast'])

        forecast_data = self._make_forecast(prev_market_data, validate_period)

        concat_market_and_forecast_df = pd.concat(
            [market_data,trend_market_data, forecast_data], axis=1).iloc[:len(trend_market_data)]

        return concat_market_and_forecast_df
