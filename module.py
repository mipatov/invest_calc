from myconst import *
import pandas as pd
import datetime
import calendar


def get_quarter(date):
    m = date.month
    return (m-1)//3+1

def get_prev_quarter(date):
    m = date.month
    q = (m-1)//3
    if q == 0:
        q = 4 
    return q

def get_quarter_from_dates(dt_list):
    return pd.DataFrame([map(lambda dt:dt.year,dt_list),map(get_quarter,dt_list)],
                        index = ['year','quarter'],columns = dt_list).T

def get_prev_quarter_from_dates(dt_list):
    get_year = lambda dt:dt.year if get_prev_quarter(dt)!= 4 else dt.year - 1 
    return pd.DataFrame([map(get_year,dt_list),map(get_prev_quarter,dt_list)],
                        index = ['year','quarter'],columns = dt_list).T

def get_quarter_months(n_quarter):
    return [(n_quarter-1)*3 + i for i in range(1,4)]

def add_months(sourcedate, months):
    month = sourcedate.month - 1 + months
    year = sourcedate.year + month // 12
    month = month % 12 + 1
    day = min(sourcedate.day, calendar.monthrange(year,month)[1])
    return datetime.datetime(year, month, day)

def diff_month(d1, d2):
    return (d1.year - d2.year) * 12 + d1.month - d2.month

def weighted_avg(values, weights):
    return (values * weights).sum() / weights.sum()

def percentage_per_annum(start_price,final_price,months_period):
    return (final_price-start_price)/start_price/months_period*12*100

def get_polygons():
    from shapely import wkt
    from geopandas import GeoDataFrame

    sel_sql = POLYGONS_SQL

#     df_polygons = pd.read_sql_query(sel_sql,ETL_CON)
#     df_polygons.to_csv('df_polygons.csv')
    df_polygons = pd.read_csv('mo.csv')

    df_polygons['geometry'] = df_polygons['WKT'].apply(wkt.loads)
    gdf_polygons = GeoDataFrame(df_polygons, crs = 'epsg:4326')

    return gdf_polygons

def get_geodf(df,cords_col_name= ['realty_longitude','realty_latitude']):
    from shapely import wkt
    from geopandas import GeoDataFrame, points_from_xy, sjoin
    
    geo_data = GeoDataFrame(df, crs = 'epsg:4326',
                                 geometry=points_from_xy(df[cords_col_name[0]], 
                                                            df[cords_col_name[1]]))
    gdf_polygons = get_polygons()

    gdf_quarters = sjoin(geo_data, gdf_polygons)
    return gdf_quarters

def aggregate_geodf(df,idx_columns,cords_col_name= ['realty_longitude','realty_latitude'], count_column = None):
    gdf_quarters = get_geodf(df,cords_col_name)

    
    pivot_cols = ['price_sqm_amt']
    aggfunc =  ['median', 'count']
    if count_column:
        pivot_cols +=[count_column]
        aggfunc=  ['median', 'sum']
        
    pivot_quarters = gdf_quarters.pivot_table( pivot_cols, idx_columns, 
                                        aggfunc = aggfunc)
    if count_column:
        pivot_quarters = pivot_quarters.iloc[:,[1,2]]
    
    pivot_quarters.columns = ['median_price_sqm_amt','count_adverts']
    
    return pivot_quarters

def aggregate_by_district(df,cords_col_name= ['realty_longitude','realty_latitude'], count_column = None):
    idx_columns =['region_name', 'ABBREV_AO','NAME', 'OKTMO', 'building_class_type', 'building_class_name']
    return aggregate_geodf(df,idx_columns=idx_columns,cords_col_name= ['realty_longitude','realty_latitude'], count_column = None)

def aggregate_by_ao(df,cords_col_name= ['realty_longitude','realty_latitude'], count_column = None):
    idx_columns =['region_name', 'ABBREV_AO', 'building_class_type', 'building_class_name']
    return aggregate_geodf(df,idx_columns=idx_columns,cords_col_name= ['realty_longitude','realty_latitude'], count_column = None)

def get_gap_df(market_df,prmary_df,good_only = False):
    gap_df = market_df.join(prmary_df,how = 'inner',lsuffix = '_second',rsuffix='_prime')

    gap_df['gap'] = gap_df.median_price_sqm_amt_second/gap_df.median_price_sqm_amt_prime
    gap_df['good'] =  (gap_df.count_adverts_second>10) & (gap_df.count_adverts_prime>10)
    
    if good_only : 
        return gap_df[['gap']][gap_df.good]
    else :
        return gap_df[['gap','good']]


def price_line(df):
    df.last_date = df.last_date.map(lambda x : str2dt(x[:10]))
    ewm_df = df.groupby([(df.last_date.dt.year), (df.last_date.dt.month)]).median()[['price_sqm_amt']]
    try :
        ewm_df = ewm_df.drop((2021,4)).ewm(span=3).mean()
    except:
        ewm_df = ewm_df.ewm(span=3).mean()
    ewm_df.index = ewm_df.index.map(lambda idx: datetime.datetime(year= idx[0],month = idx[1],day = 1))
    return ewm_df

def str2dt(dt_str):
    return datetime.datetime.strptime(dt_str,'%Y-%m-%d')


def read_json(path_to_json):
    import json
    with open(path_to_json, "rb") as f:
        x = json.load(f)
    return x 