import calendar
import datetime

import pandas as pd

from myconst import *


def get_quarter(date):
    """
    Определяет номер текущего квартала по дате
    """
    m = date.month
    return (m-1)//3+1


def get_prev_quarter(date):
    """
    Определяет номер предыдущего квартала по дате
    """
    m = date.month
    q = (m-1)//3
    if q == 0:
        q = 4
    return q


def get_quarter_from_dates(dt_list):
    """
    Переводит список дата в датафрейм со столбцами ['year', 'quarter']
    """
    return pd.DataFrame([map(lambda dt:dt.year, dt_list), map(get_quarter, dt_list)],
                        index=['year', 'quarter'], columns=dt_list).T


def get_prev_quarter_from_dates(dt_list):
    """
    Переводит список дата в датафрейм c годом и номером предыдущего квартала  
    """
    get_year = lambda dt : dt.year if get_prev_quarter(dt) != 4 else dt.year - 1
    
    return pd.DataFrame([map(get_year, dt_list), map(get_prev_quarter, dt_list)],
                        index=['year', 'quarter'], columns=dt_list).T


def get_quarter_months(n_quarter):
    """
    Возвращает индексы месяцев указанного квартала
    """
    return [(n_quarter-1)*3 + i for i in range(1, 4)]


def add_months(source_date, months):
    """
    Добавляет к дате указанное количество месяцев
    """
    month = source_date.month - 1 + months
    year = source_date.year + month // 12
    month = month % 12 + 1
    day = min(source_date.day, calendar.monthrange(year, month)[1])
    return datetime.datetime(year, month, day)


def diff_month(d1, d2):
    """
    Возвращает разницу между датами в месяцах
    """
    return (d1.year - d2.year) * 12 + d1.month - d2.month


def price_line(df, counts=False):
    """
    Собирает временной ряд из медианных значений цены [и кол-ва объявления] по датам
    """
    if df.empty:
        return None
    df.last_date = df.last_date.map(lambda x: str2dt(str(x)[:10]))
    grp = df.groupby([(df.last_date.dt.year), (df.last_date.dt.month)])
    ewm_df = grp.median()[['price_sqm_amt']]
    if counts:
        cnt_df = grp.count()[['price_sqm_amt']]
    try:
        ewm_df = ewm_df.drop((2021, 4)).ewm(span=3).mean()
        if counts:
            cnt_df = cnt_df.drop((2021, 4))
    except:
        ewm_df = ewm_df.ewm(span=3).mean()
    ewm_df.index = ewm_df.index.map(
        lambda idx: datetime.datetime(year=idx[0], month=idx[1], day=1))
    if counts:
        cnt_df.index = cnt_df.index.map(
            lambda idx: datetime.datetime(year=idx[0], month=idx[1], day=1))

        concat_df = pd.concat([ewm_df, cnt_df], axis=1)
        concat_df.columns = ['price_sqm_amt', 'counts']
        return concat_df
    else:
        return ewm_df


def str2dt(dt_str):
    return datetime.datetime.strptime(dt_str, '%Y-%m-%d')


def read_json(path_to_json):
    import json
    with open(path_to_json, "rb") as f:
        x = json.load(f)
    return x


def linear_trend(y,coef):
    import numpy as np
    from sklearn.linear_model import LinearRegression

    n = len(y)
    X = np.arange(n).reshape(-1, 1)
    LR = LinearRegression().fit(X, y)

    y_trendline = LR.predict(X)

    return y_trendline


def lowess_trend(y, frac=1.0):
    import statsmodels.api as sm

    y = y.reshape(-1)
    n = len(y)
    smoothed = sm.nonparametric.lowess(exog=range(n), endog=y, frac=frac)

    y_trendline = smoothed[:, 1]

    return y_trendline


def price_convert(data, relations, class_name, district, subdistrict=None):
    """
    Конвертирует цены объектов с параметрами, похожими на заданные, в соответствии с рассчитанными коэффициентами
    :param data: данные рынка
    :param ralations: словарь с рассчитанными коэффициентами отношений
    :param class_name: название класса
    :param district: название округа
    :param subdistrict: название района    
    """
    area_query = "building_class_name == @class_name "
    class_query = ""

    if subdistrict:
        print(f'convert price on subdistrict -- {subdistrict} -- {district} -- {class_name}')

        SUBDISTRICT_REL_DCT = relations['SUBDISTRICT_RELATIONS']

        area_convert_price = data.query(area_query+" and district == @district ").apply(\
            lambda row: row.price_sqm_amt*SUBDISTRICT_REL_DCT[row.district][row.subdistrict][subdistrict], axis=1)
        class_query = "subdistrict == @subdistrict"

    elif district:
        print(f'convert price on district -- {district} -- {class_name}')
        DISTRICT_REL_DCT = relations['DISTRICT_RELATIONS']
        area_convert_price = data.query(area_query).apply(
            lambda row: row.price_sqm_amt*DISTRICT_REL_DCT[row.district][district], axis=1)
        class_query = "district == @district"

    BUILDING_CLS_REL_DCT = relations['BUILDING_CLS_RELATIONS']
    class_convert_price = data.query(class_query).apply(
        lambda row: row.price_sqm_amt*BUILDING_CLS_REL_DCT[row.building_class_name][class_name], axis=1)

    if area_convert_price.empty and class_convert_price.empty:
        return area_convert_price

    if area_convert_price.empty:
        concated_price = class_convert_price
    elif class_convert_price.empty:
        concated_price = area_convert_price
    else:
        concated_price = pd.concat(
            [class_convert_price, area_convert_price], join='outer', axis=0)

    concated_price = concated_price[concated_price != 0].drop_duplicates()
    filtered_df = data.loc[concated_price.index]
    filtered_df['price_sqm_amt'] = concated_price
    return filtered_df
