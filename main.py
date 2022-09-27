from datetime import datetime
from flask import Flask, render_template, url_for,request, send_file
from io import StringIO
import psycopg2
from dbcon import * 
from module import percentage_per_annum
from calc import Calculator
import matplotlib
import matplotlib.pyplot as plt
from io import BytesIO
import base64
import seaborn as sns
import pandas as pd
import re
import warnings
warnings.filterwarnings('ignore')

matplotlib.use('Agg')
sns.set(rc={'figure.figsize':(10,7.5)})
sns.set_theme()


app = Flask(__name__)
calc  = Calculator(use_cache=True)

def get_db_connections(self, base): 
    if base == "ETL":
        try  : 
            con = psycopg2.connect(ETL_DSN)
            # print('[INIT] ETL DB connection successful')
            return con
        except Exception as e:
            print(e)
            return None

    if base == "SSVD":
        try  : 
            con = psycopg2.connect(SSVD_DSN)
            # print('[INIT] SSVD connection successful')
            return con
        except Exception as e:
            print(e)
            return None

    print('Wrong base-param! Excepted one of ["ELT","SSVD"]')



def get_city_list():
    dct = read_regions_json()
    return sorted(list(dct.keys()))

def get_okrug_list(city):
    dct = read_regions_json()
    return sorted(list(dct[city].keys()))

def get_raion_list(city,okrug):
    dct  =read_regions_json()
    print(city,okrug)
    if len(dct[city].keys())>0:
        if okrug and okrug.strip() != '- Не выбран -'.strip() :
            return sorted(list(dct[city][okrug]))
        else: 
            raion_list = []
            for k,v in dct[city].items():
                raion_list.extend(v)
            return raion_list
    return []

def read_regions_json():
    import json 
    with open("static/json/regions.json", "rb") as f:
        regions_dct = json.load(f)
    return regions_dct


display_pagams = {
    'obj_info_visibility':'d-none'
    ,'param_settings_visibility':'d-none'
    ,'alert_visibility':'d-none'
    ,'plot_visibility':'d-none'
    ,'high_price_alert_visibility':'d-none'
}
plot_param = {
    "plot_url":""
    ,"percent":""
    ,"out_df":""
}
object_params = {
    "city" : ""
    ,"okrug" : ""
    ,"raion" : ""
    ,"class_name" :""
    ,"commiss_dt" : ""
    ,"current_price" : ""
    ,"forecast_period":""
}

filter_checkboxes = {
    'city': True
    ,'okrug': True
    ,'raion': False
    ,'class': True
    ,'history':True
}

def get_object_params():
    return object_params

def update_object_params(dct):
    return object_params.update(dct)

def set_object_param(key,value):
    object_params[key] = value

list_data_params = {
    'city_list': get_city_list()
    ,'okrug_list': get_okrug_list("Москва")
    ,'raion_list':  get_raion_list("Москва",None)
    # ,'regions_dict': read_regions_json()
    ,'cls_name_list':['эконом','комфорт','бизнес','премиум']
}

obj_info_params = {}

def toggle_block(key,active):
    display_pagams[key] = 'd-block' if active else 'd-none' 

def toggle_all_blocks(active):
    for key in display_pagams:
        toggle_block(key, active)

def clear_object_params():
    for key in object_params:
        object_params[key] = ""
    for key in obj_info_params:
        obj_info_params[key] = ""


@app.route('/', methods=['GET','POST'])
def index():
    clear_object_params()
    return render_template('index.html')

@app.route('/info', methods=['GET','POST'])
def info():
    toggle_all_blocks(False)
    if request.method == 'POST':
        obj_id = request.form['input_id']
        if obj_id.isdigit():
            obj_info = calc.get_obj_info(obj_id)
            if obj_info.shape[0] == 0:
                print('nothing found')
                return show_alert_info()

            price_dynamics = calc.get_price_dynamics(obj_id)
            
            obj_info_dct = {
                "adress" :  obj_info['adress']
                ,"housing_complex" :  obj_info['housing_complex']
                ,"city" : obj_info['region_name']
                ,"okrug" : obj_info['district']
                ,"raion" : obj_info['subdistrict']
                ,"class_name" : obj_info['building_class_name']
                ,"commiss_dt" : obj_info['obj_comiss_dt']
                ,'current_price': "--"
                ,'contracts_cnt': "--"
                ,'last_report_dt': "--"
                ,'price_dynamics' : price_dynamics
            }
            
            if not price_dynamics.empty :
                last_price = price_dynamics.iloc[-1]
                obj_info_dct['current_price'] = '{0:,}'.format(round(last_price['avg_price_sqm'])).replace(',', ' ')
                obj_info_dct['contracts_cnt'] = last_price['contract_conclude_cnt']
                obj_info_dct['last_report_dt'] = last_price['report_month_dt']
       
            
                show_plot(price_dynamics_plot(price_dynamics))
            obj_info_params.update(obj_info_dct)
            toggle_block('obj_info_visibility',True)
            
            return render_template('info.html',obj_id = obj_id
                                            ,**obj_info_params
                                            ,**plot_param
                                            ,**display_pagams )
        else:
           return show_alert_info()
    
    clear_object_params()

    return render_template('info.html' ,**plot_param, **display_pagams)


@app.route('/forecast', methods=['GET','POST'])
def custom_object():
    toggle_block('param_settings_visibility',True)
    toggle_block('high_price_alert_visibility',False)
    toggle_block('alert_visibility',False)
    toggle_block('plot_visibility',False)
    if len(obj_info_params) != 0:
        fill_obj_params_from_eisgs()
    print(object_params.items())

    if request.method == 'POST':
        city_name = request.form['city-select']
        ao_name = request.form['okrug-select']
        raion_name = request.form['raion-select']
        class_name = request.form['class-select']
        commiss_dt = request.form['commissioning-select']
        forecast_period = request.form['period-input']
        current_price  = re.sub(r"[^0-9]","",request.form['current-price-input'])

        filter_checkboxes['city'] = bool(request.form.get('city-check'))
        filter_checkboxes['okrug'] = bool(request.form.get('okrug-check'))
        filter_checkboxes['raion'] = bool(request.form.get('raion-check'))
        filter_checkboxes['class'] = bool(request.form.get('class-check'))
        filter_checkboxes['history'] = bool(request.form.get('history-check'))
        
        
        # print(city_name,get_okrug_list(city_name))

        try:
            commiss_dt =  datetime.strptime(commiss_dt,'%Y-%m-%d').replace(day = 1)
            current_price = int(current_price)
            forecast_period = int(forecast_period)
        except Exception as e:
            
            toggle_block('alert_visibility',True)
            print(f"[WARN] Input error! \n {e}")
            return render_template('forecast.html' ,**plot_param
                                    ,**get_object_params()
                                    , **list_data_params
                                    , **display_pagams
                                    ,checkbox = filter_checkboxes
                                    ,history_allow = is_history_allow())
        params = {
                    "city" : city_name
                    ,"okrug" : ao_name
                    ,"raion" : raion_name
                    ,"class_name" :class_name
                    ,"commiss_dt" : datetime.strftime(commiss_dt,'%Y-%m-%d')
                    ,"current_price" : current_price
                    ,"forecast_period":forecast_period
                }
        update_object_params(params) 
        
        trashhold = 0
        forecast_params = {
            'current_price': remove_outliers_from_price_dinamics(trashhold)['avg_price_sqm'].to_list() \
                                if is_history_allow() and filter_checkboxes['history'] \
                                else current_price
            ,'commiss_dt':commiss_dt
            ,'period':forecast_period
            ,'city_name':city_name if city_name != '- Не выбран -' else None   
            ,'ao_name': ao_name if filter_checkboxes['okrug'] and ao_name != '- Не выбран -' else None    
            ,'raion_name': raion_name if filter_checkboxes['raion'] and raion_name != '- Не выбран -' else None    
            ,'class_name': class_name if filter_checkboxes['class'] and class_name != '- Не выбран -' else None   
        }
        forecast_df = calc.make_forecast_custom(**forecast_params)
        
        print(forecast_df)

        if forecast_df.empty:
            print('[WARN] Found no data!')
            toggle_block('alert_visibility',True)
        else:
            # annum_percent = percentage_per_annum(forecast_df.price_sqm_obj_forecast.dropna().iloc[0],forecast_df.price_sqm_obj_forecast.iloc[-1],forecast_period)
            percent = (-1+forecast_df.price_sqm_obj_forecast.iloc[-1]/forecast_df.price_sqm_obj_forecast.dropna().iloc[0]) * 100
            plot_param['percent'] = f"{percent:.2f}"
            plot_param['out_df'] = forecast_df
            # print(forecast_df)
            show_plot(price_forecast_plot(forecast_df,commiss_dt))
            check_high_price(forecast_df)

    if object_params['city']:
        list_data_params['okrug_list'] = get_okrug_list(object_params.get('city'))
        list_data_params['raion_list'] = get_raion_list(object_params.get('city'),object_params.get('okrug'))

    return render_template('forecast.html' ,**plot_param
                                        ,**get_object_params()
                                        , **list_data_params
                                        , **display_pagams
                                        ,checkbox = filter_checkboxes
                                        ,history_allow = is_history_allow())


def is_history_allow():
    return type(obj_info_params.get('price_dynamics') ) is type(pd.DataFrame()) and not obj_info_params.get('price_dynamics').empty
        
def remove_outliers_from_price_dinamics(count_trashhold = 3):
    df  = obj_info_params['price_dynamics'].copy()
    idx = df.query('contract_conclude_cnt < @count_trashhold').index
    df.loc[idx,'avg_price_sqm'] = None
    # print(df)
    df['avg_price_sqm'] = df['avg_price_sqm'].interpolate()
    return df 

def fill_obj_params_from_eisgs():
    if len(obj_info_params) == 0:
        print('No eisgs object loaded!')
        return
    
    for key in object_params:
        if key in obj_info_params.keys():
            object_params[key] = obj_info_params.get(key)
            


def show_alert_info(obj_id = ''):
    toggle_block('alert_visibility',True)
    return render_template('info.html',obj_id = obj_id
                            ,**display_pagams )

def check_high_price(forecast_df):
    if forecast_df.price_sqm_amt.dropna()[-1] < forecast_df.price_sqm_obj_forecast.dropna()[0]:
        toggle_block('high_price_alert_visibility',True)
    

def show_plot(url):
    toggle_block('plot_visibility',True)
    plot_param['plot_url'] = url


def price_dynamics_plot(price_dynamics_df):
    img = BytesIO()
    df = price_dynamics_df[:]
    df['report_month_dt']=df['report_month_dt'].map(lambda x : str(x)[:7])
    ax = sns.barplot(data = df, x = 'report_month_dt',y='avg_price_sqm',color='seagreen')
    for i,val in enumerate(df.iterrows()):
        r, val = val
        ax.annotate(val.contract_conclude_cnt,(i-0.25,val.avg_price_sqm+10),ha= 'center')
    plt.xlabel('')
    plt.ylabel('')
    plt.xticks(rotation=90)

    plt.savefig(img, format='png',bbox_inches='tight')
    plt.close()
    img.seek(0)
    plot_url = base64.b64encode(img.getvalue()).decode('utf8')
    return plot_url


def price_forecast_plot(forecast_df,commiss_dt):
    img = BytesIO()
    # sns.set_style("ticks",{'axes.grid' : True})
    ax = forecast_df.plot()
    ax.legend(labels = ['Факт рынка','Прогноз рынка','Прогноз объекта','Факт объекта'])
    number_format = lambda x : f'{x:,.0f}'.replace(",",' ')
    current_moment = forecast_df.price_sqm_amt.dropna().index[-1]
    last_moment = forecast_df.price_sqm_forecast.dropna().index[-1]
    plt.axvline(x=current_moment, color='g', linestyle='--')
    ax.annotate(str(current_moment)[:7],(current_moment,ax.get_ylim()[0]),ha= 'center')
    ax.annotate(str(last_moment)[:7],(last_moment,ax.get_ylim()[0]),ha= 'center')
    ax.annotate(number_format(forecast_df.price_sqm_obj_forecast[current_moment]),(current_moment,forecast_df.price_sqm_obj_forecast[current_moment]),ha= 'left')
    ax.annotate(number_format(forecast_df.price_sqm_forecast[current_moment]),(current_moment,forecast_df.price_sqm_forecast[current_moment]),ha= 'left')
    ax.annotate(number_format(forecast_df.price_sqm_obj_forecast[last_moment]),(last_moment,forecast_df.price_sqm_obj_forecast[last_moment]),ha= 'left')


    if commiss_dt <= last_moment:
        plt.axvline(x=commiss_dt, color='r', linestyle='--')
        ax.annotate(str(commiss_dt)[:7],(commiss_dt,ax.get_ylim()[0]),ha= 'center')
        ax.annotate(number_format(forecast_df.price_sqm_obj_forecast[commiss_dt]),(commiss_dt,forecast_df.price_sqm_obj_forecast[commiss_dt]),ha= 'left')
    else:
        ax.annotate(number_format(forecast_df.price_sqm_obj_forecast[last_moment]),(last_moment,forecast_df.price_sqm_obj_forecast[last_moment]),ha= 'left')

    plt.xlabel('')
    plt.ylabel('')
    plt.xticks(rotation=90)

    plt.savefig(img, format='png',bbox_inches='tight')
    plt.close()
    img.seek(0)
    plot_url = base64.b64encode(img.getvalue()).decode('utf8')
    return plot_url



def plot():
    img = BytesIO()
    y = [1,2,3,4,5]
    x = [0,2,1,3,4]

    plt.plot(x,y)

    plt.savefig(img, format='png')
    plt.close()
    img.seek(0)
    plot_url = base64.b64encode(img.getvalue()).decode('utf8')
    return plot_url



if __name__ == '__main__':
    app.run(debug = True)
    