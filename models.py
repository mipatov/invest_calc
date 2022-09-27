from datetime import datetime
from json import load
import numpy as np

from module import *

class Model():
    
    def __init__(self) -> None:
        pass
    
    def __call__(self,x):
        return self.predict(x)
    
    def copy(self):
        import copy

        return copy.copy(self)
    
    def predict(self,x):
        return x

    
class LinearModel(Model):
    
    def __init__(self,start_value,k) -> None:
        self.start_value = start_value
        self.k = k
   
    def predict(self,x):
        return self.start_value+self.k*x

class LinearMicroModel(LinearModel):
    
    def __init__(self,start_value,commis_period : int,macro : Model) -> None:
        k = (macro(commis_period)-start_value)/commis_period
        super().__init__(start_value,k)
        self.commis_period = commis_period
        self.macro = macro
    
    def predict(self,x):
        return super().predict(x) if x < self.commis_period else self.macro(x)

    
class CommisEffectLinearMicroModel(LinearModel):
    
    def __init__(self,start_value,commis_effect,commis_period,macro : Model) -> None:
        k = (macro(commis_period)/(1+commis_effect)-start_value)/commis_period
        super().__init__(start_value,k)
        self.commis_period = commis_period
        self.macro = macro

    def predict(self,x):
        return super().predict(x) if x < self.commis_period else self.macro(x)
    
class MacroLikeMicroModel(LinearModel):
    
    def __init__(self,start_value,commis_effect,commis_period,market_entry_period, macro : Model) -> None:
        macroLike = macro.copy()
        macroLike.start_market_price = start_value
        
        self.macroLike = macroLike
        self.commis_effect = commis_effect
        self.commis_period = commis_period
        self.market_entry_period = market_entry_period
        self.macro = macro
        
        linear_start_value = macroLike(commis_period)*(1+commis_effect)
        k = (macro(commis_period+market_entry_period)-linear_start_value)/market_entry_period
        super().__init__(linear_start_value,k)


    def predict(self,x):
        if x < self.commis_period:
            return self.macroLike(x)
        if x == self.commis_period:
            return self.macroLike(x)*(1+self.commis_effect)
        if x > self.commis_period and x < self.market_entry_period :
            return super().predict(x)
        if x >= self.market_entry_period :
            return self.macro(x)
        
class DummyMacroModel(Model):
    
    def __init__(self,start_market_price,grow_rate,infl_rate=0 ) -> None:
        self.start_market_price = start_market_price
        self.grow_rate = grow_rate 
        self.infl_rate = infl_rate 

    def predict(self,x):
        return self.start_market_price*pow(self.grow_rate,x)




class MacroMLModel(Model):

    def __init__(self,market_data,period,models_folder_path = 'models/') -> None:
        
        self.model_data_path = models_folder_path + '/model_data.csv'
        self.model_config_path = models_folder_path + '/models.conf'
        self.models_folder_path = models_folder_path + '/models'

        self.macro_forecast_dict = self.get_macro_forecast()
    
        self.market_forecast_df =self.forecast(market_data,period)


    def load_model(self,path):
        import pickle
        return pickle.load(open(path, 'rb'))


    def load_model_data(self,path,do_relative = False):
        data = pd.read_csv(path,index_col=0)
        if do_relative:
            return (data/data.shift(1)).dropna()*100
        return data


    def get_macro_forecast(self):
        from os import listdir
        from os.path import isfile, join, splitext,basename
        import json

        models_names = [join(self.models_folder_path, f) for f in listdir(self.models_folder_path) if splitext(f)[-1] == '.pkl']
        all_data = self.load_model_data(self.model_data_path,do_relative = True)
        
        with open(self.model_config_path, "r") as f:
            config_dict = json.load(f)


        print(f'Found {len(models_names)} models :')
        # print(*models_names,sep='\n')

        predicitons_list = []
        for path in models_names:
            name = splitext(basename(path))[0]
            enabled_flag = config_dict[name]['enabled']
            print(f'{name} eanbled is {enabled_flag}')
            if not enabled_flag:
                continue
            model = self.load_model(path)
            features = config_dict[name]['coefs'].keys()
            data = all_data[features]
            # print(path,features)
            predictions = model.predict(data)
            predicitons_list.append(predictions)
        
        predictions_dict = (pd.DataFrame(predicitons_list,columns=all_data.index).mean()/100).to_dict()

        return predictions_dict
    



    def forecast(self,market_data,period:int):
        from math import ceil

        first_idx = 6

        if len(market_data)<first_idx:
            first_idx = 0
        real_percent = market_data.iloc[-1]/market_data.iloc[-first_idx]
        
        quarters = pd.DataFrame([market_data[-first_idx:].index.year,market_data[-first_idx:].index.map(get_quarter)]).T.drop_duplicates()
        model_percent = 1
        print('\ncount model percent :')
        for i,v in quarters.iterrows():
            year_prec = self.macro_forecast_dict[v[0]]
            quarter_perc = year_prec**(1/4)
            model_percent *= quarter_perc
            print(i, v[0],year_prec,quarter_perc , model_percent)

        scale_c =(real_percent[0]-1)/ (model_percent-1)

        print( scale_c, real_percent[0], model_percent)

        if scale_c<0 :
            print('[WARN] negaive scale coef!')
        forecast_by_year_scaled = { k:scale_c*(v-1)+1  for k,v in self.macro_forecast_dict.items() }

        print('\nPrice dynamics prediction:')
        print(*forecast_by_year_scaled.items(),sep = '\n')

    #     prediction market
        market_quarter_df = get_quarter_from_dates(list(market_data.index))

        start_quarter_num = market_quarter_df.iloc[-1].quarter
        start_quarter_date = datetime.datetime(year = market_quarter_df.iloc[-1].year,month = get_quarter_months(start_quarter_num)[0],day = 1)
        start_price = market_data.loc[start_quarter_date][0]

        print('Quarter_data :',start_quarter_num,start_quarter_date,start_price)

        Q_period=  ceil(period/3)+1
        Q_dt = [add_months(start_quarter_date,3*i) for i in range(1,Q_period+1)]
        quarter_df = get_prev_quarter_from_dates(Q_dt)
        prediction_price_list = []
        
        
        for i,v in quarter_df.iterrows():

            year_prec = forecast_by_year_scaled[v[0]]
            quarter_perc = year_prec**(1/4)
            new_price  = start_price*quarter_perc

            prediction_price_list.append(new_price)
            start_price = new_price

        print(pd.DataFrame(prediction_price_list,index = quarter_df.index,columns=['price_sqm_forecast'])\
                .reindex([add_months(quarter_df.index[0],i) for i in range(Q_period*3)]))
       

        return pd.DataFrame(prediction_price_list,index = quarter_df.index,columns=['price_sqm_forecast'])\
                .reindex([add_months(quarter_df.index[0],i) for i in range(Q_period*3+1)])\
                .interpolate()
        
    def predict(self,x):
        return self.market_forecast.iloc[1,0]
    