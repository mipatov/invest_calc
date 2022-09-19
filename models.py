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

    def __init__(self,market_data,period,Q_weidhts=[0.15,0.25,0.25,0.35]) -> None:
        
        model_path = 'models/relative_macro_model.pkl'
        model_data_path = 'data/abs_data.csv'
        self.model = self.load_model(model_path)
        self.model_data = self.load_model_data(model_data_path,do_relative = True)
        # print(self.model_data)
        self.macro_forecast_dict = self.get_macro_forecast()
    
        self.market_forecast_df =self.forecast(market_data,period,Q_weights = Q_weidhts)


    def load_model(self,path):
        import pickle
        return pickle.load(open(path, 'rb'))


    def load_model_data(self,path,do_relative = False):
        data = pd.read_csv(path,index_col=0)
        if do_relative:
            return (data/data.shift(1)).dropna()*100
        return data


    def get_macro_forecast(self):
        # predictions_dict = {
        #     2021:1.38,
        #     2022:1.47,
        #     2023:0.91,
        #     2024:1.01
        # }
        predictions_dict = dict(zip(self.model_data.index,self.model.predict(self.model_data)/100))
        # forecast_by_monthes= {k:pow(v,1/12) for k,v in forecast_by_year.items() }
        return predictions_dict


    def forecast(self,market_data,period:int, Q_weights = [0.15,0.25,0.25,0.35]):
        from math import ceil

    #     scaling for market
        first_idx = 12
        if len(market_data)<12:
            first_idx = 0
        real_percent = market_data.iloc[-1]/market_data.iloc[-first_idx]

        quarters = pd.DataFrame([market_data[-first_idx:].index.year,market_data[-first_idx:].index.map(get_quarter)]).T.drop_duplicates()
        model_percent = 1
        for i,v in quarters.iterrows():
            year_prec = self.macro_forecast_dict[v[0]]-1
            model_percent += year_prec*Q_weights[v[1]-1]

        scale_c =real_percent[0]/ model_percent
        forecast_by_year_scaled = { k:scale_c*(v-1)+1  for k,v in self.macro_forecast_dict.items() }
        print(self.macro_forecast_dict)


    #     prediction market
        market_quarter_df = get_quarter_from_dates(list(market_data.index))

        prev_year = market_quarter_df.iloc[-1].year
        start_quarter_num = market_quarter_df.iloc[-1].quarter
        start_quarter_date = datetime.datetime(year = prev_year,month = get_quarter_months(start_quarter_num)[0],day = 1)
        start_price = market_data.loc[start_quarter_date][0]


        Q_period=  ceil(period/3)+1
        Q_dt = [add_months(start_quarter_date,3*i) for i in range(0,Q_period+2)]
        quarter_df = get_prev_quarter_from_dates(Q_dt)
        prediction_price_list = []
        
        
        for i,v in quarter_df.iterrows():
            if prev_year!=v[0]:
                prev_year=v[0]
                start_price = new_price
                start_quarter_num = 1
            year_prec = forecast_by_year_scaled[v[0]]-1
            new_price  = start_price*(1+ year_prec*sum([Q_weights[q-1] for q in range(start_quarter_num,v[1]+1)]))
            prediction_price_list.append(new_price)
          
        # dt_format = lambda dt: datetime.datetime.strftime('%Y-%m-%d')
        # date2dt = lambda dt : datetime.datetime.combine(dt, datetime.datetime.min.time())
        # print(quarter_df.index)
        # print([add_months(quarter_df.index[0],i) for i in range(Q_period*3)])
        return pd.DataFrame(prediction_price_list,index = quarter_df.index,columns=['price_sqm_forecast'])\
                .reindex([add_months(quarter_df.index[0],i) for i in range(Q_period*3)])\
                .interpolate()
        
    def predict(self,x):
        return self.market_forecast.iloc[1,0]
    