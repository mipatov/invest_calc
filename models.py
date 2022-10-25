from datetime import datetime

from module import *


class Model():

    def __init__(self) -> None:
        pass

    def __call__(self, x):
        return self.predict(x)

    def copy(self):
        import copy

        return copy.copy(self)

    def predict(self, x):
        return x


class MacroMLModel(Model):

    def __init__(self, market_data, period, models_folder_path='models/') -> None:

        self.model_data_path = models_folder_path + '/model_data.csv'
        self.model_config_path = models_folder_path + '/models.conf'
        self.models_folder_path = models_folder_path + '/models'

        self.macro_forecast_dict = self.get_macro_forecast()

        self.market_forecast_df = self.forecast(market_data, period)

    def load_model(self, path):
        """
        Загрузка модели
        """
        import pickle
        return pickle.load(open(path, 'rb'))

    def load_model_data(self, path, do_relative=False):
        """
        Загрузка данных для прогнозов (мкропараметры)
        :param path: путь к данным
        :param do_relative: Сделать данные относительными
        """
        data = pd.read_csv(path, index_col=0)
        if do_relative:
            return (data/data.shift(1)).dropna()*100
        return data

    def get_macro_forecast(self):
        """
        Макропрогноз
        """
        import json
        from os import listdir
        from os.path import basename, isfile, join, splitext

        models_names = [join(self.models_folder_path, f) for f in listdir(
            self.models_folder_path) if splitext(f)[-1] == '.pkl']
        all_data = self.load_model_data(self.model_data_path, do_relative=True)

        with open(self.model_config_path, "r") as f:
            config_dict = json.load(f)

        print(f'Found {len(models_names)} models :')
        # print(*models_names,sep='\n')

        predicitons_list = []
        for path in models_names:
            name = splitext(basename(path))[0]
            enabled_flag = config_dict[name]['enabled']
            print(f'{name} enabled is {enabled_flag}')
            if not enabled_flag:
                continue
            model = self.load_model(path)
            features = config_dict[name]['coefs'].keys()
            data = all_data[features]
            # print(path,features)
            predictions = model.predict(data)
            predicitons_list.append(predictions)

        predictions_dict = (pd.DataFrame(
            predicitons_list, columns=all_data.index).mean()/100).to_dict()

        return predictions_dict

    def forecast(self, market_data, period: int):
        """
        Прогноз рынка
        """
        from math import ceil
    
        first_idx = 12
        # следующая констуркция рассчитывает реальное изменени и сопоставляет его с изменением по прогнозу модели. 
        # таким образом высчитывается коэффициент масштабирования прогнозов модели для текущего рынка
        # в цикле периоды, на которых считаем реальное изменение, пока это изменение не станет одного знака с прогнозным изменением
        # если таких не нашлось, то тут полномочия модели - всё.
        # в таких случаях просто считаем коэффициент масштабирования = 1 , хоть это и неправильно 
        for i in range(first_idx, 1, -1):
            if len(market_data) < i:
                i = 0
     
            real_percent = market_data.iloc[-1,0]/market_data.iloc[0,0]

            quarters = pd.DataFrame(
                [market_data[-i:].index.year, market_data[-i:].index.map(get_quarter)]).T.drop_duplicates()

            model_percent = 1
            
            for i, v in quarters.iterrows():
                year_prec = self.macro_forecast_dict[v[0]]
                quarter_perc = year_prec**(1/4)
                model_percent *= quarter_perc

            scale_c = (real_percent-1) / (model_percent-1)

            if scale_c > 0:
                break

        if scale_c < 0:
            scale_c = 1
            print('[WARN] Scale_c coeff is negative. Force set scale_c = 1')

        print('scale_c = ',scale_c,' real% = ', real_percent,' model% = ', model_percent)

        # масштабируем прогнозы модели 
        forecast_by_year_scaled = {k: scale_c*(v-1)+1 for k, v in self.macro_forecast_dict.items()}

        market_quarter_df = get_quarter_from_dates(list(market_data.index))

        # определеям номер первого квартала нашего прогноза и дату его начала. берем цену в эту дату - это стартовая точка прогоза 
        start_quarter_num = market_quarter_df.iloc[-1].quarter
        start_quarter_date = datetime.datetime(year=market_quarter_df.iloc[-1].year, month=get_quarter_months(start_quarter_num)[0], day=1)
        if(datetime.datetime.strftime(start_quarter_date, '%Y-%m-%d') not in market_data.index):
            start_quarter_date = add_months(start_quarter_date, 3)
        start_price = market_data.loc[datetime.datetime.strftime(start_quarter_date, '%Y-%m-%d')][0]

        #  применяем прогнозы поквартально, считая что каждый квартал в году вносит равный порцент изменения
        Q_period = ceil(period/3)+1
        Q_dt = [add_months(start_quarter_date, 3*i) for i in range(1, Q_period+1)]
        quarter_df = get_prev_quarter_from_dates(Q_dt)
        prediction_price_list = [start_price]

        for i, v in quarter_df.iterrows():

            year_prec = forecast_by_year_scaled[v[0]]
            quarter_perc = year_prec**(1/4)
            new_price = start_price*quarter_perc

            prediction_price_list.append(new_price)
            start_price = new_price

        idx = [start_quarter_date]+list(quarter_df.index)

        forecast = pd.DataFrame(prediction_price_list, index=idx, columns=['price_sqm_forecast'])\
            .reindex([add_months(idx[0], i) for i in range(Q_period*3+1)])

        return forecast.interpolate()

    def predict(self, x):
        return self.market_forecast.iloc[x, 0]
