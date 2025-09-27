import math
from typing import Iterator
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


casos = pd.read_csv( 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/' + 'master/csse_covid_19_data/csse_covid_19_daily_reports/01-12-2021.csv ', sep=',' )


#1. Extração
#1.1 Casos de Covid

def date_range(start_date: datetime, end_date: datetime) -> Iterator[datetime]:
  date_range_days: int = (end_date - start_date).days
  for lag in range(date_range_days):
    yield start_date + timedelta(lag)


start_date = datetime(2021, 1, 1)
end_date = datetime(2021, 12, 31)

cases_list = []

for date in date_range(start_date=start_date, end_date=end_date):

  date_str = date.strftime('%m-%d-%Y')
  data_source_url = f'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{date_str}.csv'

  try:
    case = pd.read_csv(data_source_url, sep=',')
    case = case.drop(['FIPS', 'Admin2', 'Last_Update', 'Lat', 'Long_', 'Recovered', 'Active', 'Combined_Key', 'Case_Fatality_Ratio'], axis=1)
    case = case.query('Country_Region == "Brazil"').reset_index(drop=True)
    case['Date'] = pd.to_datetime(date.strftime('%Y-%m-%d'))
    cases_list.append(case)
  except Exception as e:
    print(f"Error loading data for date {date_str}: {e}")


cases = pd.concat(cases_list, axis=0, ignore_index=True) if cases_list else pd.DataFrame()

print("Data loading complete.")

cases.query('Province_State == "Sao Paulo"').head()


#1.2 Wrangling

cases.head()

cases.shape

cases.info()


cases = cases.rename(
    columns={
        'Province_State': 'state',
        'Country_Region': 'country'
    }
)

for col in cases.columns:
  cases = cases.rename(columns={col: col.lower()})


states_map = {
    'Amapa': 'Amapá',
    'Ceara': 'Ceará',
    'Espiritto Santo': 'Espírito Santo',
    'Goias': 'Goiás',
    'Para': 'Pará',
    'Paraiba': 'Paraíba',
    'Parana': 'Paraná',
    'Piaui': 'Piauí',
    'Rondonia': 'Rondônia',
    'Sao Paulo': 'São Paulo'
}

cases['state'] = cases['state'].apply(lambda state: states_map.get(state, state) if state in states_map.keys() else state)

cases['month'] = cases['date'].apply(lambda date: date.strftime('%Y-%m'))
cases['year'] = cases['date'].apply(lambda date: date.strftime('%Y'))

cases['population'] = round(100000 * (cases['confirmed'] / cases['incident_rate']))
cases = cases.drop('incident_rate', axis= 1)

cases_ = pd.DataFrame()
cases_is_empty = True

def get_trend(rate: float) -> str:

  if np.isnan(rate):
    return np.nan

  if rate <0.75:
    status = 'Downward'
  elif rate > 1.15:
    status = 'Upward'
  else:
    status = 'Stable'

  return status


for state in cases['state'].drop_duplicates():

  cases_per_state = cases.query(f'state == "{state}"').reset_index(drop=True)
  cases_per_state = cases_per_state.sort_values(by=['date'])

  cases_per_state['confirmed_1d'] = cases_per_state['confirmed'].diff(periods=1)
  cases_per_state['confirmed_moving_avg_7d'] = np.ceil(cases_per_state['confirmed_1d'].rolling(window=7).mean())
  cases_per_state['confirmed_moving_avg_7d_rate_14d'] = cases_per_state['confirmed_moving_avg_7d']/cases_per_state['confirmed_moving_avg_7d'].shift(periods=14)
  cases_per_state['confirmed_trend'] = cases_per_state['confirmed_moving_avg_7d_rate_14d'].apply(get_trend)

  cases_per_state['deaths_1d'] = cases_per_state['deaths'].diff(periods=1)
  cases_per_state['deaths_moving_avg_7d'] = np.ceil(cases_per_state['deaths_1d'].rolling(window=7).mean())
  cases_per_state['deaths_moving_avg_7d_rate_14d'] = cases_per_state['deaths_moving_avg_7d']/cases_per_state['deaths_moving_avg_7d'].shift(periods=14)
  cases_per_state['deaths_trend'] = cases_per_state['deaths_moving_avg_7d_rate_14d'].apply(get_trend)

  if cases_is_empty:
    cases_ = cases_per_state
    cases_is_empty = False
  else:
    cases_ = pd.concat([cases_, cases_per_state],axis=0, ignore_index=True)

cases = cases_

cases['population'] = cases['population'].astype('int64')
cases['confirmed_1d'] = cases['confirmed_1d'].astype('Int64')
cases['confirmed_moving_avg_7d'] = cases['confirmed_moving_avg_7d'].astype('Int64')
cases['deaths_1d'] = cases['deaths_1d'].astype('Int64')
cases['deaths_moving_avg_7d'] = cases['deaths_moving_avg_7d'].astype('Int64')

cases = cases[['date', 'country', 'state', 'population', 'confirmed', 'confirmed_1d', 'confirmed_moving_avg_7d', 'confirmed_moving_avg_7d_rate_14d', 'confirmed_trend', 'deaths', 'deaths_1d', 'deaths_moving_avg_7d', 'deaths_moving_avg_7d_rate_14d', 'deaths_trend', 'month', 'year']]

cases.to_csv('./covid-cases.csv', sep=',', index=False)


#1.3 Vacinação
#1.3.1 Extração

vaccines = pd.read_csv('https://covid.ourworldindata.org/data/owid-covid-data.csv', sep=',', parse_dates=[3], infer_datetime_format=True)

vaccines.head()

vaccines = vaccines.query('location == "Brazil"').reset_index(drop=True)
vaccines = vaccines[['location', 'population', 'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated', 'total_boosters', 'date']]

#1.3.2 Wrangling

vaccines.head()

vaccines.shape

vaccines.info()

vaccines = vaccines.fillna(method= 'ffill')

vaccines = vaccines[(vaccines['date'] >= '2021-01-01') & (vaccines['date'] <= '2021-12-31').reset_index(drop=True)]

vaccines = vaccines.rename(
    columns={
        'location': 'country',
        'total_vaccinations': 'total',
        'people_vaccinated': 'one_shot',
        'people_fully_vaccinated': 'two_shots',
        'total_boosters': 'three_shots'
    }
)

vaccines['month'] = vaccines['date'].apply(lambda date: date.strftime('%Y-%m'))
vaccines['year'] = vaccines['date'].apply(lambda date: date.strftime('%Y'))

vaccines['one_shot_perc'] = round(vaccines['one_shot'] / vaccines['population'], 4)
vaccines['two_shots_perc'] = round(vaccines['two_shots'] / vaccines['population'], 4)
vaccines['three_shots_perc'] = round(vaccines['three_shots'] / vaccines['population'], 4)

vaccines['population'] = vaccines['population'].astype('Int64')
vaccines['total'] = vaccines['total'].astype('Int64')
vaccines['one_shot'] = vaccines['one_shot'].astype('Int64')
vaccines['two_shots'] = vaccines['two_shots'].astype('Int64')
vaccines['three_shots'] = vaccines['three_shots'].astype('Int64')

vaccines = vaccines[['date', 'country', 'population', 'total', 'one_shot', 'one_shot_perc', 'two_shots', 'two_shots_perc', 'three_shots', 'three_shots_perc']]

vaccines.tail()

vaccines.to_csv('./covid-vaccines.csv', sep=',', index= False)





