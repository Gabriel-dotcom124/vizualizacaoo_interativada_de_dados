#1. Google Data Studio

#2. Extração

import pandas as pd
from datetime import datetime

!wget -q 'https://raw.githubusercontent.com/andre-marcos-perez/ebac-course-utils/main/dataset/world-population.csv' -O 'world_population.csv'

data = pd.read_csv('world_population.csv', sep= ',', parse_dates=[2], infer_datetime_format=True)

data.head()

data.shape


data = data.drop(["id"], axis = 1)

data = data.rename(columns={'location': 'country', 'time': 'date'})

data['date'] = pd.to_datetime(data['date'], errors='coerce')

data['child_perc'] = 100 * data['child'] / data['total']
data['adolescent_perc'] = 100 * data['adolescent'] / data['total']
data['adult_perc'] = 100 * data['adult'] / data['total']
data['old_perc'] = 100 * data['old'] / data['total']

data['male_perc'] = 100 * data['male'] / data['total']
data['female_perc'] = 100 * data['female'] / data['total']

for col in data.columns:
    if col not in ['date', 'country']:
        data[col] = data[col].apply(lambda num: round(num, 2))

data = data[['date','country','total', 'male','male_perc', 'female', 'female_perc', 'child', 'child_perc', 'adolescent', 'adolescent_perc','adult', 'adult_perc', 'old', 'old_perc' ]]

data.to_csv('./world-population-wrangled.csv', sep= ';', index= False)


