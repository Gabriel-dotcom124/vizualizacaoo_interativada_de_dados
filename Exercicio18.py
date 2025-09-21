import pandas as pd
import seaborn as sns
import numpy as np


flights = sns.load_dataset('flights')

print("Anos únicos:", flights['year'].unique())
print("Meses únicos:", flights['month'].unique())

num_years = flights['year'].nunique()
num_months = flights['month'].nunique()

print(f"\nNúmero de anos únicos: {num_years}")
print(f"Número de meses únicos: {num_months}")


min_year = flights['year'].min()
max_year = flights['year'].max()

min_month = flights['month'].astype(str).min()
max_month = flights['month'].astype(str).max()


print(f"Intervalo de tempo:")
print(f"Ano mínimo: {min_year}")
print(f"Ano máximo: {max_year}")
print(f"Mês mínimo: {min_month}")
print(f"Mês máximo: {max_month}")


month_map = {
    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
    'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
}

flights['month_num'] = flights['month'].map(month_map)

display(flights.head())


flights['year-month'] = flights['year'].astype(str) + '-' + flights['month_num'].astype(str).str.zfill(2)
display(flights.head())


flights = flights[['year-month', 'year', 'month', 'passengers']]
display(flights.head())


flights.to_csv('flights.csv', index=False)