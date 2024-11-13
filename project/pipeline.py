import pandas as pd
from dateutil.relativedelta import relativedelta

americas_col = [
    'Bolivia',
    'Paraguay',
    'Costa Rica',
    'Guatemala',
    'Cuba',
    'Argentina',
    'Venezuela',
    'Colombia',
    'Dominican Republic',
    'Peru',
    'El Salvador',
    'Uruguay',
    'Surinam',
    'Panama',
    'Trinidad and Tobago',
    'Haiti',
    'Mexico',
    'Nicaragua'
]

# Dataset 1: The Natural Resource Conflict Dataset

df = pd.read_stata(
    'https://cdn.cloud.prio.org/files/ee1003be-b9ed-4fba-8345-8f8501e60177/Natresconfl_v1.dta?inline=true'
)

df['start_date'] = pd.to_datetime(df['epstartdate'])
df['end_date'] = pd.to_datetime(df['ependdate'])


df = df.drop(columns=['acdid', 'sidea', 'sideb', 'ccode', 'res_confl', 'aggrav', 'finance', 'distribution', 'epstartdate', 'ependdate'])
df = df.loc[df['location'].isin(americas_col)]


def calculate_diff(row):
    rd = relativedelta(row['end_date'], row['start_date'])
    return pd.Series([rd.years, rd.months, rd.days], index=['years', 'months', 'days'])

df['whole_days'] = (df['end_date'] - df['start_date']).dt.days
df[['years', 'months', 'days']] = df.apply(calculate_diff, axis=1)
df = df.drop(columns=['start_date', 'end_date'])


df.to_sql('conflicts', 'sqlite:///data/conflicts.sqlite', if_exists='replace', index=False)

# Dataset 2: UCDP Battle-Related Deaths Dataset version 24.1

df = pd.read_csv(
    'https://ucdp.uu.se/downloads/brd/ucdp-brd-conf-241-csv.zip',
    compression='zip', 
    sep=','
)

df = df.drop(columns=['conflict_id', 'dyad_id', 'side_a_id', 'side_a_2nd', 'side_b_id', 'side_b_2nd', 'territory_name', 'type_of_conflict', 'battle_location',
                      'gwno_a', 'gwno_a_2nd', 'gwno_b', 'gwno_b_2nd', 'gwno_loc', 'gwno_battle', 'version'])

# select Americas
df = df.loc[df['region'] == '5']

df.to_sql('deaths', 'sqlite:///data/deaths.sqlite', if_exists='replace', index=False)