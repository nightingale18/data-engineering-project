import pandas as pd
from dateutil.relativedelta import relativedelta

def prevent_errors(df):
    df.dropna(how='all',axis=0, inplace=True)
    df.drop_duplicates(inplace = True)

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
def extract_d1(url='https://cdn.cloud.prio.org/files/ee1003be-b9ed-4fba-8345-8f8501e60177/Natresconfl_v1.dta?inline=true'): 
    return pd.read_stata(url)

def transform_d1(df):
    prevent_errors(df)

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
    return df


def load(df, table_name, table_path): df.to_sql(table_name, table_path, if_exists='replace', index=False)

def etl_pipeline_d1(url):
    df = extract_d1(url)
    df = transform_d1(df)
    load(df, 'conflicts', 'sqlite:///data/conflicts.sqlite')

etl_pipeline_d1()

# Dataset 2: UCDP Battle-Related Deaths Dataset version 24.1
def extract_d2(zip_url='https://ucdp.uu.se/downloads/brd/ucdp-brd-conf-241-csv.zip'):
    return pd.read_csv(
            zip_url,
            compression='zip', 
            sep=','
        )
def transform_d2(df):
    prevent_errors(df)

    df = df.drop(columns=['conflict_id', 'dyad_id', 'side_a_id', 'side_a_2nd', 'side_b_id', 'side_b_2nd', 'territory_name', 'type_of_conflict', 'battle_location',
                        'gwno_a', 'gwno_a_2nd', 'gwno_b', 'gwno_b_2nd', 'gwno_loc', 'gwno_battle', 'version'])

    # select Americas
    df = df.loc[df['region'] == '5']

    df = df.rename(columns={'location_inc': 'location'})
    df = df.drop(columns=['region'])
    return df

def etl_pipeline_d2(url):
    df = extract_d2(url)
    df = transform_d2(df)
    load(df, 'deaths', 'sqlite:///data/deaths.sqlite')

etl_pipeline_d2()