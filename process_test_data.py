import pandas as pd
import process_region_data as rd


### Data sources ###

source = 'https://www.data.gouv.fr/fr/datasets/donnees-relatives-aux-resultats-des-tests-virologiques-covid-19/'
url = "https://www.data.gouv.fr/fr/datasets/r/406c6a23-e283-4300-9484-54e78c8ae675"
meta_url = "https://www.data.gouv.fr/fr/datasets/r/39aaad1c-9aac-4be8-96b2-6d001f892b34"

## Data processing functions ###

def make_df(url=url):
    df = pd.read_csv(url, sep=';', dtype={'dep':'str'})
    return df

# make age_range col easier to understand

def make_age_ranges(ages):
    better_ranges = ['-'.join([str(x-9),str(x)]) if 90 > x > 0 else str(x) for x in ages]
    # for 90+ years
    better_ranges = [x.replace(x, '{}+'.format(x)) if x=='90' else x for x in better_ranges]
    # Warning: 0 denotes 'all ages'
    better_ranges = [x.replace(x, 'all ages') if x=='0' else x for x in better_ranges]
    
    return better_ranges
    

def fix_ages(age_col):
    '''Replace age ranges so the range is explicit in the label. 
    Makes plots easier to read OOTB.'''
    
    ages = age_col.unique()
    better_ranges = make_age_ranges(ages)
    replace_dict = dict(zip(ages, better_ranges))
    age_col = age_col.replace(replace_dict)
    
    return age_col
 

def create_testing_df(all_ages=True):
    df = make_df()
    # remove redundant age categories
    if all_ages==True:
        df = df.loc[df['cl_age90']==0]
    else:
        df = df.loc[df['cl_age90']!=0]   
    
    # rename columns
    new_names = {'P':'positive_tests',
                 'T': 'total_tests',
                 'cl_age90': 'age_range'}

    df = df.rename(columns = new_names)
    df['age_range'] = fix_ages(df['age_range'])

    # calc positivity rate - Pointless?? It all seems based on 7day rolling totals
    df['pct_pos_tests'] = df['positive_tests'].divide(df['total_tests']).multiply(100).round(2)

    # get region & dept names & codes, as well as dept population
    reg_ref_df = rd.create_region_df()
    
     # cleaning: sometimes deps are numeric
    #df['dep'] = df['dep'].astype('str')
    #reg_ref_df['dep'] = reg_ref_df['dep'].astype('str') 
    
    # merge region with testing data
    df = reg_ref_df.drop(['ICU_beds'], axis=1).merge(df)

    # flag dom-tom - REDUNDANT??
    df['dom_tom'] = df['reg'] < 10
    df['dom_tom'] = df['dom_tom'].astype('str')
    
    #df['reg'] = df['reg'].astype('int')

    return df

# make rolling variables


def make_rolling_sum(metric, facet_col, df, index_col='jour', n=7):
    rolling_sum = df.groupby([facet_col, index_col])[metric].sum().unstack(0).rolling(n).sum()
    #rolling_sum = df.pivot(index_col, facet_col, metric).rolling(n).sum()
    return rolling_sum

def rolling_avg(col, n):
    rolling_avg = col.rolling(n).mean()
    return rolling_avg

def make_rolling_avg(metric, df, facet_col='libelle_dep', index_col='jour', n=7):
    #rolling_avg = df.groupby([facet_col, index_col])[metric].sum().unstack(0).rolling(n).mean()
    rolling_avg = df.set_index([facet_col, index_col])[metric].unstack(0).rolling(7).mean().round(4)
    return rolling_avg

def to_percent(num_col, denom_col, dec_places=2):
    pct_col = num_col.multiply(100).divide(denom_col).round(dec_places)
    return pct_col

def get_pop(geo_col, df, pop_col='population'):
    pop = df.groupby(geo_col)[pop_col].max()
    return pop

def to_100k_pop(metric_col, geo_col, df, dec_places=2):
    pop_col = get_pop(geo_col, df)
    col_100k_pop = metric_col.T.multiply(100000).div(pop_col, axis=0).round(dec_places).T
    
    return col_100k_pop
    
def create_rolling_cols(df):
    rolling_pos = make_rolling_sum( 'positive_tests', 'libelle_dep', df)
    rolling_total = make_rolling_sum( 'total_tests', 'libelle_dep', df)
    rolling_pos_rate = to_percent(rolling_pos, rolling_total)
    rolling_pos_100k = to_100k_pop(rolling_pos, 'libelle_dep', df)
    rolling_test_100k = to_100k_pop(rolling_total, 'libelle_dep', df)

    df = df.merge(rolling_pos_100k.stack().reset_index()).rename(columns={0:'rolling_pos_100k'})
    df = df.merge(rolling_pos_rate.stack().reset_index()).rename(columns={0:'rolling_pos_rate'})
    df = df.merge(rolling_test_100k.stack().reset_index()).rename(columns={0:'rolling_test_100k'})
    
    return df

### for dept-age df ###

def create_pop_age_df():
    pop_age_df = rd.create_pop_age_df().reset_index('libelle_dep').set_index('libelle_dep')
    pop_age_df = pop_age_df.stack().reset_index().rename(columns={'level_1': 'age_range',
                                                                  0: 'ag_pop'})
    # manually fix dept names
    pop_age_df['libelle_dep'] = pop_age_df['libelle_dep'].replace({'Guadeloupe ':"Guadeloupe",
                                                                  'Martinique ':"Martinique"})
    
    return pop_age_df

def create_dept_age_df():
    df = create_testing_df(False)
    dept_age_df = df.set_index(['libelle_dep', 'jour', 'age_range'])[['positive_tests', 'total_tests']]
    dept_age_df['pos_rate'] = dept_age_df['positive_tests'].multiply(100).divide(dept_age_df['total_tests']).round(2)
    dept_age_df = dept_age_df.reset_index()
    
    pop_age_df = create_pop_age_df()
    
    # merge pop data with age-dept df
    dept_age_df = dept_age_df.merge(pop_age_df, on=['libelle_dep', 'age_range'], how='left').sort_values(['libelle_dep', 'jour'])
    
    # calc incidence rate
    dept_age_df['pos_100k'] = dept_age_df['positive_tests'].multiply(100000).divide(dept_age_df['ag_pop']).round(2)

    return dept_age_df

# group into above & below 70

def calc_older_incid():
    '''creates a dataframe that compares 7d-rolling 
    incidence rate for under 70s vs 70+. 
    70+ column is also a kpi for overview map of alert levels.'''
    
    older = ['70-79', '80-89', '90+']
    dept_age_df = create_dept_age_df()
    dept_age_df['older'] = dept_age_df['age_range'].isin(older)
    older_df = dept_age_df.groupby(['libelle_dep', 'jour', 'older'])[['positive_tests', 'ag_pop']].sum()

    # calculate incidence rate
    older_df['pos_100k'] = older_df['positive_tests'].multiply(100000).div(older_df['ag_pop']).round(2)
    # make rolling 7-day totals
    older_incid = older_df['pos_100k'].unstack(0).unstack().rolling(7).sum().stack(0)
    older_incid.columns = ['Under 70', '70+']
    
    return older_incid
