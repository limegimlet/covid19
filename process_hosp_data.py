import pandas as pd


## Data sources ##

# FR regions

dept_url = "https://www.insee.fr/fr/statistiques/fichier/3720946/departement2019-csv.zip"
reg_url = "https://www.insee.fr/fr/statistiques/fichier/3720946/region2019-csv.zip"

# FR hospital covid-19 cases

source = 'https://www.data.gouv.fr/en/datasets/donnees-hospitalieres-relatives-a-lepidemie-de-covid-19/'
hosp_url = "https://www.data.gouv.fr/fr/datasets/r/63352e38-d353-4b54-bfd1-f1b3ee1cabd7"
hosp_meta_url = "https://www.data.gouv.fr/fr/datasets/r/3f0f1885-25f4-4102-bbab-edec5a58e34a"

# FR ICU (réanimation) beds

icu_xls = "/data/drees_lits_reanimation_2013-2018.xlsx"

# FR population
# Source: https://www.insee.fr/fr/statistiques/4265439?sommaire=4265511

pop_csv = "data/Departements.csv"

## Helper functions ##

def get_hosp_data(url=hosp_url):
    '''Gets French hospital case counts
    for covid-19, by day & department.
    Used to plot disease progression.'''
    
    df = pd.read_csv(url, sep = ";")
    df = df.drop(df.loc[df['sexe']==0].index).reindex()  # remove sexe==0 as it's sum of M+F cases
    df['sexe'].replace({1:"m", 2:"f"}, inplace=True) # use more intuitive values too\
    
    # fix dates with wrong format
    replace_dict = {'27/06/2020': '2020-06-27',
                '28/06/2020': '2020-06-28',
                '29/06/2020': '2020-06-29'}

    df['jour'] = df['jour'].replace(replace_dict)
    
    return df
    
def get_hosp_metadata(url=hosp_meta_url):
    hosp_meta_df = pd.read_csv(url, sep = ";")
    
    return hosp_meta_df


def display_data_summary(df=get_hosp_data()):
    start = hosp_df['jour'].min()
    end = hosp_df['jour'].max()
    
    print("\n=== Main data frame (df): ===\n")
    print("For period {} to {}\n".format(start, end))
    print("HEAD:\n")
    print(df.head())
    print("\nTAIL: \n")
    print(df.tail())
    
    
def display_data_description(df=get_hosp_metadata()):
    meta_df = get_hosp_metadata(df)
    print("\n\n=== Column descriptions (meta_df): ===")
    return meta_df

def get_region_data(regions=reg_url, depts=dept_url):
    '''Gets names & codes for French regions & departments.
    Used to provide more meaningful plot titles.'''
    
    reg_ref_df = pd.read_csv(regions, compression='zip', usecols=['reg', 'libelle'])
    dept_df = pd.read_csv(depts, compression="zip", usecols=['dep', 'reg', 'libelle'])
    reg_only_df = reg_ref_df.merge(dept_df, on = 'reg', suffixes=('_reg', '_dep'))
    
    return reg_only_df


def get_icu_beds(xls=icu_xls):
    """Gets dept ICU bed counts. For simplicity, it
    is a total of public & private beds. Used
    for indicating ICU stress."""
    
    icu_beds = pd.read_excel(xls, sheet_name="Détails_statut_type_2018", skiprows=9)
    icu_beds = icu_beds[icu_beds.columns[:5]].rename(columns={'Code':'dep'}).set_index('dep')
    icu_ttl = pd.DataFrame(icu_beds.sum(axis=1), columns=['ICU_beds']).reset_index()
    
    return icu_ttl

def get_pop_data(csv=pop_csv):
    """Gets dept population, to calculate per capita
    death rates."""
    
    pop_df = pd.read_csv(csv, sep=';', usecols=['CODDEP', 'PTOT']).dropna(axis=1)
    pop_df.columns = ['dep', 'population']
    
    return pop_df

def create_main_df():
    hosp_df = get_hosp_data()
    reg_only_df = get_region_data()
    
    df = reg_only_df.merge(hosp_df)
    return df
    

def create_region_df():
    '''Combines all geo-based df's into one 'lookup' df.
    Used for adding more context to plots.'''
    
    reg_only_df = get_region_data()
    icu_df = get_icu_beds()
    pop_df = get_pop_data()
    
    reg_ref_df = reg_only_df.merge(icu_df, how='left').merge(pop_df, how='left')
    return reg_ref_df

def create_rea_df(level):
    '''Get ICU beds for regions'''
    
    reg_ref_df = create_region_df()
    hosp_df = create_main_df()
    
    if level=='reg':
        icu_beds = reg_ref_df.groupby('libelle_reg')['ICU_beds'].sum().reset_index()
        rea_df = hosp_df.groupby(['libelle_reg', 'jour'])['rea'].sum().reset_index()
        rea_df = rea_df.merge(icu_beds)\
            .merge(reg_ref_df[['libelle_reg', 'libelle_dep']])\
            .drop('libelle_reg', axis=1)\
            .set_index(['libelle_dep', 'jour'])
    
    elif level=='dep':
        icu_beds = reg_ref_df.groupby('libelle_dep')['ICU_beds'].max().reset_index()
        rea_df = hosp_df.groupby(['libelle_reg', 'libelle_dep', 'jour'])['rea'].sum().reset_index()
        rea_df = rea_df.merge(icu_beds).set_index(['libelle_dep', 'jour'])
        #rea_df['rea%'] = pt.to_percent(rea_df['rea'], rea_df['ICU_beds'])
    
    return rea_df


if __name__=='__main__':

    ## put it all together ##

    df = create_main_df()
    reg_ref_df = create_region_df()
    #meta_df = display_data_description()

    print('\n\nMAIN DATAFRAME:\n')
    print('*** HEAD ***\n')
    print(df.head())
    print('\n*** TAIL ***\n')
    print(df.tail())
          
    print("'\n\nREGION 'LOOKUP' DATAFRAME:\n")
    print('*** HEAD ***\n')
    print(reg_ref_df.head())
    print('\n*** TAIL ***\n')
    print(reg_ref_df.tail())



    ## Describe hospital data ##

    


    ## Fill nulls?

    # which columns contain nulls?

    #mask = df.isna()
    #mask.sum()

    #rows_to_fill = df.loc[mask.sum(axis=1) > 0]
