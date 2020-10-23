import pandas as pd
from datetime import datetime
from pathlib import Path

## Data sources

icu_source = 'https://drees.solidarites-sante.gouv.fr/etudes-et-statistiques/publications/article/nombre-de-lits-de-reanimation-de-soins-intensifs-et-de-soins-continus-en-france'

# FR regions

dept_url = "https://www.insee.fr/fr/statistiques/fichier/3720946/departement2019-csv.zip"
reg_url = "https://www.insee.fr/fr/statistiques/fichier/3720946/region2019-csv.zip"

icu_xls = "/data/drees_lits_reanimation_2013-2018.xlsx"

# FR population
pop_source: 'https://www.insee.fr/fr/statistiques/4265439?sommaire=4265511'
pop_age_source: 'https://www.insee.fr/fr/statistiques/1893198'

pop_csv = "data/Departements.csv"
pop_age_xls = "https://www.insee.fr/fr/statistiques/fichier/1893198/estim-pop-dep-sexe-aq-1975-2020.xls"

## Helper functions

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


def add_mayotte_pop(df, mayotte_pop=256518):
    '''manually fix Mayotte population, based on 2017 data.
    Source: https://fr.wikipedia.org/wiki/D%C3%A9mographie_de_Mayotte'''
    
    iloc = df.index[df['dep']=='976'][0]
    if pd.isnull(df.at[iloc, 'population']):
        df.at[iloc, 'population'] = mayotte_pop
        return df
    else:
        pass
    
def get_pop_age_data(xls=pop_age_xls):
    """Gets population by 5-year age brackets. Used
    for calculating incidence rate."""
    
    pop_age = pd.read_excel(xls, sheet_name="2020", skiprows=4, skipfooter=4)
    pop_age = pop_age[pop_age.columns[:22]]\
        .rename(columns={'Unnamed: 0':'dep',
                        'Unnamed: 1':'libelle_dep'})\
        .set_index(['dep', 'libelle_dep'])
        
    return pop_age

def create_pop_age_df():
    '''creates a population df from insee xls, then merges 5-year age groups
    to form 10-year age groups, to be used for incidence rate calculations.'''
    
    pop_age_df = get_pop_age_data()
    cols = pop_age_df.columns

    for i,v in enumerate(cols):
        if i%2==0:
            ag = pop_age_df.iloc[:,i:i+2]
            new_pop = ag.sum(axis=1)
            age = i*5
            new_pop.name = '-'.join([str(age), str(age+9)])
            pop_age_df = pop_age_df.join(new_pop)

    pop_age_df = pop_age_df.drop(cols, axis=1).rename(columns={'90-99':'90+'})
    
    return pop_age_df


def create_region_df():
    '''Combines all geo-based df's into one 'lookup' df.
    Used for adding more context to plots.'''
    
    reg_only_df = get_region_data()
    icu_df = get_icu_beds()
    pop_df = get_pop_data()
    
    
    reg_ref_df = reg_only_df.merge(icu_df, how='left').merge(pop_df, how='left')
    #cat_cols = reg_ref_df.columns[:4]
    #reg_ref_df[cat_cols] = reg_ref_df[cat_cols].apply(lambda x: pd.Categorical(x))
    
    reg_ref_df = add_mayotte_pop(df=reg_ref_df) # add missing population for Mayotte dept
    
    
    return reg_ref_df
def save_as_pkl(df):
    today = datetime.today().strftime("%m_%d_%Y")
    filename = "region_ref_{}.pkl".format(today)
    processed_file = Path.cwd().joinpath("data").joinpath("processed").joinpath(filename)
    df.to_pickle(processed_file)
    
    return processed_file
    
def generate_region_pkl():
    '''Creates French region df using relatively stable datasource. 
    Saved as pkl locally. Used to provide meaningful labels to plots,
    as well as more grouping options (by supplementing FR depts with regions).
    
    Returns:
        Path to .pkl'''
    
    reg_ref_df = create_region_df()
    processed_file = save_as_pkl(reg_ref_df)
    
    return processed_file

## Generate data

path_to_pkl = generate_region_pkl() 



if __name__=='__main__':
    print("A pkl of FR region codes & names, dept names, ICU_beds, & population has been generated here:/n")
    print(path_to_pkl)