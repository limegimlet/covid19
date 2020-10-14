import numpy as np
import pandas as pd
from datetime import datetime

# to display offline interactive plots
import plotly as py
#from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
#init_notebook_mode(connected=True)

import plotly.graph_objects as go

# set default plotly theme
import plotly.io as pio
pio.templates.default = "plotly_white"

# library for mapping
import plotly.express as px
from urllib.request import urlopen
import json

# hide annoying repeated deprec warnings (statsmodel issue)
import warnings
warnings.simplefilter('once', category=UserWarning)

# load regional 'lookup' df
#import process_region_data as rd

import process_test_data as pt
import process_hosp_data as hd

### Data source urls ###

source = 'https://www.data.gouv.fr/fr/datasets/donnees-relatives-aux-resultats-des-tests-virologiques-covid-19/'
url = "https://www.data.gouv.fr/fr/datasets/r/406c6a23-e283-4300-9484-54e78c8ae675"
meta_url = "https://www.data.gouv.fr/fr/datasets/r/39aaad1c-9aac-4be8-96b2-6d001f892b34"

### Geojson for FR depts and regions ###

dept_geos = 'https://static.data.gouv.fr/resources/carte-des-departements-2-1/20191202-212236/contour-des-departements.geojson'
region_geos = 'https://france-geojson.gregoiredavid.fr/repo/regions.geojson'

with urlopen(dept_geos) as response:
    fr_dept = json.load(response)

with urlopen(region_geos) as response:
    fr_region = json.load(response)

### Helper functions ###

# create df for latest data only

def get_latest(col, df):
    latest_date = df['jour'].loc[df[col].notna()].max()
    latest_df = df.query('jour==@latest_date').drop('jour', axis=1)
    latest_df.name = latest_date
    return latest_df


# for scorecards

def create_cat(col, ranges, labels):
    bins = pd.IntervalIndex.from_tuples(ranges, closed='both')
    newcol = pd.cut(col, bins, include_lowest=True).replace(bins, labels)
    return newcol


def assign_alert_level(kpi_df):
    
    cols_to_score = ['incid_tous', 'incid_70+', 'rea%']

    # set ranges for different indicators
    incid_ranges = [(0, 9.999), (10, 49.999), (50, 149.999), (150, 249.999), (250, 9999.999)]
    incid70_ranges = [(0, 49.999), (50, 99.999), (100, 9999.999)]
    icu_ranges = [(0, 29.999), (30, 59.999), (60, 9999.999)]

    # assign alert levels

    incid_alerts = ['OK', 'Vigilance', 'Alerte', 'Alerte renforcée', 'Alerte maximale']
    incid70_alerts = ['OK', 'Alerte renforcée', 'Alerte maximale']
    icu_alerts = ['OK', 'Alerte maximale', 'État d’urgence sanitaire']

    # create a col to label alert level for each indicator
    ranges = [incid_ranges, incid70_ranges, icu_ranges]
    ranges_dict = dict(zip(cols_to_score, ranges))

    alerts = [incid_alerts, incid70_alerts, icu_alerts]
    alert_dict = dict(zip(cols_to_score, alerts))
    
    for col in cols_to_score:
        alert = create_cat(kpi_df[col], ranges_dict[col], alert_dict[col])
        kpi_df = kpi_df.join(alert, rsuffix='_alerte')
    
    return kpi_df.reset_index()

def assign_overall_alert(row):
    #if row['incid_tous_alerte'] == np.isnan():
    if pd.isnull(row['incid_tous']):
        status=np.nan      
    elif (row['incid_tous'] > 250. and row['incid_70+'] >100. and row['rea%'] >60.):
        status = "État d'urgence sanitaire"
    elif (row['incid_tous'] > 250. and row['incid_70+'] >100. and row['rea%'] >30.):
        status = "Alerte maximale"
    elif (row['incid_tous'] > 150. and row['incid_70+'] >50.):
        status = "Alerte renforcée"
    elif row['incid_tous'] > 50.:
        status = "Alerte"
    elif (row['incid_tous'] <=50. and row['incid_70+'] <=50. and row['rea%'] <=30.):
        status = "OK"
    else:
        status = "Vigilance"
    ## previous logic: too alarmist
    #elif row['rea%_alerte'] != 'OK':
     #   status = row['rea%_alerte']
    #elif row['incid_70+_alerte'] != 'OK':
     #   status = row['incid_70+_alerte']
    #else:
     #   status = row['incid_tous_alerte']
    
    
    return status

def save_df(df, fmt):
    latest_date = df['jour'].loc[df['niveau_global'].notnull()].max()
    latest_date = latest_date.replace("-","_")
    fname = 'kpi_{}.{}'.format(latest_date, fmt)
    path = 'data/{}'.format(fname)
    
    if fmt=='pkl':
        df.to_pickle(path)
        print('Saved to {}'.format(path))
    elif fmt=='csv':
        df.to_csv(path, index=False)
        print('Saved to {}'.format(path))
    else:
        print("Unrecognized format. Enter 'csv' or 'pkl'")
        

def create_kpi_df(rea_level='reg'):
    '''Adds "incid_70" and "rea%"" to main df, creates alert str columns for
    all 3 indicators, as well as a "niveau_global" column. The returned
    df is used by all indicator maps.'''
    
    # create covid testing df
    df = pt.create_testing_df()
    df = pt.create_rolling_cols(df)

    # Get incidence rate for 70+
    older_incid = pt.calc_older_incid()
    incid70 = older_incid['70+'].reset_index()

    # Get ICU % saturation
    rea_df = hd.create_rea_df(rea_level)
    rea_df['rea%'] = pt.to_percent(rea_df['rea'], rea_df['ICU_beds'])
    rea_pct = rea_df['rea%'].reset_index()

    # add to testing df
    kpi_list = [incid70, rea_pct]
    for kpi in kpi_list:
        df = df.merge(kpi, how='outer')

    # streamline df
    keep_cols = ['libelle_dep','jour', 'dom_tom', 'rolling_pos_100k', '70+', 'rea%']
    kpi_df = df[keep_cols]
    kpi_df.columns = ['libelle_dep', 'jour','dom_tom', 'incid_tous', 'incid_70+', 'rea%']
    kpi_df = kpi_df.set_index(['libelle_dep', 'jour'], drop=True)
    
    # create new cols for alert labels
    #kpi_df = assign_alert_level(kpi_df)
    kpi_df['niveau_global'] = kpi_df.apply(assign_overall_alert, axis=1)
    kpi_df['niveau_global'] = pd.Categorical(kpi_df['niveau_global'], 
                                             categories=['OK', 
                                                         'Vigilance', 
                                                         'Alerte', 
                                                         'Alerte renforcée',
                                                         'Alerte maximale',
                                                         'État d’urgence sanitaire'], 
                                            ordered=True)

    
    kpi_df = kpi_df.sort_values(['libelle_dep', 'jour']).reset_index()
    
    for fmt in ['csv','pkl']:
        save_df(kpi_df, fmt)
        
    return kpi_df

## Redundant??
def get_geojson(dept_geos, region_geos):

    with urlopen(dept_geos) as response:
        fr_dept = json.load(response)

    with urlopen(region_geos) as response:
        fr_region = json.load(response)
        
    return fr_dept, fr_region


def create_overview_df(kpi_df):
    '''Concatenates kpi value columns into one string, then adds
    this as a new column 'valeurs'. Then reduces kpi_df to 3 columns.
    Used as the dataframe for mapping "niveau global"'''
    
    ov_df = get_latest('niveau_global', kpi_df)
    date = ov_df.name
    val_cols = ['incid_tous', 'incid_70+', 'rea%']
    
    # make hover text
    ov_vals_long = ov_df.set_index('libelle_dep')[val_cols].stack()
    ov_str = ov_vals_long.round(0).astype('int').astype('str').reset_index(1)
    ov_lines = ov_str.apply(" : ".join, axis=1).reset_index() # join columns
    hover_text = ov_lines.groupby('libelle_dep')[0].apply(', '.join).reset_index() # join rows
    
    # reduce cols in returned df
    hover_text.columns = ['libelle_dep', 'valeurs']
    ov_df = ov_df.merge(hover_text)
    #ov_cols = ['libelle_dep', 'niveau_global', 'valeurs']
    #ov_df = ov_df.copy()[ov_cols]
    
    return ov_df, date
    #return hover_text


# verify
#ov_df, latest_date = create_overview_df(kpi_df)
#ov_df.tail()


def make_overview_colormap(kpi_df):
    all_alerts = kpi_df['niveau_global'].cat.categories
    reds = px.colors.sequential.Reds
    palette = [red for i, red in enumerate(reds) if i % 2 == 0]
    palette.append('rgb(0,0,0)')
    colormap = dict(zip(all_alerts, palette))

    return colormap


### Map functions ###

colormap = {'OK': 'rgb(255,245,240)',
 'Vigilance': 'rgb(252,187,161)',
 'Alerte': 'rgb(251,106,74)',
 'Alerte renforcée': 'rgb(203,24,29)',
 'Alerte maximale': 'rgb(103,0,13)',
 'État d’urgence sanitaire': 'rgb(0,0,0)'}

# scatterplot of major FR cities

def add_cities():
    villes_df = pd.read_csv("data/villes.csv")
    trace = go.Scattergeo(
            lon = villes_df['lon'],
            lat = villes_df['lat'],
            #showlegend = False,
            text = villes_df['Libelle'],
            hoverinfo = 'text',
            mode='markers',
            marker_color='skyblue',
            name="Major cities")
    
    return trace

def make_overview_map(map_col, date, latest_df, source=source, colormap=colormap):
    '''Plots a discrete-scale choropleth for Covid alert levels in France.'''
    
    #map_df, date = create_overview_df(kpi_df)
    #map_df = get_latest('niveau_global', kpi_df)
    #date = map_df.name
    title = 'Alert levels on {}<br>(hover for more info)'.format(date)
    source_str = source_str = "Source: <a href='{}' color='blue'>Santé Public France</a>".format(source)
    
    fig = px.choropleth(latest_df, geojson=fr_dept, color=map_col,
                    locations="libelle_dep", featureidkey="properties.nom",
                    #animation_frame="variable", animation_group='libelle_dep',
                    hover_name='libelle_dep',  hover_data={#'valeurs':True,
                                                           'libelle_dep': False,
                                                  'incid_tous':':.0f',
                                                  'incid_70+': ':.0f',
                                                  'rea%': ':.0f'},
                    color_discrete_map=colormap,
                    category_orders={map_col: list(colormap.keys())},
                    projection="mercator")

                
    fig.update_geos(fitbounds="locations", visible=False, resolution=50)
    

    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":20},
                 title={'text': title,
                        'y':.97,
                        'x':0.15,
                        'xanchor': 'left',
                        'yanchor': 'top'},
                  legend_title_text='Levels<br>(click to hide/show)',
                  legend_title_font_size=14,
                  legend=dict(yanchor="top", # so legend doesn't block hover menu
                              y=0.9,
                              xanchor="left",
                              x=0.825),
                  annotations=[dict(x= 1, 
                                   y= 0, 
                                   text = source_str, 
                                   showarrow = False,
                                   xref='paper', 
                                   yref='paper',
                                   xanchor='right', 
                                   yanchor='auto')],
                                   #xshift=0, 
                                   #yshift=0)]
                 )
    
    return fig

def make_value_map(map_col, df, source=source):
    '''Plots a continuous-scale choropleth for Covid alert indicators in France.'''
    
    # var used in map function
    map_df = get_latest(map_col, df)
    date = map_df.name # for title
    title = '{} - {}'.format(str.capitalize(map_col).replace("_"," "), date)    
    source_str = source_str = "Source: <a href='{}' color='blue'>Santé Public France</a>".format(source) # for annotation
    #alert_col = '{}_alerte'.format(map_col) # for hover text
    
    fig = px.choropleth(map_df, geojson=fr_dept, color=map_col,
                    locations="libelle_dep", featureidkey="properties.nom",
                    #animation_frame="variable", animation_group='libelle_dep',
                    hover_name='libelle_dep',  hover_data={#alert_col: True,
                                                           'libelle_dep': False},
                    color_continuous_scale="Reds",
                    #range_color=[min(metric_score), max(metric_score)],
                    projection="mercator")
    
    #fig.add_traces(cities)
                    
    fig.update_geos(fitbounds="locations", visible=False, resolution=50)

    fig.update_layout(margin=dict(r=0, t=0, l=0, b=20),
                     title=dict(text=title, y=.98, x=0.2, xanchor='left', yanchor='top'),
                     annotations=[dict(x= 1, y= 0, 
                                       text=source_str, showarrow = False,
                                       xref='paper', yref='paper',
                                       xanchor='right', yanchor='auto')])
    
    return fig


def map_rea(map_col, df, source=hd.source):
    '''Plots a continuous-scale choropleth for ICU saturation (in %) in France.
    Max range value = 60%, which is the threshold for "état d'urgence sanitaire"
    (when incidence rate > 250 & elderly incidence rate > 100 are also reached).'''
    
    map_df = get_latest(map_col, df)
    date = map_df.name # for title
    
    title = '{} - {}'.format(str.capitalize(map_col).replace("_"," "), date)    
    source_str = source_str = "Source: <a href='{}' color='blue'>Santé Public France</a>".format(source) # for annotation
    #alert_col = '{}_alerte'.format(map_col) # for hover text
    
    fig = px.choropleth(map_df, geojson=fr_dept, color=map_col,
                    locations="libelle_dep", featureidkey="properties.nom",
                    #animation_frame="variable", animation_group='libelle_dep',
                    hover_name='libelle_dep',  hover_data={#alert_col: True,
                                                           'libelle_dep': False},
                    color_continuous_scale="Reds",
                    range_color=[0,60],
                    projection="mercator")
    
                    
    fig.update_geos(fitbounds="locations", visible=False, resolution=50)
    
    #fig.add_traces(cities)

    fig.update_layout(margin=dict(r=0, t=0, l=0, b=20),
                     title=dict(text=title, y=.98, x=0.2, xanchor='left', yanchor='top'),
                     annotations=[dict(x= 1, y= 0, 
                                       text=source_str, showarrow = False,
                                       xref='paper', yref='paper',
                                       xanchor='right', yanchor='auto')])
    
    return fig



def to_html(fig, fname, auto_open=True):
    filepath = "../covid_dataviz/{}".format(fname)
    pio.write_html(fig, file=filepath, auto_open=auto_open)
    print("Map saved to {}".format(filepath))
    
### KPI lineplot functions ###

def make_kpi_long_df(kpi_df):
    val_cols = ['incid_tous', 'incid_70+', 'rea%']
    index_cols = ['libelle_reg', 'reg', 'libelle_dep', 'jour']
    kpi_long = pd.DataFrame(kpi_df.set_index(index_cols)[val_cols].stack(dropna=False)).reset_index()
    kpi_long.columns = ['libelle_reg', 'reg', 'libelle_dep', 'jour','indicator', 'value']
    
    return kpi_long

def plot_reg_kpi(reg, df):
    regname = reg_ref_df['libelle_reg'].loc[reg_ref_df['reg']==reg].unique()[0]
    fig = px.line(df.query('reg==@reg').dropna(), x='jour', y='value', 
                  color='libelle_dep',
                  title="{} alert indicators over time<br>(Hover for more info)".format(regname),
                  hover_name='libelle_dep',
                  hover_data={'libelle_dep': False},
                  category_orders={'indicator':['incid_tous', 'incid_70+', 'rea%']},
                  color_discrete_sequence=px.colors.qualitative.D3,
                  facet_row='indicator',
                  facet_row_spacing=.05,
                  render_mode='svg',
                  height=900)
    
    return fig

def output_reg_plots(reglist, df, auto_open=False):
    kpi_long_df = make_kpi_long_df(df)
    for reg in reglist:
        regname = reg_ref_df['libelle_reg'].loc[reg_ref_df['reg']==reg].unique()[0]
        fig = plot_reg_kpi(reg, kpi_long_df)

        # save to html
        fname = "kpi_{}.html".format(reg)
        path = "../covid_dataviz/{}".format(str.lower(fname))
        kpi.to_html(fig, path, auto_open=auto_open)
        
def output_reg_iframes(reglist):        
    for reg in reglist:
        regname = reg_ref_df['libelle_reg'].loc[reg_ref_df['reg']==reg].unique()[0]
        reg_heading = "# {}".format(regname)

        fname = "kpi_{}.html".format(reg)
        url_str = "https://limegimlet.github.io/covid_dataviz/{}".format(fname)
        iframe_str = '<iframe id="igraph" scrolling="no" style="border:none;" seamless="seamless" src="{}" height="1000" width="100%"></iframe>'.format(url_str)
        iframe = '{% raw %}' + iframe_str + '{% endraw %}'

        print(reg_heading + '\n')
        print(iframe + '\n')
    

if __name__ == '__main__':
    
   
    print("Creating KPI dataframe...")
    kpi_df = create_kpi_df()
    colormap = make_overview_colormap(kpi_df)
    metric = 'niveau_global'
    latest_df = get_latest(metric, kpi_df)
    latest_date = latest_df.name
    
    print("Generating FR map...")
    q = "dom_tom=='False'"
    fig = make_overview_map(metric, latest_date, latest_df.query(q))
     
    # add cities to FR maps
    #print("Adding cities...")
    cities_trace = add_cities()
    fig.add_traces(cities_trace)
    
    # save alert level map as html
    print("Saving to HTML...")
    fname = 'alerts.html'
    path = '../covid_dataviz/{}'.format(fname)
    to_html(fig, path, False)
    
    # save IDF alert level map as html
    reg = 11
    reg_ref_df = pt.rd.create_region_df()
    idf = reg_ref_df['libelle_dep'].loc[reg_ref_df['reg']==reg].values
    fname = 'alerts_idf.html'
    path = '../covid_dataviz/{}'.format(fname)
    
    print("\nGenerating IDF map...")
    q = "libelle_dep in @idf"
    idf_fig = make_overview_map(metric, latest_date, latest_df.query(q))
    
    print("Saving to HTML...")
    to_html(idf_fig, path, False)
    
    print("Done!\n")
    
    #reg_list = reg_ref_df.query('reg > 10')['reg'].unique()
    #

