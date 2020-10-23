# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
from datetime import datetime

# to display offline interactive plots
import plotly as py
#from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
#init_notebook_mode(connected=True)
import cufflinks as cf
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

#curfew_cities = ['Paris', 'Rouen', 'Marseille', 'Lyon', 'Montpellier', 'Saint-Etienne', 'Montpellier']
#metro_df_wide.reset_index('class_age')[curfew_cities]

### Data source urls ###

source = 'https://www.data.gouv.fr/fr/datasets/donnees-relatives-aux-resultats-des-tests-virologiques-covid-19/'
url = "https://www.data.gouv.fr/fr/datasets/r/406c6a23-e283-4300-9484-54e78c8ae675"
meta_url = "https://www.data.gouv.fr/fr/datasets/r/39aaad1c-9aac-4be8-96b2-6d001f892b34"

metro_rea = 'https://www.data.gouv.fr/fr/datasets/r/62ec32ae-6b3e-4e4a-b81f-eeb4e8759a4d'

# AT LAST!!!!!!!!!
#metropoles_src = ''
rea_metro_src = 'https://www.data.gouv.fr/en/datasets/indicateurs-de-lactivite-epidemique-part-des-patients-covid-19-dans-les-reanimations/'


### Geojson for FR depts and regions ###

dept_geos = 'https://static.data.gouv.fr/resources/carte-des-departements-2-1/20191202-212236/contour-des-departements.geojson'
region_geos = 'https://france-geojson.gregoiredavid.fr/repo/regions.geojson'
commune_geos = 'https://public.opendatasoft.com/explore/dataset/geoflar-communes-2013/download/?format=geojson&timezone=Europe/Berlin&lang=en'

epci_communes = 'https://www.data.gouv.fr/fr/datasets/contours-des-epci-2015/'

# geo csv
villes = 'https://public.opendatasoft.com/explore/dataset/code-insee-postaux-geoflar/download/?format=csv&timezone=Europe/Berlin&lang=en&use_labels_for_header=true&csv_separator=%3B'
curfew_cities = "data/fr_curf_cities.pkl"


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
    #if pd.isnull(row['incid_tous']) or isnull('incid_70+') or isnull(row['rea%']):
    if pd.isnull(row[['incid_tous', 'incid_70+', 'rea%']]).any():
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
        
    # backfill rea% - for cases like Oct 15 missing data from rea only
    # doing it before creating 'niveau global' ensures all 3 kpi are used for alert label
    df['rea%'].fillna(method='pad', inplace=True)

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
    
    #for fmt in ['csv','pkl']:
     #   save_df(kpi_df, fmt)
        
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

def add_curfew_cities(path=curfew_cities):
    
    fr_curf_cities = pd.read_pickle(path)
    trace = go.Scattergeo(
        lon = fr_curf_cities['lon'],
        lat = fr_curf_cities['lat'],
        text = fr_curf_cities['hovertext'],
        hoverinfo = 'text',
        mode='markers',
        marker=dict(color='yellow',
                    size=7,
                    line=dict(width=2,
                              color='aqua')),
        name='Oct 17: Under curfew')
    
    return trace
    

def make_overview_map(map_col, date, latest_df, source=source, colormap=colormap):
    '''Plots a discrete-scale choropleth for Covid alert levels in France.'''
    
    #map_df, date = create_overview_df(kpi_df)
    #map_df = get_latest('niveau_global', kpi_df)
    #date = map_df.name
    title = 'Alert levels on {}<br>(hover for more info)'.format(date)
    source_str = "Source: <a href='{}' color='blue'>Santé Public France</a>".format(source) # for annotation
        
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

                
    fig.update_geos(fitbounds="locations", visible=False)
    

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

def make_value_map(map_col, date, latest_df, source=source):
    '''Plots a continuous-scale choropleth for Covid alert indicators in France.'''
    
    # var used in map function
    #map_df = get_latest(map_col, df)
    #date = map_df.name # for title
    title = '{} - {}'.format(str.capitalize(map_col).replace("_"," "), date)    
    source_str = "Source: <a href='{}' color='blue'>Santé Public France</a>".format(source) # for annotation
    #alert_col = '{}_alerte'.format(map_col) # for hover text
    
    fig = px.choropleth(latest_df, geojson=fr_dept, color=map_col,
                    locations="libelle_dep", featureidkey="properties.nom",
                    #animation_frame="variable", animation_group='libelle_dep',
                    hover_name='libelle_dep',  hover_data={#alert_col: True,
                                                           'libelle_dep': False},
                    color_continuous_scale="Reds",
                    range_color=[min(metric_score), max(metric_score)],
                    projection="mercator")
    
    #fig.add_traces(cities)
                    
    fig.update_geos(fitbounds="locations", visible=False)

    fig.update_layout(margin=dict(r=0, t=0, l=0, b=20),
                     title=dict(text=title, y=.98, x=0.2, xanchor='left', yanchor='top'),
                     annotations=[dict(x= 1, y= 0, 
                                       text=source_str, showarrow = False,
                                       xref='paper', yref='paper',
                                       xanchor='right', yanchor='auto')])
    
    return fig


def map_rea(map_col, date, latest_df, source=hd.source):
    '''Plots a continuous-scale choropleth for ICU saturation (in %) in France.
    Max range value = 60%, which is the threshold for "état d'urgence sanitaire"
    (when incidence rate > 250 & elderly incidence rate > 100 are also reached).'''
    
    #map_df = get_latest(map_col, df)
    #date = map_df.name # for title
    
    title = 'Rea % saturation - {}'.format(date)
    source_str = source_str = "Source: <a href='{}' color='blue'>Santé Publique France</a>".format(source) # for annotation
    #alert_col = '{}_alerte'.format(map_col) # for hover text
    
    fig = px.choropleth(latest_df, geojson=fr_dept, color=map_col,
                    locations="libelle_dep", featureidkey="properties.nom",
                    #animation_frame="variable", animation_group='libelle_dep',
                    hover_name='libelle_dep',  hover_data={#alert_col: True,
                                                           'libelle_dep': False},
                    color_continuous_scale="Reds",
                    range_color=[0,60],
                    projection="mercator")
    
                    
    fig.update_geos(fitbounds="locations", visible=False)
    
    #fig.add_traces(cities)

    fig.update_layout(margin=dict(r=0, t=0, l=0, b=20),
                     title=dict(text=title, y=.98, x=0.2, xanchor='left', yanchor='top'),
                     annotations=[dict(x= 1, y= 0, 
                                       text=source_str, showarrow = False,
                                       xref='paper', yref='paper',
                                       xanchor='right', yanchor='auto')])
    
    return fig

### For first page of covid_dataviz ###


def create_kpi_summary(df):
    val_cols = ['incid_tous', 'incid_70+', 'rea%']
    q = "dom_tom=='False'" # still missing rea values for domtom
    kpi_fr_df = df.query(q).groupby('jour')[val_cols].mean().round(0)
    
    return kpi_fr_df



def plot_kpi_trends(kpi_fr_df):
    '''Plots daily ttls for alert indicator for all of France.'''
    
    #rea_hlines = [dict(y=30, color='darkgreen', width=1.5, dash='dash')]
     #            #dict(y=60, color='darkred')]
    
    latest_date = kpi_fr_df.index.max()
    #hline_annot = [{'text':'30% ICU occupancy','y':'30', 'x':'2020-07-02',
                    #'textangle':0,'ay':-10}]
    
    fig = kpi_fr_df.iplot(title="<b>Covid-19 indicator trends - France</b>",
                          kind='bar',
                          #hline=rea_hlines,
                          #annotations=hline_annot,
                          asFigure=True)
    
    line_trace = go.Scatter(x=[kpi_fr_df.index.min(), kpi_fr_df.index.max()], 
                            y=[30,30],  # on oct 15, macron said we need to keep new ICU admissions to < 200
                            mode='lines', 
                            line_color='green',
                            line_dash='dashdot',
                            name="<br>\nICU % capacity threshold<br>for Alerte maximale")

    fig.add_traces(line_trace)


    fig.update_layout(legend_title_text="Click to hide/show,<br>double-click to show only:",
                  legend_title_font_size=13)
    
    return fig

# get NEW reanimations

def get_new_admissions(url=hd.new_patients_url):
    '''Creates dataframe of *new* ICU patients & patient deaths in hospital.
    Used on covid_dataviz home page, as a companion plot to kpi_trends .'''
    
    new_admissions = pd.read_csv(url, sep=';', dtype=dict(dep='str')) 
    new_admissions = new_admissions.groupby('jour').sum().rolling(7).mean().round(0) # rolling 7d avg, no decimals
    cols = new_admissions.columns
    newcols = [str.split(col, "_")[1] for col in cols]
    new_admissions.columns = newcols
    
    return new_admissions

def plot_rea_dc(kpi_fr_df, palette=hd.hosp_colormap):
    #kpi_fr_df = kpi.create_kpi_summary(kpi_df)
    cols = ['rea', 'dc']
    new_ad = get_new_admissions()
    rea_dc_df = kpi_fr_df.join(new_ad)

    fig = rea_dc_df[cols].iplot(
            kind='bar',
            colors=palette,
            title="<b>Patients admitted to ICU vs. died in hospital - France</b>\n", 
            asFigure=True)
    
    return fig


def to_html(fname, fig, auto_open=False):
    filepath = "../covid_dataviz/{}".format(fname)
    pio.write_html(fig, filepath, auto_open=False, include_plotlyjs='cdn')
    print("Map saved to {}".format(filepath))
    
### KPI by region lineplots ###

hline_dict = dict(incid_tous=[dict(y=50, color='red', dash='dash'),
                               dict(y=150,color='darkred', dash='dash'),
                               dict(y=250,color='maroon', dash='dash')],
                    incid_70=[dict(y=50,color='red', dash='dash'),
                                  dict(y=100,color='darkred', dash='dash')],
                    rea=[dict(y=30,color='darkred', dash='dash'),
                            dict(y=60,color='black', dash='dash')])



def plot_reg_kpi(metric, df):

    title="<b>{}</b><br>(Use legend to hide/show regions)".format(metric)
    icu_hlines = [dict(y=30, color='darkred', dash='dash'),
                  dict(y=60, color='black', dash='dash')]
    
    q = "dom_tom=='False'" # no icu numbers for DOM
    plot_df = df.query(q).dropna().groupby(['libelle_reg', 'jour'])[metric].mean().unstack(0)
    
    if metric=='incid_70+': # workaround special char in col names
        hl_key = 'incid_70'
    elif metric=='rea%':
        hl_key = 'rea'
    else:
        hl_key = 'incid_tous'
        
    fig = plot_df.iplot(asFigure=True,
               hline=hline_dict[hl_key],
               title=title)

    fig.update_layout(legend_title_text="Click to hide/show,<br>double-click to show only:",
                      legend_title_font_size=13)
    
    return fig

def output_reg_kpi(kpi_df):
    for metric in ['incid_tous', 'incid_70+', 'rea%']:

        fig = plot_reg_kpi(metric, kpi_df)
        if metric=='rea%':
            metric='rea'
        elif metric=='incid_70+':
            metric='incid_70'
        else:
            pass
        fname = "kpi_{}_by_reg.html".format(metric)
        to_html(fname, fig)
        
### KPI lineplot functions  - for regional breakdown pg of covid_dataviz###

def make_kpi_long_df(kpi_df):
    val_cols = ['incid_tous', 'incid_70+', 'rea%']
    index_cols = ['libelle_reg', 'reg', 'libelle_dep', 'jour']
    kpi_long = pd.DataFrame(kpi_df.set_index(index_cols)[val_cols].stack(dropna=False)).reset_index()
    kpi_long.columns = ['libelle_reg', 'reg', 'libelle_dep', 'jour','indicator', 'value']
    
    return kpi_long  

def plot_reg_dept_kpi(reg, df):
    '''Compares indicator lines by department & by indicator for a given region. 
    Used on regional breakdown page of covid_dataviz.'''
    
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

def output_reg_dept_plots(reglist, df):
    kpi_long_df = make_kpi_long_df(df)
    for reg in reglist:
        #regname = reg_ref_df['libelle_reg'].loc[reg_ref_df['reg']==reg].unique()[0]
        fig = plot_reg_dept_kpi(reg, kpi_long_df)

        # save to html
        fname = "kpi_{}.html".format(reg)
        #path = "../covid_dataviz/{}".format(str.lower(fname))
        to_html(fname, fig)
        
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
    
    ## Alert choropleth
    print("Generating FR map...")
    q = "dom_tom=='False'"
    fig = make_overview_map(metric, latest_date, latest_df.query(q))
     
    # add cities to FR maps
    print("Adding cities...")
    #cities_trace = add_cities()
    curfew_trace = add_curfew_cities(curfew_cities)
    fig.add_traces(curfew_trace)
    
    print("Saving to HTML...")
    fname = 'alerts.html'
    to_html(fname, fig, False)
    
    ## Indicator trendline barplot
    print("Generating FR indicator trends plot...")
    kpi_fr_df = create_kpi_summary(kpi_df)
    fig = plot_kpi_trends( kpi_fr_df)
  
    print("Saving to HTML...")
    fname="kpi_fr_trends.html"
    to_html(fname, fig, auto_open=False)
    
    ## ICU vs Deaths in hosp barplot
    print("Generating FR ICU admisssions & deaths plot...")
    fig = plot_rea_dc(kpi_fr_df)
    fname='kpi_rea_dc_trends.html'
    to_html(fname, fig, auto_open=False)
    

    # add regions to kpi_df
    reg_ref_df = pt.rd.create_region_df()
    new_kpi_df = reg_ref_df[['reg', 'libelle_reg', 'libelle_dep']].merge(kpi_df).sort_values(['reg', 'jour'])
    reglist = [11, 24, 27, 28, 32, 44, 52, 53, 75, 76, 84, 93, 94]
    
    ## Indicators by region line plots      
    print("Generating KPI by region plots...")
    output_reg_kpi(new_kpi_df)
    
    ## for Compare dept page
    print("Generating KPI by dept plots...")
    output_reg_dept_plots(reglist, new_kpi_df)
    
    
    print("****** DONE! ******\n")
    
