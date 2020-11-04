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

### Main data source urls ###

source = 'https://www.data.gouv.fr/fr/datasets/donnees-relatives-aux-resultats-des-tests-virologiques-covid-19/'
url = "https://www.data.gouv.fr/fr/datasets/r/406c6a23-e283-4300-9484-54e78c8ae675"
meta_url = "https://www.data.gouv.fr/fr/datasets/r/39aaad1c-9aac-4be8-96b2-6d001f892b34"

metro_rea = 'https://www.data.gouv.fr/fr/datasets/r/62ec32ae-6b3e-4e4a-b81f-eeb4e8759a4d'

# AT LAST!!!!!!!!!
#metropoles_src = ''
rea_metro_src = 'https://www.data.gouv.fr/en/datasets/indicateurs-de-lactivite-epidemique-part-des-patients-covid-19-dans-les-reanimations/'


### Geojson for FR depts and regions ###

dept_geos = 'https://static.data.gouv.fr/resources/carte-des-departements-2-1/20191202-212236/contour-des-departements.geojson'
#region_geos = 'https://france-geojson.gregoiredavid.fr/repo/regions.geojson'
region_geos='https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/regions-version-simplifiee.geojson'
commune_geos = 'https://public.opendatasoft.com/explore/dataset/geoflar-communes-2013/download/?format=geojson&timezone=Europe/Berlin&lang=en'

#epci_communes = 'https://www.data.gouv.fr/fr/datasets/contours-des-epci-2015/'

# geo csv
villes = 'https://public.opendatasoft.com/explore/dataset/code-insee-postaux-geoflar/download/?format=csv&timezone=Europe/Berlin&lang=en&use_labels_for_header=true&csv_separator=%3B'
curfew_cities = "data/fr_curf_cities.pkl"


with urlopen(dept_geos) as response:
    fr_dept = json.load(response)

with urlopen(region_geos) as response:
    fr_region = json.load(response)


### Global variables

# Get lookup info locally

path = 'data/'

# boilperplate textangle

boilerplate_fr = dict(main_subtitle="Faire glisser le curseur pour l'infobulle",
                   legend_subtitle="Cliquer pour <br>masquer ou afficher")

# reg_ref_df = pd.read_pickle(path + 'reg_ref_df.pkl')


### Helper functions ###

# create df for latest data only

def get_latest(col, df):
    latest_date = df['jour'].loc[df[col].notna()].max()
    latest_df = df.query('jour==@latest_date').drop('jour', axis=1)

    ## no longer relevant - DELETE?
    #if latest_date > '2020-10-14': # when curfews started
    #    curfew_df = pd.read_pickle('data/curfew_depts.pkl')
    #    latest_df = latest_df.merge(curfew_df[['libelle_dep','under_curfew']], how='outer')
    #    latest_df.loc[latest_df['under_curfew']==True, 'niveau_global'] = 'Couvre-feu'

    latest_df.name = latest_date
    return latest_df

# for scorecards

def create_cat(col, ranges, labels):
    bins = pd.IntervalIndex.from_tuples(ranges, closed='both')
    newcol = pd.cut(col, bins, include_lowest=True).replace(bins, labels)
    return newcol


def assign_alert_level(kpi_df): ## Obsolete now - to remove???

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
    #elif (row['under_curfew'] and row['jour'] > '2020-10-14'):
    #    return "Couvre-feu"
    elif (row['incid_tous'] > 250. and row['incid_70+'] >100. and row['rea%'] >60.0):
        return "État urgence sanitaire"
    elif (row['incid_tous'] > 250. and row['incid_70+'] >100. and row['rea%'] >30.0):
        return  "Alerte maximale"
    elif (row['incid_tous'] > 150. and row['incid_70+'] >50.):
        return "Alerte renforcée"
    elif row['incid_tous'] > 50.:
        return "Alerte"
    elif (row['incid_tous'] <=50. and row['incid_70+'] <=50. and row['rea%'] <=30.0):
        return "OK"
    else:
        return "Vigilance"
    ## previous logic: too alarmist
    #elif row['rea%_alerte'] != 'OK':
     #   status = row['rea%_alerte']
    #elif row['incid_70+_alerte'] != 'OK':
     #   status = row['incid_70+_alerte']
    #else:
     #   status = row['incid_tous_alerte']


def save_df(df, fmt):
    latest_date = df['jour'].loc[df['niveau_global'].notnull()].max()
    latest_date = latest_date.replace("-","_")
    fname = 'latest_kpi.{}'.format(fmt)
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
    df['reg'] = df['reg'].astype('int')

    # Get incidence rate for 70+
    older_incid = pt.calc_older_incid()
    incid70 = older_incid['70+'].reset_index()

    # Get ICU % saturation by region
    rea_df = hd.create_rea_df('reg')
    rea_df['rea%'] = pt.to_percent(rea_df['rea'], rea_df['ICU_beds'])
    rea_pct = rea_df[['reg', 'libelle_reg','rea%']].reset_index()

    # to help clarify curfew decisions,
    # include ICU % saturation by department as well
    dep_rea_df = hd.create_rea_df('dep')
    dep_rea_df['rea%_dep'] = pt.to_percent(dep_rea_df['rea'], dep_rea_df['ICU_beds'])
    dep_rea_pct = dep_rea_df['rea%_dep'].reset_index()

    # add to testing df
    kpi_list = [rea_pct, dep_rea_pct, incid70]
    for kpi in kpi_list:
        df = df.merge(kpi, how='outer')

    # backfill rea% - for cases like Oct 15 missing data from rea only
    # doing this before creating 'niveau global' ensures
    # all 3 kpi are used for the alert label
    df['rea%'].fillna(method='pad', inplace=True)

    # streamline df - TODO: test keeping these after all
    keep_cols = ['reg','libelle_reg','libelle_dep','jour', 'dom_tom', 'rolling_pos_100k',
                '70+', 'rea%', 'rea%_dep']

    kpi_df = df[keep_cols]
    kpi_df.columns = ['reg','libelle_reg','libelle_dep', 'jour','dom_tom', 'incid_tous', 'incid_70+',
                        'rea%', 'rea%_dep']
    kpi_df = kpi_df.set_index(['reg','libelle_reg','libelle_dep', 'jour'], drop=True)

    # create new cols for alert labels
    #kpi_df = assign_alert_level(kpi_df)
    kpi_df['niveau_global'] = kpi_df.apply(assign_overall_alert, axis=1)
    kpi_df['niveau_global'] = pd.Categorical(kpi_df['niveau_global'],
                                             categories=['OK',
                                                         'Vigilance',
                                                         'Alerte',
                                                         'Alerte renforcée',
                                                         'Alerte maximale',
                                                         'État urgence sanitaire',
                                                         'Couvre-feu'],
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



def make_overview_colormap(kpi_df):
    all_alerts = kpi_df['niveau_global'].cat.categories
    reds = px.colors.sequential.Reds
    palette = [red for i, red in enumerate(reds) if i % 2 == 0]
    palette.append('rgb(0,0,0)') # for Etat d'urgence
    palette.append('rgg(101, 67, 33)') # dark brown for curfew
    colormap = dict(zip(all_alerts, palette))

    return colormap


### Map functions ###

# for mapping alerts
colormap = {'OK': 'rgb(255,245,240)',
 'Vigilance': 'rgb(252,187,161)',
 'Alerte': 'rgb(251,106,74)',
 'Alerte renforcée': 'rgb(203,24,29)',
 'Alerte maximale': 'rgb(103,0,13)',
 'État urgence sanitaire': 'rgb(37,37,37)',
 'Couvre-feu': 'rgb(102, 101, 101)'}

label_trans = {#'rea%': '% occup. réa (rég)',
               #'rea%_dep': '% occup. réa (dép)',
               'libelle_reg': 'région',
               'libelle_dep': 'département',
               'niveau_global': 'niveau'}

# for adding scatterplot trace of curfew cities over alert map
def add_cities(): # TODO: merge with function below
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
    title = "<b>Niveaux d'alerte  - {}</b><br>({})".format(date, boilerplate_fr['main_subtitle'])
    source_str = "Source: <a href='{}' color='blue'>Santé Publique France</a>".format(source) # for annotation

    # modify dataframe to display more readableß hovertext
    plot_df = latest_df.round(0)
    plot_df['hovername'] = plot_df['libelle_dep'] + " (" + plot_df['libelle_reg'] + ")"
    plot_df['rea%'] = plot_df['rea%'].divide(100)

    fig = px.choropleth(plot_df, geojson=fr_dept, color=map_col,
                    locations="libelle_dep", featureidkey="properties.nom",
                    #animation_frame="variable", animation_group='libelle_dep',
                    hover_name='hovername',  hover_data={
                                                           'libelle_dep': False,
                                                           'incid_tous':':.0f',
                                                           'incid_70+': ':.0f',
                                                           'rea%': ':.0%'},
                    locationmode='geojson-id',
                    labels=label_trans,
                    color_discrete_map=colormap,
                    category_orders={map_col: list(colormap.keys())},
                    projection="mercator")


    fig.update_geos(fitbounds="locations",
                    visible=False,
                    countrycolor='rgb(255,255,255)')


    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":20},
                 title={'text': title,
                        'y':.97,
                        'x':0.10,
                        'xanchor': 'left',
                        'yanchor': 'top'},
                hoverlabel=dict(
                        bgcolor="white",
                        font_size=12,
                        font_family="Rockwell"),
                  legend_title_text='Niveaux<br>({})'.format(boilerplate_fr['legend_subtitle']),
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


def map_rea(map_col, date, latest_df, source=hd.source, labels=boilerplate_fr):
    '''Plots a continuous-scale choropleth for ICU saturation (in %) in France.
    Max range value = 60%, which is the threshold for "état d'urgence sanitaire"
    (when incidence rate > 250 & elderly incidence rate > 100 are also reached).'''

    latest_df['reg'] = latest_df['reg'].astype('str')
    latest_df = latest_df.round(0)

    # arg vals depend on whether rea% based on dep or region
    if map_col=='rea%_dep':
        title = '<b> - {}</b>'.format(date)
        color_range = [0,100]
        feat_id = 'properties.nom'
        geo = fr_dept
        loc = 'libelle_dep'
        hovername = 'libelle_dep'
    elif map_col=='rea%':
        color_range = [0,60]
        feat_id = 'properties.code'
        geo = fr_region
        loc = 'reg'
        hovername = 'libelle_reg'
    else:
        print("Not a valid map column. Use rea% or rea%_dep.")

    source_str = "Source: <a href='{}' color='blue'>Santé Publique France</a>".format(source) # for annotation
    title = '<b>{} - {}</b><br>({})'.format(map_col, date, boilerplate_fr['main_subtitle'])

    fig = px.choropleth(latest_df, geojson=geo, color=map_col,
                    locations=loc, featureidkey=feat_id,
                    #animation_frame="variable", animation_group='libelle_dep',
                    hover_name=hovername,
                    hover_data={'reg': False,
                                'libelle_reg':False,
                                'libelle_dep': False},
                    labels=label_trans,

                    color_continuous_scale="Reds",
                    range_color=color_range,
                    projection="mercator")

    fig.update_geos(fitbounds="locations",
                    visible=False)

    fig.update_layout(margin=dict(r=0, t=0, l=0, b=20),
                     title=dict(text=title, y=.95, x=0.05, xanchor='left', yanchor='top'),
                     hoverlabel=dict(bgcolor="white",
                                    font_size=12,
                                    font_family="Rockwell"),
                     # source at bottom
                     annotations=[dict(x= 1, y= 0,
                                       text=source_str, showarrow = False,
                                       xref='paper', yref='paper',
                                       xanchor='right', yanchor='auto')],
                     )

    fig.update_layout(coloraxis_colorbar=dict(
                                        title=None,
                                        thicknessmode="pixels", thickness=25,
                                        lenmode="fraction", len=.6,
                                        yanchor="top", y=.8,
                                        ticks="outside", ticksuffix="%",
                                    ))


    return fig

### For first page of covid_dataviz ###


def create_kpi_summary(df):
    val_cols = ['incid_tous', 'incid_70+', 'rea%']
    q = "dom_tom=='False'" # still missing rea values for domtom
    kpi_fr_df = df.query(q).groupby('jour')[val_cols].mean().round(0)

    return kpi_fr_df



def plot_kpi_trends(kpi_fr_df):
    '''Plots daily AVG for alert indicator for all of France.'''

    #rea_hlines = [dict(y=30, color='darkgreen', width=1.5, dash='dash')]
     #            #dict(y=60, color='darkred')]

    latest_date = kpi_fr_df.index.max()
    #hline_annot = [{'text':'30% ICU occupancy','y':'30', 'x':'2020-07-02',
                    #'textangle':0,'ay':-10}]

    fig = kpi_fr_df.iplot(title="<b>Covid-19 indicator trends - France</b><br>(daily avg of all depts)",
                          kind='bar',
                          #hline=rea_hlines,
                          #annotations=hline_annot,
                          asFigure=True)

    max_trace = go.Scatter(x=[kpi_fr_df.index.min(), kpi_fr_df.index.max()],
                            y=[30,30],
                            mode='lines',
                            line_color='green',
                            line_dash='dot',
                            name="<br>rea% saturation threshold<br>for Alerte maximale")

    urg_trace = go.Scatter(x=[kpi_fr_df.index.min(), kpi_fr_df.index.max()],
                            y=[60,60],
                            mode='lines',
                            line_color='green',
                            line_dash='dashdot',
                            name="<br>rea% saturation threshold<br>for Etat urgence<br>sanitaire")

    fig.add_traces([max_trace, urg_trace])


    fig.update_layout(legend_title_text="Click to hide/show,<br>double-click to show only:",
                  legend_title_font_size=13)

    return fig

# get NEW reanimations

def get_new_admissions(geo='fr', url=hd.new_patients_url):
    '''Creates dataframe of *new* ICU patients & patient deaths in hospital.
    Used on covid_dataviz home page, as a companion plot to kpi_trends .'''

    new_admissions = pd.read_csv(url, sep=';', dtype=dict(dep='str'))
    if geo=='fr':
        new_admissions = new_admissions.groupby('jour').sum().rolling(7).mean().round(0) # rolling 7d avg, no decimals
    elif geo=='dep':
        new_admissions = new_admissions.groupby(['dep', 'jour']).sum().rolling(7).mean().round(0) # rolling 7d avg, no decimals
    else:
        print("not a valid value. Use 'fr' or 'dep' instead.")
    cols = new_admissions.columns
    newcols = [str.split(col, "_")[1] for col in cols]
    new_admissions.columns = newcols

    return new_admissions

def plot_rea_dc(kpi_fr_df, palette=hd.hosp_colormap):
    cols = ['rea', 'dc']
    new_ad = get_new_admissions().dropna()
    rea_dc_df = kpi_fr_df.join(new_ad, how='right')

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
    val_cols = ['incid_tous', 'incid_70+', 'rea%', 'rea%_dep']
    index_cols = ['libelle_reg', 'reg', 'libelle_dep', 'jour']
    kpi_long = pd.DataFrame(kpi_df.set_index(index_cols)[val_cols].stack(dropna=False)).reset_index()
    kpi_long.columns = ['libelle_reg', 'reg', 'libelle_dep', 'jour','indicator', 'value']

    return kpi_long

def plot_reg_dept_kpi(reg, df):
    '''Compares indicator lines by department & by indicator for a given region.
    Used on regional breakdown page of covid_dataviz.'''

    reg_ref_df = pd.read_pickle('data/reg_ref_df.pkl')
    regname = reg_ref_df['libelle_reg'].loc[reg_ref_df['reg']==reg].unique()[0]
    fig = px.line(df.query("reg==@reg & indicator !='rea%'").dropna(), x='jour', y='value',
                  color='libelle_dep',
                  title="Covid indicators - {}".format(regname),
                  hover_name='libelle_dep',
                  hover_data={'libelle_dep': False},
                  category_orders={'indicator':['incid_tous', 'incid_70+', 'rea%_dep']},
                  color_discrete_sequence=px.colors.qualitative.D3,
                  facet_row='indicator',
                  facet_row_spacing=.05,
                  render_mode='svg',
                  height=900)

    return fig


def output_reg_dept_plots(reglist, df):
    kpi_long_df = make_kpi_long_df(df)

    # define the traces for threshold lines
    #TODO: turn into for lookup

    x_min = kpi_long_df['jour'].min()
    x_max = kpi_long_df['jour'].max()

    incid_tous_trace2 = go.Scatter(x=[x_min, x_max],
                            y=[250, 250] ,
                            mode='lines',
                            line_color='darkred',
                            line_dash='dot',
                            showlegend=False)

    incid_tous_trace = go.Scatter(x=[x_min, x_max],
                            y=[150, 150],
                            mode='lines',
                            line_color='red',
                            line_dash='dot',
                            showlegend=False)

    incid_70_trace2 = go.Scatter(x=[x_min, x_max],
                            y=[100,100],
                            mode='lines',
                            line_color='darkred',
                            line_dash='dot',
                            showlegend=False)

    incid_70_trace = go.Scatter(x=[x_min, x_max],
                            y=[50,50],
                            mode='lines',
                            line_color='red',
                            line_dash='dot',
                            showlegend=False)

    rea_max_trace = go.Scatter(x=[x_min, x_max],
                            y=[30,30],
                            mode='lines',
                            line_color='darkred',
                            line_dash='dot',
                            showlegend=False)

    rea_urg_trace = go.Scatter(x=[x_min, x_max],
                            y=[60,60],
                            mode='lines',
                            line_color='black',
                            line_dash='dot',
                            showlegend=False)
    # generate plots
    for reg in reglist:
        fig = plot_reg_dept_kpi(reg, kpi_long_df)

        # add alert threshold lines
        fig.add_trace(incid_tous_trace, row=3, col=1)
        fig.add_trace(incid_tous_trace2, row=3, col=1)
        fig.add_trace(incid_70_trace, row=2, col=1)
        fig.add_trace(incid_70_trace2, row=2, col=1)
        fig.add_trace(rea_max_trace, row=1, col=1)
        fig.add_trace(rea_urg_trace, row=1, col=1)

        # make labels easier to read
        fig.update_yaxes(matches=None)
        fig.update_xaxes(title=None)
        fig.update_yaxes(title=dict(text='cases per 100k pop.', font_size=12))
        fig.update_yaxes(title='% occupied', row=1, col=1)
        fig.for_each_annotation(lambda a: a.update(textangle=.5, x=.5, yshift=120,
                                                   text=(a.text.split("=")[-1]),
                                                    font_size=13))

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
    #print("Adding cities...")
    #cities_trace = add_cities()
    #curfew_trace = add_curfew_cities(curfew_cities)
    #fig.add_traces(curfew_trace)

    #print("Saving to HTML...")
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

    # add regions to kpi_df - REDUNDANT???
    #reg_ref_df = pt.rd.create_region_df()
    #new_kpi_df = reg_ref_df[['reg', 'libelle_reg', 'libelle_dep']].merge(kpi_df).sort_values(['reg', 'jour'])

    ## Indicators by region line plots
    print("Generating KPI by region plots...")
    #output_reg_kpi(new_kpi_df)
    output_reg_kpi(kpi_df)

    ## for Compare dept page
    print("Generating KPI by dept plots...")
    reglist = [11, 24, 27, 28, 32, 44, 52, 53, 75, 76, 84, 93, 94]
    #output_reg_dept_plots(reglist, new_kpi_df)
    output_reg_dept_plots(reglist, kpi_df)

    ## rea% choropleth maps for Overvew page
    print("Generating region rea% maps...")
    map_col = 'rea%'
    latest_rea = get_latest(map_col, kpi_df)
    latest_rea_date = latest_rea.name
    rea_fig = map_rea(map_col, latest_rea_date, latest_rea)

    fname = "rea_pct_region.html"
    to_html(fname, rea_fig)

    print("Generating dept rea% maps...")
    map_col = 'rea%_dep'
    dep_rea_fig = map_rea(map_col, latest_rea_date, latest_rea)

    fname = "rea_pct_dept.html"
    to_html(fname, dep_rea_fig)

    print("****** DONE! ******\n")
