#librairies indispensables : bokeh - pandas
from bokeh.io import curdoc, show
from bokeh.plotting import figure, output_file
from bokeh.transform import jitter, factor_cmap, dodge
from bokeh.models import (ColumnDataSource, Drag, PanTool, BoxZoomTool, LassoSelectTool,
                          WheelZoomTool, ResetTool, SaveTool, HoverTool, BoxSelectTool,
                          LinearColorMapper, BasicTicker, PrintfTickFormatter, ColorBar, )
from bokeh.models.widgets import MultiSelect, DateRangeSlider, Panel, Tabs, Button, Select
from bokeh.models.annotations import Span
from bokeh.palettes import Set3, Spectral6, RdYlGn4, Category10
from bokeh.layouts import column, row
from bokeh.models.glyphs import Text
from bokeh.tile_providers import STAMEN_TERRAIN
import pandas as pd
import sqlalchemy as sqlal
import numpy as np

# Utiliser la commande bokeh serve --show BokehServerHermes3.py pour
#lancer le serveur local et profiter des callbacks

# dir dans lequel sont installés les db sqlite3
SQLiteDir = "C:\\Users\\afontaine\\Documents\\Python\\SQLite\\"
HermesDB = "Hermes2018.db" #DB sqlite3 avec donnees des véhicules
MeldpuntenDB = "Meldpunten.db" #DB sqlite3 avec données th des pts d'emission
#SQLite => SQLAlchemy engine
hermes_engine = sqlal.create_engine('sqlite:///' + SQLiteDir + HermesDB)
meldpunten_engine = sqlal.create_engine('sqlite:///' + SQLiteDir + MeldpuntenDB)

# liste des b3s dans la base de donnée hermes
B3S_list=sqlal.inspect(hermes_engine).get_table_names()

#carrefour affiché en premier
CurrentB3S = 'B3S274'
StartDate = pd.Timestamp('2018-09-14 00:00:00')
FirstDate = '2017-01-01 00:00:00'
FirstDate = pd.Timestamp('2018-01-01 00:00:00')

def List_to_SQL(list):
    first = True
    sql_str = ''
    for i in list:
        if first:
            sql_str = str(i) + ')'
            first = False
        else:
            sql_str = str(i) + ',' + sql_str
    sql_str = '(' + sql_str
    return sql_str


def list_kiko(b3s):
    # Extrait en en liste et str les kikos de la db
    ListKiKo = pd.read_sql_query('SELECT DISTINCT KiKo FROM ' + b3s + ';', hermes_engine)
    ListKiKoStr = List_to_SQL(ListKiKo['KiKo'])
    ListKiKo = ListKiKo['KiKo'].tolist()
    ListKiKo = [str(i) for i in ListKiKo]
    return ListKiKoStr, ListKiKo


def list_lignes(b3s):
    # Extrait en en liste et str les numeros de lignes de la db
    ListLignes = pd.read_sql_query('SELECT DISTINCT NumLigne FROM ' + b3s + ';', hermes_engine)
    ListLignesStr = List_to_SQL(ListLignes['NumLigne'])
    ListLignes = ListLignes['NumLigne'].tolist()
    ListLignes = [str(i) for i in ListLignes]
    return ListLignesStr, ListLignes


def first_date(b3s):
    # Extrait en Timestamp la date la plus ancienne de la db
    FirstDate = pd.read_sql_query('SELECT min(Jour) FROM ' + b3s + ';', hermes_engine)
    FirstDate = list(FirstDate['min(Jour)'])
    # FirstDateStr = FirstDate[0]
    FirstDate = pd.Timestamp(FirstDate[0])
    return FirstDate


def last_date(b3s):
    # Extrait en Timestamp la date la plus récente de la db
    LastDate = pd.read_sql_query('SELECT max(Jour) FROM ' + b3s + ';', hermes_engine)
    LastDate = list(LastDate['max(Jour)'])
    LastDateStr = str(LastDate[0])
    LastDateTimestamp = pd.Timestamp(LastDate[0])
    return LastDateStr, LastDateTimestamp


def FormatDataFrame(DataFrame):
    # Formatte la DF pour qu'elle soit lisible
    DataFrame['JourDatetime'] = pd.to_datetime(DataFrame['Jour'], format='%Y-%m-%d %H:%M:%S')
    DataFrame['H_VA'] = pd.to_datetime(DataFrame['H_VA'], format='%H:%M:%S')
    DataFrame['H_AA'] = pd.to_datetime(DataFrame['H_AA'], format='%H:%M:%S')
    DataFrame['H_LF'] = pd.to_datetime(DataFrame['H_LF'], format='%H:%M:%S')
    DataFrame['H_AF'] = pd.to_datetime(DataFrame['H_AF'], format='%H:%M:%S')
    DataFrame['TpsVA_LF'] = DataFrame['H_LF'].sub(DataFrame['H_VA'])
    DataFrame['TpsVA_LF'] = DataFrame['TpsVA_LF'].dt.total_seconds()
    DataFrame = DataFrame.replace(0.0, np.NaN)
    return DataFrame


def SQLquery(b3s, KiKo, NumLigne, StartDate, EndDate):
    # Cree une requete SQL lisible par sqlite3
    ColumnInDF = """ Jour, NumLigne, H_VA, H_AA, H_LF, H_AF, KiKo, NumParc, WeekDay,
    TpsVA_AA, SpeedVA_AA, TpsAA_LF, SpeedAA_LF, TpsLF_AF, SpeedLF_AF, SpeedVA_AF,
    X_VA_merc, Y_VA_merc, X_AA_merc, Y_AA_merc, X_AF_merc, Y_AF_merc """
    RemoveFalseX = ' AND X_VA BETWEEN 4.2 AND 4.5 AND X_AA BETWEEN 4.2 AND 4.5 AND X_LF BETWEEN 4.2 AND 4.5 AND X_AF BETWEEN 4.2 AND 4.5'
    RemoveFalseY = ' AND Y_VA BETWEEN 50.2 AND 51 AND Y_AA BETWEEN 50.2 AND 51 AND Y_LF BETWEEN 50.2 AND 51 AND Y_AF BETWEEN 50.2 AND 51'
    RemoveNaN = ' AND SpeedVA_AF != 0.0 AND SpeedVA_AF < 80'
    query = 'SELECT' + ColumnInDF + 'FROM ' + b3s + ' WHERE KiKo IN ' + KiKo + ' AND NumLigne IN ' + NumLigne + ' AND Jour BETWEEN ' + '"' + StartDate + '"' + ' AND ' + '"' + EndDate + '"' + RemoveFalseX + RemoveFalseY + RemoveNaN + ';'
    DataFrame = pd.read_sql(query, hermes_engine)
    DataFrame = FormatDataFrame(DataFrame)
    return DataFrame


def Meldpuntenquery(b3s, KiKo, NumLigne):
    # Cree une requete pour SQL pour extraire une DF depuis sql
    ColumnInDF = """ KiKo, NumLigne, Type_Message, Priorite, Sens, Type_Gestion, Power, X_effectif_webmerc,
    Y_effectif_webmerc, TpsTheoriques """
    query = 'SELECT' + ColumnInDF + 'FROM ' + b3s + ' WHERE KiKo IN ' + KiKo + ' AND NumLigne IN ' + NumLigne + ';'
    Dataframe = pd.read_sql_query(query, meldpunten_engine)
    # Dataframe['TpsTheoriques'] = Dataframe.apply(lambda row: float(row['TpsTheorique']), axis=1)
    return Dataframe


# Recupère les valeurs de base pour travailler sur la db
ListKiKoStr, ListKiKo = list_kiko(CurrentB3S)
ListlignesStr, ListLignes = list_lignes(CurrentB3S)
LastDateStr, LastDateTimestamp = last_date(CurrentB3S)

# cree un Column Data Source lisible par bokeh et utilisee par les graphes
StartDict = dict(Jour=[], NumLigne=[], H_VA=[], H_AA=[], H_LF=[], H_AF=[], KiKo=[], NumParc=[], WeekDay=[], TpsVA_LF=[],
                 TpsLF_AF=[], SpeedLF_AF=[], SpeedVA_AF=[], X_VA_merc=[], Y_VA_merc=[], X_AA_merc=[], Y_AA_merc=[]
                 , X_AF_merc=[], Y_AF_merc=[], JourDatetime=[])

events_sources = ColumnDataSource(data=StartDict)

StartDictMeldpunten = dict(KiKo=[], NumLigne=[], Type_Message=[], Priorite=[], Sens=[], Type_Gestion=[], Power=[],
                           X_effectif_webmerc=[], Y_effectif_webmerc=[], TpsTheorique=[])

meldpunten_source = ColumnDataSource(data=StartDictMeldpunten)

# cree les curseurs utilisees avec les valeurs par defaut
# B3S à Selectionner
B3SSelect = Select(title="Carrefour", value=CurrentB3S, options=B3S_list, width=200)
# Dates pour les points:
DeltaDates = DateRangeSlider(title="Dates a prendre en compte", start=FirstDate, end=LastDateTimestamp,
                             value=(StartDate, LastDateTimestamp), step=1, width=450)

# KiKos
multi_select_KiKo = MultiSelect(title="KiKos a selectionner", value=ListKiKo, options=ListKiKo, width=200)
# Lignes
multi_select_Lignes = MultiSelect(title="Lignes a selectionner", value=ListLignes, options=ListLignes, width=200)
# Bouton pour faire l'update
button_update = Button(label="Changer les dates", width=100)

# GRAPHE DES DONNEES

# cree un graphe scatter avec tout les elements repartis par heure et jour de la semaine
DAYS = ['Dim', 'Sam', 'Ven', 'Jeu', 'Mer', 'Mar', 'Lun']
hover = HoverTool(tooltips=[("Jour", "@Jour"), ("Ligne", "@NumLigne"), ("Véhicule", "@NumParc"), ("KiKo", "@KiKo"),
                            ("Vitesse", "@SpeedVA_AF")])
TOOLS = [PanTool(), BoxZoomTool(), LassoSelectTool(), BoxSelectTool(), WheelZoomTool(), ResetTool(), SaveTool(),
         hover]
Jitter = figure(plot_width=560, plot_height=370, y_range=DAYS, x_axis_type='datetime', tools=TOOLS,
                output_backend="webgl")

Jitter.xaxis[0].formatter.days = ['%Hh']
Jitter.x_range.range_padding = 0
Jitter.ygrid.grid_line_color = None
tab_points = Panel(child=Jitter, title="Données")

# données du graphe scatter avec tout les elements repartis par heure et jour de la semaine
CircleChart = Jitter.circle(x='H_VA', y=jitter('WeekDay', width=0.8, range=Jitter.y_range), size=3, legend="KiKo",
                            color=factor_cmap('KiKo', palette=Category10[10], factors=ListKiKo), source=events_sources,
                            alpha=0.8, hover_color='gold')

# GRAPHE DES VITESSES MOYENNES

# initialise les données
VitessesStartDict = dict(heure=[], Jour=[], rate=[])
VitesseSource = ColumnDataSource(data=VitessesStartDict)


# fonction pour extraire les vitesses depuis la DataFrame
def vitesses_mediannes(df):
    DAYS = ['Lun', 'Mar', 'Mer', 'Jeu', 'Ven', 'Sam', 'Dim']
    d = {'heure': [], 'Lun': [], 'Mar': [], 'Mer': [], 'Jeu': [], 'Ven': [], 'Sam': [], 'Dim': []}
    heure1 = pd.to_datetime('1900-01-01 00:00:00')
    for i in range(48):
        templist = []
        heure2 = heure1 + pd.Timedelta(minutes=30)
        for day in DAYS:
            d[day].append(df['SpeedVA_AF'][(df['H_VA'] > heure1) & (df['H_VA'] < heure2) & (df['WeekDay'] == day)].mean(
                skipna=True))
        d['heure'].append(heure1 + pd.Timedelta(minutes=15))
        heure1 = heure2
    df = pd.DataFrame(data=d)
    return df


def vitesse_df(Dataframe):
    VitesseDF = vitesses_mediannes(Dataframe)
    VitesseDF = VitesseDF.set_index('heure')
    VitesseDF.columns.name = 'Jour'
    VitesseSource = pd.DataFrame(VitesseDF.stack(), columns=['rate']).reset_index()
    return VitesseSource


# cree un graphe avec les vitesses mediannes par demi-heure
colors = ["#550b1d", "#550b1d", "#933b41", "#cc7878", "#ddb7b1", "#dfccce", '#e2e2e2', "#c9d9d3", "#a5bab7",
          "#75968f", "#577C74", "#577C74", '#33514B', '#33514B', "#1F3631", "#1F3631"]
mapper = LinearColorMapper(palette=colors, low=0, high=40)
TOOLS_vitesse = "hover,save,pan,box_zoom,reset,wheel_zoom"
Vitesses = figure(plot_width=597, plot_height=370, x_axis_type='datetime', y_range=DAYS, tools=TOOLS_vitesse)
Vitesses.grid.grid_line_color = None
Vitesses.axis.major_tick_line_color = None
Vitesses.xaxis[0].formatter.days = ['%Hh']
Vitesses.x_range.range_padding = 0

# barre de légende
bar = ColorBar(color_mapper=mapper, location=(0, -10), major_label_text_font_size="5pt", width=5, height=335)
Vitesses.add_layout(bar, 'right')
tab_vitesses = Panel(child=Vitesses, title="Vitesses Moyennes")
Vitesses.select_one(HoverTool).tooltips = [('vitesse moyenne', '@rate'), ]

# données du graphe de vitesses
VitessesChart = Vitesses.rect(x='heure', y='Jour', width=1800000, height=1, source=VitesseSource,
                              fill_color={'field': 'rate', 'transform': mapper}, line_color=None)
# CARTE

# cree une carte avec les points
TOOLS3 = [PanTool(), BoxZoomTool(), LassoSelectTool(), BoxSelectTool(), WheelZoomTool(),
          ResetTool(), SaveTool(), hover]
map = figure(plot_width=600, plot_height=600, tools=TOOLS3, match_aspect=True, aspect_scale=1.0)
map.add_tile(STAMEN_TERRAIN)
map.axis.visible = False

# données de la carte
MapAF = map.circle(x='X_AF_merc', y='Y_AF_merc', size=4, alpha=0.05, color='green', source=events_sources,
                   hover_color='gold')
MapVA = map.circle(x='X_VA_merc', y='Y_VA_merc', size=4, alpha=0.05, color='red', source=events_sources,
                   hover_color='gold')
MapAA = map.circle(x='X_AA_merc', y='Y_AA_merc', size=4, alpha=0.05, color='blue', source=events_sources,
                   hover_color='gold')

colormap = ['darkred', 'darkblue', 'darkgreen']
messages = ['VA', 'AA', 'AF']
MapTh = map.square(x='X_effectif_webmerc', y='Y_effectif_webmerc', size=10, source=meldpunten_source,
                   color=factor_cmap('Type_Message', palette=colormap, factors=messages))
# HISTOGRAMMES

# cree un histogramme avec les valeurs de temps de parcours
TOOLS2 = [PanTool(), BoxZoomTool(), WheelZoomTool(), ResetTool(), SaveTool(), HoverTool()]
hist = figure(title="Distribution temps de parcours pour le " + CurrentB3S, plot_width=1200, plot_height=400,
              tools=TOOLS2, x_range=(0, 120), y_range=(0,0.5))
hist.xaxis.axis_label = 'temps en secondes'

# initialise les données des histogrammes

histVA_LF, edgesVA_LF = np.histogram([0, 120], density=True, bins=range(0, 121))
histAA_LF, edgesAA_LF = np.histogram([0, 120], density=True, bins=range(0, 121))
histLF_AF, edgesLF_AF = np.histogram([0, 120], density=True, bins=range(0, 121))

HistLF_AF = hist.quad(top=histLF_AF, bottom=0, left=edgesLF_AF[:-1], right=edgesLF_AF[1:], fill_color='yellowgreen',
                      fill_alpha=0.5, line_color='darkgreen')
HistVA_LF = hist.quad(top=histVA_LF, bottom=0, left=edgesVA_LF[:-1], right=edgesVA_LF[1:], fill_color='coral',
                      fill_alpha=0.5, line_color='red')
HistAA_LF = hist.quad(top=histAA_LF, bottom=0, left=edgesAA_LF[:-1], right=edgesAA_LF[1:], fill_color='steelblue',
                      fill_alpha=0.5, line_color='navy')

# donnees theoriques du tableau

TempsTheoriques = dict(TpsTheoriques=[], TpsTheoriques2=[])
Spans = hist.quad(top=0.5, bottom=0, left='TpsTheorique', right='TpsTheorique', source=meldpunten_source,
                  color=factor_cmap('Type_Message', palette=colormap, factors=messages))
BoxTh = hist.quad(top=0.5, bottom=0.4, left='TpsTheorique', right='TpsTheorique', source=meldpunten_source,
                  color=factor_cmap('Type_Message', palette=colormap, factors=messages))

# Initialise le tableau avec les donnees des histogrammes dans l'histogramme des temps de parcours

GlyphSource = ColumnDataSource(dict(x=[60, 80, 90, 100], y=[0.1, 0.1, 0.1, 0.1],
                                    text=["", "", "", ""], colors=['black', 'coral', 'steelblue', 'yellowgreen']))
glyph = Text(x='x', y='y', text='text', text_font_size='12pt', text_baseline='bottom', text_color='colors',
             text_font_style='bold')
hist.add_glyph(GlyphSource, glyph)

def update_database():
    # mise a jour des valeurs et de la nouvelle DataFrame

    # Reset des valeurs de la requete
    newb3s = B3SSelect.value
    newKiKo = multi_select_KiKo.value
    newKiKo = List_to_SQL(newKiKo)
    newNumLigne = multi_select_Lignes.value
    newNumLigne = List_to_SQL(newNumLigne)
    newStartDate = DeltaDates.value_as_datetime[0].strftime('%Y-%m-%d %H:%M:%S')
    newEndDate = DeltaDates.value_as_datetime[1].strftime('%Y-%m-%d %H:%M:%S')

    # mise a jour des Datasource des donnees vehicules pour la carte et le graphes des données
    NewDataFrame = SQLquery(newb3s, newKiKo, newNumLigne, newStartDate, newEndDate)
    events_sources.data = dict(Jour=NewDataFrame['Jour'], NumLigne=NewDataFrame['NumLigne'],
                               H_VA=NewDataFrame['H_VA'], H_AA=NewDataFrame['H_AA'],
                               H_LF=NewDataFrame['H_LF'], H_AF=NewDataFrame['H_AF'],
                               KiKo=NewDataFrame['KiKo'], NumParc=NewDataFrame['NumParc'],
                               WeekDay=NewDataFrame['WeekDay'],
                               TpsVA_LF=NewDataFrame['TpsVA_LF'], TpsLF_AF=NewDataFrame['TpsLF_AF'],
                               SpeedLF_AF=NewDataFrame['SpeedLF_AF'], SpeedVA_AF=NewDataFrame['SpeedVA_AF'],
                               X_VA_merc=NewDataFrame['X_VA_merc'], Y_VA_merc=NewDataFrame['Y_VA_merc'],
                               X_AA_merc=NewDataFrame['X_AA_merc'], Y_AA_merc=NewDataFrame['Y_AA_merc'],
                               X_AF_merc=NewDataFrame['X_AF_merc'], Y_AF_merc=NewDataFrame['Y_AF_merc'],
                               JourDatetime=NewDataFrame['JourDatetime'])

    # mise a jour des Datasource des donnees theoriques pour la carte
    NewMeldpuntenDataFrame = Meldpuntenquery(newb3s, newKiKo, newNumLigne)
    meldpunten_source.data = dict(KiKo=NewMeldpuntenDataFrame['KiKo'],
                                  NumLigne=NewMeldpuntenDataFrame['NumLigne'],
                                  Type_Message=NewMeldpuntenDataFrame['Type_Message'],
                                  Priorite=NewMeldpuntenDataFrame['Priorite'],
                                  Sens=NewMeldpuntenDataFrame['Sens'],
                                  Type_Gestion=NewMeldpuntenDataFrame['Type_Gestion'],
                                  Power=NewMeldpuntenDataFrame['Power'],
                                  X_effectif_webmerc=NewMeldpuntenDataFrame['X_effectif_webmerc'],
                                  Y_effectif_webmerc=NewMeldpuntenDataFrame['Y_effectif_webmerc'],
                                  TpsTheorique=NewMeldpuntenDataFrame['TpsTheoriques'])

    # Nouvelle Source pour le graphe des vitesses
    vitesse_NewDF = vitesse_df(NewDataFrame)
    VitesseSource.data = dict(heure=vitesse_NewDF['heure'],
                              Jour=vitesse_NewDF['Jour'],
                              rate=vitesse_NewDF['rate'])

    # données de l'histogramme avec les valeurs de temps de parcours
    histVA_LF, edgesVA_LF = np.histogram(NewDataFrame['TpsVA_LF'], density=True, bins=range(0, 121))
    histAA_LF, edgesAA_LF = np.histogram(NewDataFrame['TpsAA_LF'], density=True, bins=range(0, 121))
    histLF_AF, edgesLF_AF = np.histogram(NewDataFrame['TpsLF_AF'], density=True, bins=range(0, 121))

    # mise a jour des histogrammes

    HistVA_LF.data_source.data["top"] = histVA_LF
    HistAA_LF.data_source.data["top"] = histAA_LF
    HistLF_AF.data_source.data["top"] = histLF_AF

    # Mise a jour du Tableau avec les donnees des histogrammes dans l'histogramme des temps de parcours
    label_text_lignes = ("Moyenne = \nMediane = \nEcart-type = ")
    label_text_VA_LF = ("VA_LF \n" + str(NewDataFrame['TpsVA_LF'].mean())[:6] + '\n' + str(
        NewDataFrame['TpsVA_LF'].median())[:6] + '\n' + str(NewDataFrame['TpsVA_LF'].std())[:6])
    label_text_AA_LF = ("AA_LF \n" + str(NewDataFrame['TpsAA_LF'].mean())[:6] + '\n' + str(
        NewDataFrame['TpsAA_LF'].median())[:6] + '\n' + str(NewDataFrame['TpsAA_LF'].std())[:6])
    label_text_LF_AF = ("LF_AF \n" + str(NewDataFrame['TpsLF_AF'].mean())[:6] + '\n' + str(
        NewDataFrame['TpsLF_AF'].median())[:6] + '\n' + str(NewDataFrame['TpsLF_AF'].std())[:6])

    GlyphSource.data["text"] = [label_text_lignes, label_text_VA_LF, label_text_AA_LF, label_text_LF_AF]
    #conn.commit()


def update_b3s():
    # Si la requete se fait sur le b3s
    newb3s = B3SSelect.value

    # Changements des KiKos
    NewListKiKoStr, NewListKiKo = list_kiko(newb3s)

    multi_select_KiKo.value = NewListKiKo
    multi_select_KiKo.options = NewListKiKo

    # Changement des Lignes
    NewListlignesStr, NewListLignes = list_lignes(newb3s)
    multi_select_Lignes.value = NewListLignes
    multi_select_Lignes.options = NewListLignes

    update_database()


controls = [multi_select_KiKo, multi_select_Lignes]
for control in controls:
    control.on_change('value', lambda attr, old, new: update_database())
button_update.on_click(update_database)

B3SSelect.on_change('value', lambda attr, old, new: update_b3s())

# mise en page initiale
TabsDays = Tabs(tabs=[tab_vitesses, tab_points])

layout = column(
    row(column(row(B3SSelect, multi_select_KiKo, multi_select_Lignes), row(DeltaDates, button_update), TabsDays),
        map), row(hist))

# initialise les données
update_database()
output_file('Hermes3.html')
curdoc().add_root(layout)

# show(layout) a utiliser sans "bokeh serve"
