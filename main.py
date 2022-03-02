#=============================================================================
# Importation de la librairie
#=============================================================================

from tinydb import TinyDB, Query, where
import pandas as pd

from owslib.csw import CatalogueServiceWeb

#=============================================================================
# Connexion à un service CSW et inspection de ses propriétés
#=============================================================================

csw = CatalogueServiceWeb('http://www.geocat.ch/geonetwork/srv/fre/csw?')

# Affichage du type de service

# print(csw.identification.type)

# Affichage des opérations

# print([op.name for op in csw.operations])

#=============================================================================
# Affichage du type de résultat pris en charge
#=============================================================================

# csw.getdomain('GetRecords.resultType')

# print(csw.results)

#=============================================================================
# Recherche des métadonnées par mot-clé
#=============================================================================

from owslib.fes import PropertyIsEqualTo, PropertyIsLike, BBox

# Mot-clé de recherche

keyword = 'landcover'

# Requête

query = PropertyIsEqualTo('csw:AnyText', keyword)

# Cointraintes de recherche (10 par défaut)

csw.getrecords2(constraints=[query], maxrecords=50)

# Affichage des statistiques de recherche

# print(csw.results)

# Création d'un dataset avec les titres des services CSW et leur identifiant respectif

d = []
for rec in csw.records:
    d.append((csw.records[rec].title,csw.records[rec].identifier))

collection = pd.DataFrame(d, columns=('title', 'identifier'))
        
#=============================================================================
# Recherche d'une métadonnée en fonction de son id (GetRecordById)
# Recherche équivalente au résultat obtenu par l'url ci-dessous:
# https://www.geocat.ch/geonetwork/srv/fre/csw?request=GetRecordById&service=CSW&version=2.0.2&elementSetName=full&id=cfbd4793-4225-4743-942b-d9b97acfbfcc
#=============================================================================

# Sélection de l'identifiant

identifier = collection.iloc[2]['identifier']

records = pd.DataFrame(dir(csw.records[identifier]),columns = ['param'])

# Sous-échantillonnage du jeu de données avec les informations standards

records = records.loc[~records['param'].str.contains('__')]

# Mise à jour de l'indexation

records = records.reset_index(drop=True)

# Affichage des valeurs

# for i in range(0, len(records)):
#     print(getattr(csw.records[identifier], records.iloc[i]['param']))

# Enregistrement des valeurs dans le jeu de données

values = []
for i in range(0, len(records)):
    values.append(getattr(csw.records[identifier], records.iloc[i]['param']))
records['values'] = values

# Remarques

# Des différences notables sont constatées au niveau de la réponse de l'url et le traitement et owslib
# Cela est peu-être dû à l'implémentation de la lib

#=============================================================================
# Solution de contournement, lecture à partir d'une url à l'aide de pandas
#=============================================================================

url = 'https://www.geocat.ch/geonetwork/srv/fre/csw?request=GetRecordById&service=CSW&version=2.0.2&elementSetName=full&id=cfbd4793-4225-4743-942b-d9b97acfbfcc'

df_xml = pd.read_xml(url)

# Transposition du jeu de données (par souci de lisibilité)

df_xml = df_xml.T

# Remarques

# Nécessite la version 1.4.0 (ou ultérieure) de pandas
# Comme pressenti, toutes les informations ne sont pas remontées
# Consulter la doc et essayer de trouver une solution 

#%%
# =============================================================================
# Prochaines étapes:

# 0) Tester l'API GeoAdmin: http://api3.geo.admin.ch/services/sdiservices.html#layers-metadata
# 1) vérifier que les valeurs obtenues ci-dessous figurent bien dans de le dataset
# 2) parcourir la doc et remonter l'intégralité des informations via pd.read_xml 
# 3) intégrer le tout dans une db tinydb
# 4) publier un jupyternotebook dans la stac
# 5) identifier le problème au niveau d'owslib et de voir s'il est possible de contribuer
# 6) éventuellement publier un process sur pygeoapi
# 7) faire part idée analyse services csw geocat (stats records)
# =============================================================================

#%%
print(csw.records['cfbd4793-4225-4743-942b-d9b97acfbfcc'].abstract)

#%%==============================================

# Affichage du titre du jeu de données

print(csw.records[identifier].language)

#%%==============================================

print(csw.records['cfbd4793-4225-4743-942b-d9b97acfbfcc'].title)

# Affichage du résumé

print(csw.records['cfbd4793-4225-4743-942b-d9b97acfbfcc'].abstract)

# Affichage des mots-clés associés

print(csw.records['cfbd4793-4225-4743-942b-d9b97acfbfcc'].subjects)


# Affichage des informations relatives à l'auteur

print(csw.records['cfbd4793-4225-4743-942b-d9b97acfbfcc'].creator)


# Affichage des informations relatives aux contributeurs

print(csw.records['cfbd4793-4225-4743-942b-d9b97acfbfcc'].contributor)

# Affichage des informations relatives à la publication

print(csw.records['cfbd4793-4225-4743-942b-d9b97acfbfcc'].publisher)

# Affichage des informations relatives aux modifications

print(csw.records['cfbd4793-4225-4743-942b-d9b97acfbfcc'].modified)

# Affichage des informations relatives à la langue

print(csw.records['cfbd4793-4225-4743-942b-d9b97acfbfcc'].language)

# Affichage des informations relatives au format

print(csw.records['cfbd4793-4225-4743-942b-d9b97acfbfcc'].format)

# Affichage des informations relatives aux droits

print(csw.records['cfbd4793-4225-4743-942b-d9b97acfbfcc'].rights)

# Affichage des informations relatives à la bbox

print(csw.records['cfbd4793-4225-4743-942b-d9b97acfbfcc'].uris)


# Missing attributes:

# Bounding Box
# links

#%%==============================================









# spécification de la base de données

db = TinyDB('db.json')
# s111_metadata = TinyDB('s111_metadata.tinydb')

 
# df = pd.read_json('s111_metadata.tinydb')

df = pd.read_json('s111_metadata.tinydb')

row_1=df.iloc[0:1]


row_1_2 = df.head(2)


#%%


print(s111_metadata.all())

# sélection d'un item en fonction de son id




#%%==============================================








# création de la table

fruits = db.table('fruits')

# intégration de données

fruits.insert({'type':'fraise','quantite':4})
fruits.insert({'type':'orange','quantite':1})
fruits.insert({'type':'banane','quantite':7})

print(fruits.all())

#%%==============================================
# recherche sur les entités (items)
#%%==============================================

# Syntaxe longue

Fruit = Query()

fraise = fruits.search(Fruit.type == 'fraise')

print(fraise)

# Syntaxe courte

fraise = fruits.search(where('type') == 'fraise')

print(fraise)

#%%==============================================

# via l'id du document (tuple)

fraise = fruits.get(doc_id=1)
print(fraise)

#%%==============================================
# utilisation d'opérateurs logiques et de comparaison
#%%==============================================




