import re
import uuid
from numpy import shares_memory
from numpy.lib.function_base import append
import pandas as pd
from pandas import tseries

#PASO 1: LEER ARCHIVOS CSV y borrar las columnas uuid;id_user;gclid del archivo navegacion.csv

def dataset_navegacion():
    navegacion = pd.read_csv("navegacion.csv", sep = ";")
    # borrar las columnas con drop uuid;id_user;gclid del archivo navegacion.csv
    navegacion = navegacion.drop(['uuid', 'id_user', 'gclid'], axis = 1)
    return navegacion
print(dataset_navegacion())

def dataset_conversiones():
    conversiones = pd.read_csv("conversiones.csv", sep = ";")
    return conversiones
print(dataset_conversiones())


#PASO 2: FILTRAR DATOS DE LA URL DEL DATASET DE NAVEGACION, PARA OBTENER LOS DATOS DE LA CAMPAÑA, ADGROUP, ADVERTISEMENT, SITELINK 

def filtrar_url(navegacion):
    campaña=[]
    adgroup=[]
    advertisement=[]
    sitelink=[]
    ide_user1=[]
    gclid1=[]
    uuid1=[]
    urls = navegacion['url_landing'] #columna de url_landing
    for url in urls:
        try:
            sep= str(url).split("camp=")
            bueno = sep[1].split("&")
            campaña.append(bueno[0])
        except:
            campaña.append(0)
    for url in urls:
        try:
            sep= str(url).split("adg=")
            bueno = sep[1].split("&")
            adgroup.append(bueno[0])
        except:
            adgroup.append(0)
    for url in urls:        
        try:
            sep= str(url).split("adv=")
            bueno = sep[1].split("&")
            advertisement.append(bueno[0])
        except:
            advertisement.append(0)
    for url in urls:
        try:
            sep= str(url).split("sl=")
            bueno = sep[1].split("&")
            sitelink.append(bueno[0])
        except:
            sitelink.append(0)
    for url in urls:
        try:
            sep= str(url).split("idUser=")
            bueno = sep[1].split("&")
            ide_user1.append(bueno[0])
        except:
            ide_user1.append(0)
    for url in urls:
        try:
            sep= str(url).split("gclid=")
            bueno = sep[1].split("&")
            gclid1.append(bueno[0])
        except:
            gclid1.append(0)
    for url in urls:
        try:
            sep= str(url).split("uuid=")
            bueno = sep[1].split("&")
            uuid1.append(bueno[0])
        except:
            uuid1.append(0)



    navegacion['id_camp'] = campaña
    navegacion['id_adg'] = adgroup
    navegacion['id_adv'] = advertisement
    navegacion['id_sl'] = sitelink
    navegacion['id_user1'] = ide_user1
    navegacion['gclid1'] = gclid1
    navegacion['uuid1'] = uuid1
    return navegacion
print(filtrar_url(dataset_navegacion()))


# añade  la funcion de filtrar_url al navegación.csv y lo guarda en un nuevo archivo llamado navegacion_filtrada.csv
def guardar_navegacion(navegacion):
    navegacion.to_csv("navegacion_filtrada.csv", sep = ";")
guardar_navegacion(filtrar_url(dataset_navegacion()))

def borrar_repetidos(navegacion):
    navegacion = pd.read_csv('navegacion_filtrada.csv')
    navegacion = navegacion.drop_duplicates(subset = ['id_user1','gclid1','uuid1'], keep = 'first')
    return navegacion
print(borrar_repetidos(dataset_navegacion()))








    

















