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



# PASO 3: IDENTIFICAR REPETIDOS Y ELIMINARLOS

def identificarRepetidos (navegacion_filtrada):

    for i in range (0,len(navegacion_filtrada)):
        DataFrame2=navegacion_filtrada
        navegacion_filtrada = pd.read_csv("navegacion_filtrada.csv", sep = ";")
        
        if(navegacion_filtrada.iloc[i]["id_user1"]!= ''):
            DataFrame2= navegacion_filtrada.duplicated(navegacion_filtrada.columns[~navegacion_filtrada.columns.isin(['id_user1'])])
        elif(navegacion_filtrada.iloc[i]["gclid1"]!= ''):
            DataFrame2= navegacion_filtrada.duplicated(navegacion_filtrada.columns[~navegacion_filtrada.columns.isin(['gclid1'])])
        else:
            DataFrame2= navegacion_filtrada.duplicated(navegacion_filtrada.columns[~navegacion_filtrada.columns.isin(['url_landing1'])])
    return DataFrame2
print(identificarRepetidos(dataset_navegacion()))

def eliminarRepetidos (navegacion_filtrada):

    for i in range (0, len(navegacion_filtrada)):
        DataFrame2=navegacion_filtrada
        navegacion_filtrada = pd.read_csv("navegacion_filtrada.csv", sep = ";")
        if(navegacion_filtrada.iloc[i]["id_user1"]!= ''):
            DataFrame2 = navegacion_filtrada.drop_duplicates(navegacion_filtrada.columns[~navegacion_filtrada.columns.isin(['id_user1'])])
        elif(navegacion_filtrada.iloc[i]["gclid1"]!= ''):
            DataFrame2 = navegacion_filtrada.drop_duplicates(navegacion_filtrada.columns[~navegacion_filtrada.columns.isin(['gclid1'])])
        else:
            DataFrame2 = navegacion_filtrada.drop_duplicates(navegacion_filtrada.columns[~navegacion_filtrada.columns.isin(['url_landing1'])])
    return DataFrame2
print(eliminarRepetidos(dataset_navegacion()))
# Hazme una funcion que ordene el dataset por la columna ts
def ordenar_por_ts(navegacion_filtrada):
    navegacion_filtrada = pd.read_csv("navegacion_filtrada.csv", sep = ";")
    navegacion_filtrada = navegacion_filtrada.sort_values(by=['ts'])
    return navegacion_filtrada
print(ordenar_por_ts(dataset_navegacion()))

# PASO 4: UNIR CSVS
# limpia el csv conversiones
def Limpiar_conversiones(dato=[]):
    for i in range (len(dato)):
        if (dato[i] == 'None'):
            dato[i] = ""
    return dato
    # aplica la funcion limpiar a las columnas 'id_user y ' gclid'de conversiones
Limpiar_conversiones(dataset_conversiones['id_user'])
Limpiar_conversiones(dataset_conversiones['gclid'])
conversion_final = pd.DataFrame({'id_user':Limpiar_conversiones(dataset_conversiones()['id_user']), 'gclid':Limpiar_conversiones(dataset_conversiones()['gclid']),'date': dataset_conversiones (), 'hour':dataset_conversiones()['hour'], 'id_lead':dataset_conversiones()['id_lead'], 'lead_type': dataset_conversiones()['lead_type'],'result':dataset_conversiones()['result']})
conversion_final.to_csv("conversiones_final.csv", sep = ";")



def Conversiones(data1, data2):
    conversion = []
    for i in data1:
        if i in data2:
            conversion.append(1)
        else:
            conversion.append(0)
    return conversion
# aplicamos la funcion a las columnas id_user y gclid 
navegacion_final = pd.read_csv("navegacion_filtrada.csv", sep = ";")
conversion_final = pd.read_csv("conversiones_final.csv", sep = ";")
conversion_por_id= Conversiones(navegacion_final['id_user1'], conversion_final['id_user'])
conversion_por_gclid= Conversiones(navegacion_final['gclid1'], conversion_final['gclid'])
# creamos la union de los datos
union = {'Campaña':navegacion_final['Campaña'], 'Adgroup':navegacion_final['Adgroup'], 'Advertisement':navegacion_final['Advertisement'], 'Site_link':navegacion_final['Site_link'], 'id_user_navegacion':navegacion_final['id_user1'], 'gclid_navegacion':navegacion_final['gclid1']}
union_final= pd.DataFrame(union)
csv_union= union_final.assign(conversion_id=conversion_por_id, conversion_gclid=conversion_por_gclid)
csv_union.to_csv("union_final.csv", sep = ";")

















