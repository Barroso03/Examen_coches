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
















def juntarTablas(Dataframe1,Dataframe2):

    Dataframe1= eliminarRepetidos(Dataframe1)
    Dataframe2= eliminarRepetidos(Dataframe2)
    Dataframe3= pd.DataFrame()
    if "id_suite" in Dataframe1.columns and "id_suite" in Dataframe2.columns:
        Dataframe3= pd.merge(Dataframe1, Dataframe2, on='id_suite', how='outer',suffixes=("_nav","_conv"))
    elif "gclid" in Dataframe1.columns and "gclid" in Dataframe2.columns:
        Dataframe3= pd.merge(Dataframe1, Dataframe2, on='gclid', how='outer',suffixes=("_nav","_conv"))
    else:
        Dataframe3= pd.merge(Dataframe1, Dataframe2, on='url_landing', how='outer',suffixes=("_nav","_conv"))

    return Dataframe3

conv=pd.DataFrame(conversiones)
nav= nav.assign(Convertido=0)
conv=conv.assign(Convertido=1)
union= juntarTablas(nav, conv)
union.to_csv("union.csv", index=False,sep=";")
conversiones = pd.read_csv("conversiones.csv", sep = ";")
navegacion = pd.read_csv("navegacion_filtrada.csv", sep = ";")
def filtrar_conversiones():
    index = []
    for i in range(conversiones.shape[0]):
        if conversiones._get_value(i, "id_user") == "None":
            index.append(i)

    conversiones = pd.DataFrame(conversiones.drop(conversiones.index[index]), columns = conversiones.columns)
    conversiones = conversiones.reset_index()

#Numero de visitas que recibe el cliente
    print("El numero de visitas que recibe es:", navegacion.shape[0], "visitas")

def conversiones_por_cliente():
#Cuántas de ellas convierten y cuántas no (en %)
    convertidos = 0
    users_navegacion = navegacion["id_user"]
    users_conversion = conversiones["id_user"]
    for user_nav in users_navegacion:
        for user_conv in users_conversion:
            if user_nav == user_conv:
             convertidos += 1

    print("El porcentaje de visitas a convertidos es de " , convertidos / navegacion.shape[0] * 100, end="%\n")

#Por tipo de conversión (CALL o FORM), ¿cuántas hay de cada una?
#Primero observamos que no hay ningun user repetido, a simple vista se ve
def  tipo_conversion():
    call = 0
    form = 0
    tipo = conversiones["lead_type"]
    for type in tipo:
        if type == "CALL":
            call += 1
        else:
            form += 1

    print("EL numero de conversiones del tipo call es:", call)
    print("EL numero de conversiones del tipo form es:", form)

#Porcentaje de usuarios recurrentes sobre el total de usuarios
#Solo nos interesa una vez cada uno de los users asi que los guardamos en un conjunto para que no haya repetidos
def usuarios_recurrentes():
    recurrente = {()}
    number_of_users = {()}

    for i in range(navegacion.shape[0]):
        number_of_users.add(navegacion._get_value(i, "id_user"))
        if navegacion._get_value(i, "user_recurrent") == True:
         recurrente.add(navegacion._get_value(i, "id_user"))
    print("El porcentaje de usuarios recurrentes frente a los totales es de:", len(recurrente) / len(number_of_users) * 100, end="%\n")

#Unir a la tabla de navegacion si convierte o no.
def unir_conversiones():
    data = []
    only_user = navegacion["id_user"]
    for user in only_user:
        convierte = False
        for i in conversiones["id_user"]:
            if user == i:
                convierte = True
    
        if convierte:
            data.append(1)
        else:
            data.append(0)

    navegacion["convierte"] = data
#Ahora navegacio contiene todos los datos de si ese usuario convierte o no.

#Ver cual es el coche más visitado de la página.
def coche_mas_visitado():
    cars = {
    
    }

    for i in range(navegacion.shape[0]):
        m = re.search("http(?:s?):\/(?:\/?)www\.metropolis\.com\/es\/(.+?)\/.*", str(navegacion._get_value(i, "url_landing")))
        if m != None:
            if m.groups()[0] in cars:
                cars[m.groups()[0]] += 1
            else:
                cars[m.groups()[0]] = 1
        
    for car in cars.keys():
        print("El coche", car, "ha sido buscado", cars[car], "veces")

def separar_datos():
#Separar cada una de las partes para añair a la tabla
    campaña = []
    adg = []
    adv = []
    sl = []
    urls = navegacion["url_landing"]
#Valor del id campaña
    for url in urls:
        try:
            esp = str(url).split("camp=")
            bueno = esp[1].split("&")
            campaña.append(bueno[0])
        except:
            campaña.append(0)
#Valor del id del adgroup
    for url in urls:
        try:
            esp = str(url).split("adg=")
            bueno = esp[1].split("&")
            adg.append(bueno[0])
        except:
            adg.append(0)
#valor del adv
    for url in urls:
        try:
            esp = str(url).split("adv=")
            bueno = esp[1].split("&")
            adv.append(bueno[0])
        except:
            adv.append(0)
#valor del sl
    for url in urls:
        try:
            esp = str(url).split("sl=")
            bueno = esp[1].split("&")
            sl.append(bueno[0])
        except:
            sl.append(0)
    navegacion["id_camp"] = campaña
    navegacion["id_adg"] = adg
    navegacion["id_adv"] = adv
    navegacion["id_sl"] = sl

    print(navegacion)
#Esto es para guardar el fichero final, comentar si no se usa
    navegacion.to_csv("navegacion_final.csv", index = False)

# ejecuta las funciones

unir_conversiones()
separar_datos()
coche_mas_visitado()
conversiones_por_cliente()
tipo_conversion()
usuarios_recurrentes()




    
    




















    

















