<h1 align="center">	Examen coches</h1>

<h2>Repositorio:</h2>

Este es el link del [repositorio](https://github.com/Barroso03/Examen_coches.git)
***
<h2>Explicación</h2>

Esta tarea trata de una empresa de coches en el cual tienes una serie de datos y tienes que reponder preguntas para saber que tal va la empresa.
Para que veas que lo he entendidio te voy a hacer un resumen de lo que he hecho, en primer lugar he importado las libreriasnecesarias, tras esto he leido los dos csvs y se puede comprobar que hay que limpiarlos, ya que hay datos vacios y datos mal hechos, tras limpiar los dataset que es lo mas complejo del examen hay que unirlos y crear solo un csv. Ya hecho todo esto respondemos facilmente las preguntas ques cogiendo los datos del dataset ya limpiado, tras esto por ultimo hago las graficas para que se vea visualmente los resultados.

***




## Código:<a name="id1"></a>



```
import re
from numpy.lib.function_base import append #Esta libreria nos permite elementos a un array
import pandas as pd #Esta libreria nos permite trabajar con dataframes
#Una vez exportadas las librerias, definimos las funciones para cada dataset
import matplotlib.pyplot as plt
import seaborn as sns


#PASO 1: LEER LOS FICHEROS CSV
def Dataset_conversiones():
    datos_conversion=pd.read_csv('conversiones.csv', sep=';')
    return datos_conversion
Dataset_conversiones()

#Acabamos de ejecutarlo y vemos que nos da el resultado esperado

def Dataset_navegacion():
    datos_navegacion=pd.read_csv('navegacion.csv', sep=';') 
    return datos_navegacion
print (Dataset_navegacion())

#PASO 2: SEPARAR LOS DATOS DE LA URL DE LOS DATOS DE LA CAMPAÑA, ADGROUP, ADVERTISEMENT, SITE_LINK
#hay fallos en alguna de las url pues el idUser y gclid en muchas de las lineas no coinciden con la de la url proporcionada, luego estos datos también los sacaremos de la url y crearemos columnas nuevas

def Separacion_datos_url(URL):

    #En primer lugar creo lista vacias, que luego usaremos para hacer columnas en la tabla
    campaña=[]
    adgroup=[]
    advertisement=[]
    site_link=[]
    id_user1=[]
    gclid_1=[]
    uuid_1=[]
    #Estos datos los vamos a extraer de las urls que se encuentran en la columna de url_landings
    
    for url in URL: #En esta parte se separan los datos de la url
        try:
            x= str(url).split('camp=') #Se separa la url por el caracter 'camp=', para acceder a este string delante hemos puesto lo de str(url)
            y= (x[1]).split('&') #ponemos x[1] para que nos deje solo lo que esta después de 'camp='
            campaña.append(y[0]) #Se añade a la lista de campaña tan solo lo que esta después de 'camp='
        except:
            campaña.append(0)
    for url in URL:
        try:
            x= str(url).split('adg=')
            y= (x[1]).split('&')
            adgroup.append(y[0])
        except:
            adgroup.append(0)
    for url in URL:
        try:
            x= str(url).split('ad=')
            y= (x[1]).split('&')
            advertisement.append(y[0])
        except:
            advertisement.append(0)
    for url in URL:
        try:
            x= str(url).split('sl=')
            y= (x[1]).split('&')
            site_link.append(y[0])
        except:
            site_link.append(0)
    for url in URL:
        try:
            x= str(url).split('idUser=')
            y= (x[1]).split('&')
            id_user1.append(y[0])
        except:
            id_user1.append(0)
    for url in URL:
        try:
            x= str(url).split('gclid=')
            y= (x[1]).split('&')
            gclid_1.append(y[0])
        except:
            gclid_1.append(0)
    for url in URL:
        try:
            x= str(url).split('uuid=')
            y= (x[1]).split('&')
            uuid_1.append(y[0])
        except:
            uuid_1.append(0)

#Con esto vamos a creasr el nuevo csv con los datos separados
    Datos= {'Campaña':campaña, 'Adgroup':adgroup, 'Advertisement':advertisement, 'Site_link':site_link, 'id_user':id_user1, 'gclid':gclid_1, 'uuid':uuid_1, 'ts':Dataset_navegacion()['ts'], 'url_landing': Dataset_navegacion()['url_landing']} #Se crea un diccionario con los datos de la url
    navegacion_final=pd.DataFrame(Datos) #Se crea un dataframe con los datos de la url
    navegacion_final.to_csv('navegacion_final.csv', sep=';')
Separacion_datos_url(Dataset_navegacion()['url_landing'])

#PASO 3: ELIMINAMOS LOS USUARIOS DONDE SE REPITE LOS DATOS DEL ID_USER, GCLID, UUID

def Eliminacion_datos_repetidos():
    navegacion_final=pd.read_csv('navegacion_final.csv', sep=';')
    navegacion_final=navegacion_final.drop_duplicates(subset=['id_user', 'gclid', 'uuid'], keep='first')
    navegacion_final.to_csv('navegacion_final.csv', sep=';')
Eliminacion_datos_repetidos()

# A continuacion hacemos una funcion que ordena los datos por fecha de navegacion
def Ordenacion_por_ts():
    navegacion_final=pd.read_csv( 'navegacion_final.csv', sep=';')
    navegacion_final=navegacion_final.sort_values('ts', ascending=False)
    navegacion_final.to_csv('navegacion_final.csv', sep=';')
Ordenacion_por_ts()

#PASO 4: UNIMOS AMBAS CSV
#4.1 Antes de unir limpiamos el dataframe de conversiones
#ahora vamos a limpiar el csv de conversiones 
def Limpiar_conversiones(dato=[]):
    for i in range(len(dato)): #en este bucle vamos a sustituir los valores 0 por None por blancos
        if dato[i]=='None': 
            dato[i]= ''
    return dato
#Aplicamos la funcion a las columnas 'id_user' y 'gclid'
Limpiar_conversiones(Dataset_conversiones()['id_user'])
Limpiar_conversiones(Dataset_conversiones()['gclid'])
conversion_final = pd.DataFrame({'id_user':Limpiar_conversiones(Dataset_conversiones()['id_user']), 'gclid':Limpiar_conversiones(Dataset_conversiones()['gclid']), 'date':Dataset_conversiones()['date'], 'hour':Dataset_conversiones()['hour'], 'id_lead':Dataset_conversiones()['id_lead'], 'lead_type': Dataset_conversiones()['lead_type'], 'result':Dataset_conversiones()['result']})
conversion_final.to_csv('conversion_final.csv', sep=';')

#4.2 Ahora vamos a unir los datos de navegacion y conversiones

def Conversiones(data_1, data_2):
    conversion= []
    for i in data_1:
        if i in data_2:
            conversion.append(1)
        else:
            conversion.append(0)
    return conversion
#Aplicamos la funcion a las columnas 'id_user' y 'gclid'
navegacion_final=pd.read_csv('navegacion_final.csv', sep=';')
conversion_final=pd.read_csv('conversion_final.csv', sep=';')
conversiones_por_id= Conversiones(navegacion_final['id_user'], conversion_final['id_user'])
conversiones_por_gclid=Conversiones(navegacion_final['gclid'], conversion_final['gclid'])
#creamos un csv con la union de los datos
union={'Campaña':navegacion_final['Campaña'],'Adgroup':navegacion_final['Adgroup'], 'Advertisement':navegacion_final['Advertisement'],'Site_link': navegacion_final['Site_link'], 'id_user_navegacion': navegacion_final['id_user'], 'gclid_navegacion':navegacion_final['gclid'], 'uuid': navegacion_final['uuid'], 'ts': navegacion_final['ts'], 'id_user_conversiones': conversion_final['id_user'], 'gclid_conversiones': conversion_final['gclid'], 'hour': conversion_final['hour'], 'date': conversion_final['date'], 'id_lead': conversion_final['id_lead'], 'lead_type': conversion_final['lead_type'], 'result': conversion_final['result'] }
union_final=pd.DataFrame(union)
csv_union= union_final.assign(conversiones_por_gclid= conversiones_por_gclid, conversiones_por_id=conversiones_por_id)
csv_union.to_csv('union_final.csv', sep=';')


#PASO 5: RESPONDEMOS A LAS PREGUNTAS
#5.1: Cuantos id_user hay en total
def visitas():
    datos_navegacion=pd.read_csv('navegacion.csv', sep=';')
    total=datos_navegacion['id_user'].shape[0] #nos cuenta el numero de filas, luego tiene en cuenta los que se repite(que es importante puesto que al fin y al cabo son visitas)
    return total
print(f'El número de visitas que recibe es igual a {visitas()}')

#5.2: Cuantos CALL y FORM hay en total
def call_form():
    conversiones=pd.read_csv('conversion_final.csv', sep=';')
    call=conversiones[conversiones['lead_type']=='CALL'].shape[0]
    form=conversiones[conversiones['lead_type']=='FORM'].shape[0]
    return call, form
print(f'El número de llamadas que recibe es igual a {call_form()[0]}')
print(f'El número de formularios que recibe es igual a {call_form()[1]}')

#5.3: Porcentaje de usuarios recurrentes sobre el total de usuarios
def porcentaje_recurrentes():
    datos_navegacion=pd.read_csv('navegacion.csv', sep=';')
    total=datos_navegacion['id_user'].shape[0]
    recurrentes=datos_navegacion[datos_navegacion['id_user'].duplicated()].shape[0]
    porcentaje=round(recurrentes/total *100)
    return porcentaje
print(f'El porcentaje de usuarios recurrentes es igual a {porcentaje_recurrentes()}%')

#5.4: Coche mas visitado

def coche_mas_visitado():
    datos_navegacion=pd.read_csv('navegacion_final.csv', sep=';')
    cars = {}
    for i in range(datos_navegacion.shape[0]):
        m = re.search("http(?:s?):\/(?:\/?)www\.metropolis\.com\/es\/(.+?)\/.*", str(datos_navegacion._get_value(i, "url_landing"))) #busca en la url_landing el coche que se esta visitando
        if m != None:
            if m.groups()[0] in cars:
                cars[m.groups()[0]] += 1
            else:
                cars[m.groups()[0]] = 1
    return max(cars, key=cars.get)
print(f'El coche mas visitado es {coche_mas_visitado()}')
#5.5:Gráfico de sectores para er si hay mas call o form
def grafico_sectores_call_form():
    conversiones=pd.read_csv('conversion_final.csv', sep=';')
    call=conversiones[conversiones['lead_type']=='CALL'].shape[0]
    form=conversiones[conversiones['lead_type']=='FORM'].shape[0]
    labels = 'CALL', 'FORM'
    sizes = [call, form]
    colors = ['gold', 'yellowgreen']
    explode = (0.1, 0.1)
    plt.title('CALL vs FORM')
    plt.pie(sizes, explode=explode, labels=labels, colors=colors,
            autopct='%1.1f%%', shadow=True, startangle=140)
    plt.axis('equal')
    plt.show()
    # salva el grafico en un archivo .png y meteo en una carpeta llamada graficos
    plt.savefig('graficos/grafico_sectores_call_form.png')

grafico_sectores_call_form()


#5.6:Gráfico de porcentaje de usuarios recurrentes
def grafico_porcentaje_recurrentes():
    datos_navegacion=pd.read_csv('navegacion_final.csv', sep=';')
    total=datos_navegacion['id_user'].shape[0]
    recurrentes=datos_navegacion[datos_navegacion['id_user'].duplicated()].shape[0]
    porcentaje=round(recurrentes/total *100)
    labels = 'Recurrentes', 'No recurrentes'
    sizes = [porcentaje, 100-porcentaje]
    colors = ['gold', 'yellowgreen']
    explode = (0.1, 0.1)
    plt.title('Porcentaje de usuarios recurrentes')
    plt.pie(sizes, explode=explode, labels=labels, colors=colors,
            autopct='%1.1f%%', shadow=True, startangle=140)
    plt.axis('equal')
    plt.show()
    # salva el grafico en un archivo .png y meteo en una carpeta llamada graficos
    plt.savefig('graficos/grafico_porcentaje_recurrentes.png')
grafico_porcentaje_recurrentes()

#5.7:Gráfico de coche mas visitado
def grafico_coche_mas_visitado():
    datos_navegacion=pd.read_csv('navegacion_final.csv', sep=';')
    cars = {}
    for i in range(datos_navegacion.shape[0]):
        m = re.search("http(?:s?):\/(?:\/?)www\.metropolis\.com\/es\/(.+?)\/.*", str(datos_navegacion._get_value(i, "url_landing")))
        if m != None:
            if m.groups()[0] in cars:
                cars[m.groups()[0]] += 1
            else:
                cars[m.groups()[0]] = 1
    labels = list(cars.keys())
    sizes = list(cars.values())
    
    
    plt.bar(labels, sizes)
    plt.title('Coches mas visitado')
    plt.xticks(rotation=90)
   
    plt.show()
    # salva el grafico en un archivo .png y meteo en una carpeta llamada graficos
    plt.savefig('graficos/grafico_coche_mas_visitado.png')
    
grafico_coche_mas_visitado()






***

