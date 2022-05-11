import pandas as pd
conversiones=pd.read_csv("conversiones.csv", sep=";")
navegacion=pd.read_csv("navegacion.csv", sep=";")
from pandas.core.frame import DataFrame


#para eliminar los elementos que estén mal buscamos aquellos que no tengan sl(Site link) y los eliminamos
def separarUrl(URL,palabraclave):
    url_parts=URL.split("&")
    camp=0
    ar=[]
    for part in url_parts:
        if "=" in part:
            ar=part.split("=")
            if(len(ar)>2):
                return -1
            else:
                key=ar[0]
                value=ar[1]
            #print(key,value)
            if key==palabraclave:
                camp=value
                break
    return camp

def sacarunaURL(Dataframe,i):
    url=""
    url=Dataframe.iloc[i]["url_landing"]
    return url

def separarUrlColumnaDDF(Dataframe,palabraclave):
    Campa=pd.DataFrame(columns=[palabraclave])
    Data2=Dataframe
    borrados=0
    for i in range (0,len(Data2)):
        camp=separarUrl(sacarunaURL(Data2,i),palabraclave)
        if(camp==-1):
            Dataframe=Dataframe.drop(i-borrados)
            borrados+=1
        else:
            #print(type(Campa))
            Campa=Campa.append({palabraclave:camp}, ignore_index=True)
    return Campa

if __name__ == "__main__":
    conversiones=pd.read_csv("conversiones.csv", sep=";")
    navegacion=pd.read_csv("navegacion.csv", sep=";")

    nav=pd.DataFrame(navegacion)
    conv=pd.DataFrame(conversiones)
    #print(len(nav))
    nav.dropna(subset=["url_landing"], inplace=True)
    #print(len(nav))

    #camp,adg, adv, sl
    #Campaña=separarUrlColumnaDDF(nav,"camp")
    adgroup=separarUrlColumnaDDF(nav,"adg")
    #Adv=separarUrlColumnaDDF(nav,"adv")
    #sl=separarUrlColumnaDDF(nav,"sl")
    #Campaña.to_csv("Campaña.csv")
    adgroup.to_csv("Adgroup.csv", index=False)
    #Adv.to_csv("Adv.csv", index=False)
    #sl.to_csv("sl.csv", index=False)







#para identificar si hay elementos repetidos
def identificarRepetidos (DataFrame):

    for i in range (0,len(DataFrame)):
        DataFrame2=DataFrame
        if(DataFrame.iloc[i]["id_user"]!= ''):
            DataFrame2= DataFrame.duplicated(DataFrame.columns[~DataFrame.columns.isin(['id_user'])])
        elif(DataFrame.iloc[i]["gclid"]!= ''):
            DataFrame2= DataFrame.duplicated(DataFrame.columns[~DataFrame.columns.isin(['gclid'])])
        else:
            DataFrame2= DataFrame.duplicated(DataFrame.columns[~DataFrame.columns.isin(['url_landing'])])


    return DataFrame2

def eliminarRepetidos (DataFrame):

    for i in range (0, len(DataFrame)):
        DataFrame2=DataFrame
        if(DataFrame.iloc[i]["id_user"]!= ''):
            DataFrame2 = DataFrame.drop_duplicates(DataFrame.columns[~DataFrame.columns.isin(['id_user'])])
        elif(DataFrame.iloc[i]["gclid"]!= ''):
            DataFrame2 = DataFrame.drop_duplicates(DataFrame.columns[~DataFrame.columns.isin(['gclid'])])
        else:
            DataFrame2 = DataFrame.drop_duplicates(DataFrame.columns[~DataFrame.columns.isin(['url_landing'])])


    return DataFrame2

if __name__ == "__main__":
    conversiones=pd.read_csv("conversiones.csv", sep=";")
    navegacion=pd.read_csv("navegacion.csv", sep=";")

    nav=pd.DataFrame(navegacion)
    conv=pd.DataFrame(conversiones)
    print(len(nav))
    print("numero1: " + str(len(nav)))
    nav = eliminarRepetidos(nav)
    print("numero2: " + str(len(nav)))
    nav.sort_values(by=["ts"], inplace=True)
    nav.to_csv("navegacionsort.csv", index=False, sep=";")











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

if __name__ == "__main__":
    conversiones=pd.read_csv("conversiones.csv", sep=";")
    navegacion=pd.read_csv("navegacion.csv", sep=";")

    nav=pd.DataFrame(navegacion)
    conv=pd.DataFrame(conversiones)
    nav=nav.assign(Convertido=0)
    conv=conv.assign(Convertido=1)

    union= juntarTablas(nav, conv)
    union.to_csv("union.csv", index=False,sep=";")









































import re
from numpy import shares_memory
from numpy.lib.function_base import append
import pandas as pd
from pandas import tseries

conversiones = pd.read_csv("conversiones.csv", sep = ";")
navegacion = pd.read_csv("navegacion.csv", sep = ";")

index = []
for i in range(conversiones.shape[0]):
    if conversiones._get_value(i, "id_user") == "None":
        index.append(i)

conversiones = pd.DataFrame(conversiones.drop(conversiones.index[index]), columns = conversiones.columns)
conversiones = conversiones.reset_index()

#Numero de visitas que recibe el cliente
print("El numero de visitas que recibe es:", navegacion.shape[0], "visitas")

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
recurrente = {()}
number_of_users = {()}

for i in range(navegacion.shape[0]):
    number_of_users.add(navegacion._get_value(i, "id_user"))
    if navegacion._get_value(i, "user_recurrent") == True:
        recurrente.add(navegacion._get_value(i, "id_user"))
print("El porcentaje de usuarios recurrentes frente a los totales es de:", len(recurrente) / len(number_of_users) * 100, end="%\n")

#Unir a la tabla de navegacion si convierte o no.
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




    
    




















    







































