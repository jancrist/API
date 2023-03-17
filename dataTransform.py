import pandas as pd
from time import sleep
import http.client
from datetime import datetime, timedelta
import time
import requests
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from gspread_dataframe import set_with_dataframe
import os
import pandas as pd
import emojis
#creamos las funciones principales
scope=['https://www.googleapis.com/auth/spreadsheets',
'https://www.googleapis.com/auth/drive.file',
'https://www.googleapis.com/auth/drive']
creds= ServiceAccountCredentials.from_json_keyfile_name('test.json', scope)
client=gspread.authorize(creds)
py=client.open('CALENDARIO 2023')
# Lista de hojas de cálculo
hojas = ['ENERO','FEBRERO','MARZO', 'ABRIL', 'MAYO', 'JUNIO', 'JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE']

# Diccionario para almacenar los DataFrames
dataframes = {}

# Obtener los datos de cada hoja de cálculo
for hoja in hojas:
    worksheet = py.worksheet(hoja)
    data = worksheet.get(f"A6:Z{worksheet.row_count}")
    dataframes[hoja] = pd.DataFrame(data)

# Acceder a los DataFrames creados con los nombres de las hojas de cálculo
enerodf = dataframes["ENERO"]
febrerodf = dataframes["FEBRERO"]
marzodf = dataframes["MARZO"]
abrildf = dataframes["ABRIL"]
mayodf = dataframes["MAYO"]
juniodf = dataframes["JUNIO"]
juliodf = dataframes["JULIO"]
agostodf = dataframes["AGOSTO"]
septiembredf = dataframes["SEPTIEMBRE"]
octubredf = dataframes["OCTUBRE"]
noviembredf = dataframes["NOVIEMBRE"]
diciembredf = dataframes["DICIEMBRE"]



enerodf.columns = enerodf.iloc[0]
febrerodf.columns = febrerodf.iloc[0]
marzodf.columns = marzodf.iloc[0]
abrildf.columns = abrildf.iloc[0]
mayodf.columns = mayodf.iloc[0]
juniodf.columns = juniodf.iloc[0]
juliodf.columns = juliodf.iloc[0]
agostodf.columns = agostodf.iloc[0]
septiembredf.columns = septiembredf.iloc[0]
octubredf.columns = octubredf.iloc[0]
noviembredf.columns = noviembredf.iloc[0]
diciembredf.columns = diciembredf.iloc[0]

enerodf= enerodf[1:]
febrerodf= febrerodf[1:]
marzodf= marzodf[1:]
abrildf= abrildf[1:]
mayodf= mayodf[1:]
juniodf= juniodf[1:]
juliodf= juliodf[1:]
agostodf= agostodf[1:]
septiembredf= septiembredf[1:]
octubredf= octubredf[1:]
noviembredf= noviembredf[1:]
diciembredf= diciembredf[1:]





#Esta funcion se encarga de realizar recortes a partir del dataframe descargado
def recortar_dataframe(df, longitud_deseada):
    longitud_actual = len(df)
    if longitud_actual >= longitud_deseada:
        df_recortado = df.iloc[:longitud_deseada, :]
    else:
        df_recortado = df.iloc[:longitud_actual, :]
    return df_recortado

def generar_dataframes(df, num_dataframes, longitud_deseada, intervalo, inicio, mes):
    for i in range(1, num_dataframes+1):
        fin = inicio + longitud_deseada
        nombre_df = f"{mes}_{i}_b"
        df_temp = None
        
        if fin > len(df):
            df_temp = recortar_dataframe(df.iloc[inicio:, :], longitud_deseada)
        else:
            df_temp = df.iloc[inicio:fin, :]

        # Verificar si la columna "Fecha y Hora" tiene valores faltantes
        if df_temp["Fecha y Hora"].isna().any():
            print(f"Se encontraron valores faltantes en el dataframe {nombre_df}. No se creará.")
        else:
            globals()[nombre_df] = df_temp

        inicio += intervalo



#Para llamar a la funcion

generar_dataframes(enerodf,100, 20, 26, 0,"enero")
generar_dataframes(febrerodf,100, 20, 26, 0,"febrero")
generar_dataframes(marzodf,100, 20, 26, 0,"marzo")
generar_dataframes(abrildf,100, 20, 26, 0,"abril")
generar_dataframes(mayodf,100, 20, 26, 0,"mayo")
generar_dataframes(juniodf,100, 20, 26, 0,"junio")
generar_dataframes(juliodf,100, 20, 26, 0,"julio")
generar_dataframes(agostodf,100, 20, 26, 0,"agosto")
generar_dataframes(septiembredf,100, 20, 26, 0,"septiembre")
generar_dataframes(octubredf,100, 20, 26, 0,"octubre")
generar_dataframes(noviembredf,100, 20, 26, 0,"noviembre")
generar_dataframes(diciembredf,100, 20, 26, 0,"diciembre")

#Hasta aca funciona bien





 
#Esta funcion se encarga de realizar recortes a partir del dataframe descargado
def recortar_dataframe(df, longitud_deseada):
    longitud_actual = len(df)
    if longitud_actual >= longitud_deseada:
        df_recortado = df.iloc[:longitud_deseada, :]
    else:
        df_recortado = df.iloc[:longitud_actual, :]
    return df_recortado



def generar_dataframes(df, num_dataframes, longitud_deseada, intervalo, inicio, mes):
    lista_dataframes = []
    for i in range(1, num_dataframes+1):
        fin = inicio + longitud_deseada
        nombre_df = f"{mes}_{i}_b"
        df_temp = None
        
        if fin > len(df):
            df_temp = recortar_dataframe(df.iloc[inicio:, :], longitud_deseada)
        else:
            df_temp = df.iloc[inicio:fin, :]

        # Verificar si la columna "Fecha y Hora" tiene valores faltantes
        if df_temp["Fecha y Hora"].isna().any():
            print(f"Se encontraron valores faltantes en el dataframe {nombre_df}. No se creará.")
        else:
            lista_dataframes.append(df_temp)

        inicio += intervalo
    
    # Concatenar los dataframes
    df_concatenado = pd.concat(lista_dataframes)
    
    return df_concatenado



def transforma_dataframe(df):
    # Obtener el DataFrame a partir de su nombre
    
    
    try:
        # Obtener el valor de la primera celda de la columna "Fecha y Hora"
        fecha_inicio = df.iloc[0, 0]
    except (TypeError, KeyError):
        print(f"Error: El DataFrame {df} no tiene valor en la primera celda de la columna 'Fecha y Hora'")
        return None
    
    # Agregar la columna "fecha" y asignarle el valor de la primera celda
    df["fecha"] = fecha_inicio
        
    # Eliminar la primera columna de la fecha y renombrar "Fecha y Hora" a "Hora"
    df = df.drop(df.index[0])

    df.rename(columns={"Fecha y Hora": "Hora"}, inplace=True)
        
    # Eliminar columnas innecesarias
    df = df.drop(["Nombre", "Nombre.1", "Nombre.2", "Nombre.3"], axis=1)
    
    
        
    # Crear nuevos dataframes con las columnas necesarias
    aux1 = df[["fecha", "Hora", "Numero"]].rename(columns={"Numero": "Number"})
    aux2 = df[["fecha", "Hora", "Numero.1"]].rename(columns={"Numero.1": "Number"})
    aux3 = df[["fecha", "Hora", "Numero.2"]].rename(columns={"Numero.2": "Number"})
    aux4 = df[["fecha", "Hora", "Numero.3"]].rename(columns={"Numero.3": "Number"})
    
    # Concatenar los nuevos dataframes en uno final
    final_df = pd.concat([aux1, aux2, aux3, aux4], ignore_index=True)
        
    # Imprimir el número de dataframe y su forma
    print(f"Procesando DataFrame {df} de forma {final_df.shape}")
    
    return final_df


def procesar_dataframes(nombres_meses):
    # Lista para guardar los dataframes procesados
    dataframes_procesados = []

    # Loop para procesar los dataframes
    for mes in nombres_meses:
        # Generar el dataframe correspondiente
        df = generar_dataframes(globals()[f"{mes}df"], 100, 20, 26, 0, mes)

        # Si la generación del dataframe fue exitosa, procesarlo y agregarlo a la lista
        if df is not None:
            df_procesado = transforma_dataframe(df)
            if df_procesado is not None:
                dataframes_procesados.append(df_procesado)

    # Concatenar los dataframes procesados en uno solo
    resultado_final = pd.concat(dataframes_procesados, ignore_index=True)
    resultado_final = resultado_final[resultado_final["Hora"].str.contains(":")]

    return resultado_final
nombres_meses = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]

resultado_final = procesar_dataframes(nombres_meses)
resultado_final.to_csv('total1.csv',index=False)


def limpiar_y_transformar_number(number):
    # Eliminar espacios y caracteres especiales
    PREFIJOS_PAISES= [
    "+93", "+355", "+213", "+1-684", "+376", "+244", "+1-264", "+672", "+1-268",
    "+374", "+297", "+61", "+43", "+994", "+1-242", "+973", "+880", "+1-246",
    "+375", "+32", "+501", "+229", "+1-441", "+975", "+591", "+387", "+267", "+55",
    "+246", "+673", "+359", "+226", "+257", "+855", "+237", "+1", "+238", "+1-345",
    "+236", "+235", "+56", "+86", "+61", "+61", "+57", "+269", "+682", "+506", "+385",
    "+53", "+599", "+357", "+420", "+243", "+45", "+253", "+1-767", "+1-809", "+1-829",
    "+670", "+593", "+20", "+503", "+240", "+291", "+372", "+251", "+500", "+298",
    "+679", "+358", "+33", "+689", "+241", "+220", "+995", "+49", "+233", "+350",
    "+30", "+299", "+1-473", "+590", "+1-671", "+502", "+44-1481", "+224", "+245",
    "+592", "+509", "+504", "+852", "+36", "+354", "+91", "+62", "+98", "+964", "+353",
    "+44-1624", "+972", "+39", "+225", "+1-876", "+81", "+44-1534", "+962", "+7",
    "+254", "+686", "+383", "+965", "+996", "+856", "+371", "+961", "+266", "+231",
    "+218", "+423", "+370", "+352", "+853", "+389", "+261", "+265", "+60", "+960",
    "+223", "+356", "+692", "+222", "+230", "+262", "+52", "+691", "+373", "+377",
    "+976", "+382", "+1-664", "+212", "+258", "+95", "+264", "+674", "+977", "+31",
    "+599", "+687", "+64", "+505", "+227", "+234", "+683", "+672", "+1-670", "+47",
    "+968", "+92", "+680", "+970", "+507", "+675", "+595", "+51", "+63", "+64",
    "+48", "+351", "+1-787", "+1-939", "+974", "+242", "+262", "+40", "+7", "+250",
    "+590", "+290", "+1-869", "+1-758", "+590", "+508", "+1-784", "+685", "+378",
    "+239", "+966", "+221", "+381", "+248", "+232", "+65", "+1-721", "+421", "+386",
    "+677", "+252", "+27", "+82", "+211", "+34", "+94", "+249", "+597", "+47",
    "+268", "+46", "+41", "+963", "+886", "+992", "+255", "+66", "+228", "+690",
    "+676", "+1-868","+54","+59"]
    if number is not None:
        

        number = re.sub(r'[^\d+]', '', number)
        
        # Si el número comienza con un prefijo de país de la lista, no hacer ninguna transformación
        if number.startswith(tuple(PREFIJOS_PAISES)):
            number=number[1:]
            return number
        
        # Si el número comienza con "11", agregar el prefijo "549"
        if number.startswith('11'):
            number = f"54911{number[2:]}"
        # Si el número no comienza con "11" y no tiene el prefijo "54", agregar el prefijo "54"
        elif not number.startswith('54'):
            number = f"54{number}"
        
        return number
    
        



resultado_final['Number'] = resultado_final['Number'].apply(limpiar_y_transformar_number)

#Funcion que envia Mensajes por medio de solicitudes Post
def sendMessage(para, mensaje):
    url = 'URL DE NUESTRA API'
    
    data = {
        "message": mensaje,
        "phone": para
    }
    headers = {
        'Content-Type':'application/json'
    }
    print(data)
    response = requests.post(url, json=data, headers=headers)
    time.sleep(10)
    return response
  
  for number, date, hour in zip(numbers, dates, hours):
            # Saltar si number es una cadena vacía
            if number == "":
                continue
            try:
                # Convertir number a int
                number = int(number)
                if opcion== True:
                    if number in numeros_enviados:
                        messagebox.showinfo("Información", f"El número {number} ya ha sido enviado.")
                        continue
                    numeros_enviados.append(number)
                    date= str(date)
                    hour=str(hour)
                    mensaje='Hola🤩! Te recordamos que tu turno es el ⏰ *'+date+'* a las *'+hour+'* ⏰. Te esperamos!🌪😄🤙.'
                    sendMessage(int(number), f"{mensaje}")
                    clientes_atendidos += 1
                else:
                    numeros_enviados.append(number)
                    date= str(date)
                    hour=str(hour)
                    mensaje='Hola🤩! Te recordamos que tu turno es el ⏰ *'+date+'* a las *'+hour+'* ⏰. Te esperamos!🌪😄🤙.'
                    sendMessage(int(number), f"{mensaje}")
                    clientes_atendidos += 1
            except ValueError:
                # Si la conversión falla, saltar este valor y continuar con el siguiente
                pass
