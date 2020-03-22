import numpy as np
import pywren_ibm_cloud as pywren
import pickle
from random import randint


def inicialitzacio(files, columnes, columnesB, ibm_cos):

   #Columnes de a = files de b
   #Creem unes matrius random i les omplim un altre cop (perque el random.ranndint no m'anava bé)  
    A =np.random.randint(10, size=(files, columnes))
    B =np.random.randint(10, size=(columnes, columnesB))
    C =np.random.randint(10, size=(files, columnesB))
    for i in range(files):
        for j in range(columnes):
            A[i,j]=randint(0,10)

    for i in range(columnes):
        for j in range(columnesB):
            B[i,j]=randint(0,10)
    
    #Seria més efectiu pujar les files i columnes separades
    fitxerA="Fila_"
    fitxerB="Columna_"
    #Per cada fila de A faig un fitxer
    for i in range(files):
        fitxer=fitxerA + str(i)
        ibm_cos.put_object(Bucket='sdurv', Key=fitxer, Body = pickle.dumps(A[i], pickle.HIGHEST_PROTOCOL))
    
    for i in range(columnesB):
        fitxer=fitxerB + str(i)
        ibm_cos.put_object(Bucket='sdurv', Key=fitxer, Body = pickle.dumps(B[:,i], pickle.HIGHEST_PROTOCOL))

    #Pujo la matriu C per omplir-la al reduce
    ibm_cos.put_object(Bucket='sdurv', Key='MatriuC.txt', Body = pickle.dumps(C, pickle.HIGHEST_PROTOCOL))

def matrix_mul(fila, columna, operacions, fila_max, columna_max, ibm_cos):

    resultats=[]
    filaStr="Fila_"
    colunnaStr="Columna_"
    resultatStr="F_"
    i=0
    while i < operacions:
        if fila < fila_max:
            if columna >= columna_max:
                #si estem aqui vol dir que hem de posar columna 0 i aumentar la fila
                columna=0
                fila=fila+1
            #Descarrego els arxius que em fan falta
            fitxerA=filaStr + str(fila)
            fitxerB=colunnaStr + str(columna)
            A_s=ibm_cos.get_object(Bucket='sdurv', Key=fitxerA)['Body'].read()
            B_s=ibm_cos.get_object(Bucket='sdurv', Key=fitxerB)['Body'].read()
            A=pickle.loads(A_s)
            B=pickle.loads(B_s)
            #Faig les operacions
            C=A.dot(B)
            resultats.append(C)
        columna=columna+1
        i=i+1
    return resultats

def reduce_function(results, ibm_cos):
    #En aquesta funció el que hem de fer es recollir els resultats de l'execució anterior
    C_s=ibm_cos.get_object(Bucket='sdurv', Key='MatriuC.txt')['Body'].read()
    C=pickle.loads(C_s)
    files=len(C)
    columnes=len(C[0])
    k=0
    contador_intern=0
    for i in range(files):
        for j in range(columnes):
            #He de mirar si el valor de resutats es una llista
            longitud = len(results[k])
            if longitud > 1:
                #Si estic aqui el valor es una llista i he d'iterar en aquesta llista 
                if contador_intern < longitud:
                    #Encara em queden valors per agafar de la llista interna
                    valor = results[k][contador_intern]
                    C[i,j]=valor
                    contador_intern=contador_intern+1
                else:
                    k=k+1   #passo al seguent valor perque la llista interna ja la he mirat tota
                    contador_intern=0
                    #He d'agafar el seguen perque no puc estar una posició sense apuntar res
                    longitud=len(results[k])
                    if longitud >1 :
                        alor = results[k][contador_intern]
                        C[i,j]=valor
                        contador_intern=contador_intern+1
                    else:
                        #Si es una llista de 1 valor
                        valor = results[k][0]
                        C[i,j]=valor
                        k=k+1
            else:
                #Si es una llista de 1 valor
                valor = results[k][0]
                C[i,j]=valor
                k=k+1           
    return C

if __name__ == '__main__':
    pw = pywren.ibm_cf_executor()
    fila=2
    columna=2
    columnaB=4
    pw.call_async(inicialitzacio, [fila,columna,columnaB])
    operacions=fila *columnaB
    #1 operació cada un
    #iterdata= [[0,0,1,fila,columnaB], [0,1,1,fila,columnaB], [1,0,1,fila,columnaB], [1,1,1,fila,columnaB]]
    iterdata= [[0,0,4,fila,columnaB], [1,0,2,fila,columnaB], [1,2,1,fila,columnaB], [1,3,1,fila,columnaB]]
    pw.map_reduce(matrix_mul, iterdata, reduce_function)
    #pw.call_async(matrix_mul, [0,0,operacions, fila, columnaB] )
    print(pw.get_result())