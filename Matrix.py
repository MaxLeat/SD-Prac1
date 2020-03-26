import numpy as np
import pywren_ibm_cloud as pywren
import pickle
import time
from random import randint

#ESPAI VARIABLES GLOBALS
fila=2
columna=2
columnaB=4
workers=2
filaStr="Fila_"
colunnaStr="Columna_"

def inicialitzacio(files, columnes, columnesB, ibm_cos):

   #Columnes de a = files de b
   #Creem unes matrius random i les omplim un altre cop (perque el random.ranndint no va bé)  
    A =np.random.randint(10, size=(files, columnes))
    B =np.random.randint(10, size=(columnes, columnesB))
    C =np.random.randint(10, size=(files, columnesB))

    #S'omplen les matrius amb valors aleatoris de rang 10
    for i in range(files):
        for j in range(columnes):
            A[i,j]=randint(0,10)

    for i in range(columnes):
        for j in range(columnesB):
            B[i,j]=randint(0,10)
 
    #Per cada fila de A faig un fitxer
    for i in range(files):
        fitxer=filaStr + str(i)
        ibm_cos.put_object(Bucket='sdurv', Key=fitxer, Body = pickle.dumps(A[i], pickle.HIGHEST_PROTOCOL))
    
    #Per cada columna de B faig un fitxer
    for i in range(columnesB):
        fitxer=colunnaStr + str(i)
        ibm_cos.put_object(Bucket='sdurv', Key=fitxer, Body = pickle.dumps(B[:,i], pickle.HIGHEST_PROTOCOL))

    #Pujo la matriu C per omplir-la al reduce
    ibm_cos.put_object(Bucket='sdurv', Key='MatriuC.txt', Body = pickle.dumps(C, pickle.HIGHEST_PROTOCOL))

def matrix_mul(fitxers, ibm_cos):

    resultats=[]
    i=0
    #Aqui tinc de dos en dos els noms dels fitxers
    fit= fitxers.split()
    num_strings = len(fit)

    while i < num_strings:
        #Descarrego els arxius que em fan falta
        fitxerA=fit[i]
        fitxerB=fit[i+1]
        A_s=ibm_cos.get_object(Bucket='sdurv', Key=fitxerA)['Body'].read()
        B_s=ibm_cos.get_object(Bucket='sdurv', Key=fitxerB)['Body'].read()
        A=pickle.loads(A_s)
        B=pickle.loads(B_s)
        #Faig les operacions
        C=A.dot(B)
        resultats.append(C)
        i=i+2

    return resultats

def reduce_function(results, ibm_cos):

    #En aquesta funció el que hem de fer es recollir els resultats de l'execució anterior
    C_s=ibm_cos.get_object(Bucket='sdurv', Key='MatriuC.txt')['Body'].read()
    C=pickle.loads(C_s)
    f_inici=0
    c_inici=0
    #Fem un recorregut per la llista de resultats i els posem a la matriu C
    for i in range(len(results)):
        for j in range(len(results[i])):
            C[f_inici][c_inici] = results[i][j]
            c_inici = c_inici + 1
            if c_inici >= columnaB:
                c_inici = 0
                f_inici = f_inici +1

    return C

if __name__ == '__main__':
    pw = pywren.ibm_cf_executor()
    operacions=fila *columnaB

    #Fem la comprovació de que els workers que s'han posat siguin acceptables
    if (workers <= operacions) and (workers > 0):
        #D'aquesta manera tenim les operacions que ha de fer cada worker i les que falten
        operacions_worker= operacions // workers
        resten = operacions - (operacions_worker * workers)
        #Inicialitzem les matrius
        pw.call_async(inicialitzacio, [fila,columna,columnaB])

        iterdata=[]
        f_inici=0;
        c_inici=0;

        #Creem les dades per passar al map_reduce
        for i in range(workers):
            iterdata.append([])
            iterdata[i]=""
            #Miro si és l'últim per fer més operacions o no depenent de si hi ha resto
            if (resten !=0) and (i == workers - 1):
                operacions_worker=resten + operacions_worker
            for j in range(operacions_worker):
                if f_inici < fila:
                    if c_inici >= columnaB:
                        #si estem aqui vol dir que hem de posar columna 0 i aumentar la fila
                        c_inici=0
                        f_inici=f_inici+1
                #Si es l'ultim ha de fer les que faltin no només les necessaries
                nom_f=filaStr + str(f_inici)
                nom_c=colunnaStr + str(c_inici)
                iterdata[i]=iterdata[i] + str(nom_f) + " " + str(nom_c) + " "
                c_inici = c_inici + 1 
        
        print(iterdata)
            
        #Fem la crida al map_reduce
        start_time = time.time()
        futures = pw.map_reduce(matrix_mul, iterdata, reduce_function)
        pw.wait(futures)
        elapsed_time = time.time() - start_time
        print(pw.get_result())
        print()
        print("EL TEMPS QUE HA TRIGAT ES:")
        print(elapsed_time)
    else:
        print("ERROR: NUMERO DE WORKERS NO PERMÉS, HA DE SER SUPERIOR 0 I INFERIOR A " + str(operacions+1))