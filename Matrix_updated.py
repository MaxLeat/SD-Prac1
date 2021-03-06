import numpy as np
import pywren_ibm_cloud as pywren
import pickle
import time
import math
from random import randint

# ESPAI VARIABLES GLOBALS
# Dimensions de les matrius
fila = 100  # = m
columna = 100  # = n
columnaB = 100      # = l
# Tamany de les divisions de les matrius
divisio = 1
SubMA = 0  # Numero de submatrius A que tindrem, Estblim el tamany un cop haguem comprovat que la resta de valors son correctes
SubMB = 0  # Numero de Submatrius B que tindrem, Estblim el tamany un cop haguem comprovat que la resta de valors son correctes
# Variables Auxiliars per creacio de fitxers
filaStr = "f"
colunnaStr = "c"
workerStr = "w"
# Nom del cos que s'utilitzara
nom_cos = 'sdurv'


def inicialitzacio(files, columnes, columnesB, operacions_worker, workers, resten, ibm_cos):

    fitxers = dict()
    SubMA = math.ceil(fila/divisio)
    SubMB = math.ceil(columnaB/divisio)
    operacions = SubMA * SubMB

   # Columnes de a = files de b
   # Creem unes matrius random i les omplim un altre cop (perque el random.ranndint no va bé)
    A = np.random.randint(10, size=(files, columnes))
    B = np.random.randint(10, size=(columnes, columnesB))
    C = np.random.randint(10, size=(files, columnesB))

    # S'omplen les matrius amb valors aleatoris de rang 10
    for i in range(files):
        for j in range(columnes):
            A[i, j] = randint(0, 10)

    for i in range(columnes):
        for j in range(columnesB):
            B[i, j] = randint(0, 10)


    f_inici = 0
    c_inici = 0
    for i in range(workers):
        data = dict()
        # Si hem acabat les operacions, coloquem les sobrants, (Si n'hi ha) Al ultim worker
        if (resten != 0) and (i == workers - 1):
            operacions_worker = resten + operacions_worker
        # Per cada operació que ha de fer el worker 
        for j in range(operacions_worker):
            if f_inici < fila:
                # Mirem si hem arriba al final de la fila
                if c_inici >= columnaB:
                    # Com hem arribat al final baixem una fila
                    c_inici = 0
                    f_inici = f_inici+divisio
            # Actualitzem el diccionari afegint les files necessaries per la operació (si ja estan no posem noves entrades)
            nom_f = filaStr + str(f_inici)
            nom_c = colunnaStr + str(c_inici)
            if np.size(A[f_inici:((f_inici+1))]) != 0:
                data.update({nom_f: A[f_inici:((f_inici+divisio))]})
            if np.size(B[:, c_inici:((c_inici+1))]) != 0:
                data.update({nom_c: B[:, c_inici:((c_inici+divisio))]})
            c_inici = c_inici + divisio
        # Pujem la data (un diccionari de matrius) amb el nom de w_x
        fitxer = workerStr + str(i)
        ibm_cos.put_object(Bucket=nom_cos, Key=fitxer,
                           Body=pickle.dumps(data, pickle.HIGHEST_PROTOCOL))

    # Pujem la matriu C per omplir-la al reduce
    ibm_cos.put_object(Bucket=nom_cos, Key='MatriuC.txt',
                       Body=pickle.dumps(C, pickle.HIGHEST_PROTOCOL))


def matrix_mul(fitxers, ibm_cos):

    resultats = []
    i = 0
    #Particionem l'entrada separant per espais
    fit = fitxers.split()
    num_strings = len(fit)-1
    #Descarreguem el diccionari 
    W_s = ibm_cos.get_object(Bucket=nom_cos, Key=fit[0])['Body'].read()
    W = pickle.loads(W_s)
    i=i+1
    #Anem agafant de dos en dos els noms de les operacions a fer
    while i < num_strings:
        # Multipliquem les dues matrius
        C = W[fit[i]].dot(W[fit[i+1]])
        # Guardem el resultat
        resultats.append(C)
        i = i+2

    return resultats


def reduce_function(results, ibm_cos):
    x = 0
    y = 0
    C_s = ibm_cos.get_object(Bucket=nom_cos, Key='MatriuC.txt')['Body'].read()
    C = pickle.loads(C_s)
    # Recorrem els diferents workers
    for w in range(len(results)):
        # Recorrem les operacions de cad worker
        for o in range(len(results[w])):
            # Recorrem la coordenada i
            for i in range(len(results[w][o])):
                # Recorrem la cordenada j
                for j in range(len(results[w][o][i])):
                    if(x+i < fila) and (y+j < columnaB):
                        C[i+x][j+y] = results[w][o][i][j]

            # Amb aixo controlem on coloquem els numeros a la matriu resultant ja que sino els posariem desordenats
            y = divisio + y
            if(y >= columnaB):
                x = divisio + x
                if(x >= divisio):
                    y = 0
    return C


if __name__ == '__main__':
    pw = pywren.ibm_cf_executor()
   #Calculem els workers
    workers = math.ceil(fila/divisio)
    # Comprovem el valor de les variables referents a la creacio de submatrius Exercici 2
    if divisio > (fila/workers):
        divisio = math.ceil(fila/workers)
    if divisio <= 0:
        divisio = 1
    if(workers > 100):
        workers = 100
    # Calculem el numero de divisions que obtindrem
    SubMA = math.ceil(fila/divisio)
    SubMB = math.ceil(columnaB/divisio)
    operacions = SubMA * SubMB
    # Fem la comprovació de que els workers que s'han posat siguin acceptables
    if (workers <= operacions) and (workers > 0):

        # Calculem les operacions que haura de fer cada worker
        operacions_worker = operacions // workers
        # En cas de que no fos una divisio exacte, guardem el numero d'operacions extra
        resten = operacions - (operacions_worker * workers)

        # Imprimim les variables amb les que treballarem per veure si s'ha hagut de corregir alguna dada
        print("Finalment el programa funcionara amb aquestes dades: ")
        print("Files Matriu A :        " + str(fila))
        print("Columnes Matriu A :     " + str(columna))
        print("Files Matriu B :        " + str(columna))
        print("Columnes Matriu B :     " + str(columnaB))
        print("Files per divisio :     " + str(divisio))
        print("Columnes per divisio :  " + str(divisio))
        print("Workers :               " + str(workers))
        print("Operacions totals :     " + str(operacions))
        print("Operacions per Worker : " + str(operacions_worker))
        print("Submatrius A :          " + str(SubMA))
        print("Submatrius B :          " + str(SubMB))

        iterdata = []
        f_inici = 0
        c_inici = 0
        for i in range(workers):
            iterdata.append([])
            iterdata[i] = ""
            fit_worker = workerStr + str(i)
            iterdata[i] = iterdata[i] + fit_worker + " "
            # Si hem acabat les operacions, coloquem les sobrants, (Si n'hi ha) Al ultim worker
            if (resten != 0) and (i == workers - 1):
                operacions_worker = resten + operacions_worker
            # Per cada operació que ha de fer el worker 
            for j in range(operacions_worker):
                if f_inici < fila:
                    # Mirem si hem arriba al final de la fila
                    if c_inici >= columnaB:
                        # Com hem arribat al final baixem una fila
                        c_inici = 0
                        f_inici = f_inici+divisio
                # Anem escribint les operacions que fara cada worker indicant quina fila i columna han d'operar
                nom_f = filaStr + str(f_inici)
                nom_c = colunnaStr + str(c_inici)
                iterdata[i] = iterdata[i] + str(nom_f) + " " + str(nom_c) + " "
                c_inici = c_inici + divisio

        
        # Inicialitzem les matrius
        futures = pw.call_async(
            inicialitzacio, [fila, columna, columnaB, operacions_worker, workers, resten])
        pw.wait(futures)
        
        #Esperem a que s'hagi realitzat per descarregar les matrius A i B correctes
        # Iniciem el timer
        start_time = time.time()
        # Fem la crida al map_reduce
        futures = pw.map_reduce(matrix_mul, iterdata, reduce_function)
        pw.wait(futures)
        # Calculem el temps que ha passat
        elapsed_time = time.time() - start_time
        print(pw.get_result())
        print()
        print("EL TEMPS QUE HA TRIGAT ES:")
        print(elapsed_time)
    else:
        print("ERROR: NUMERO DE WORKERS NO PERMÉS, HA DE SER SUPERIOR 0 I INFERIOR A " + str(operacions+1))
