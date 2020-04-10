from cos_backend import COSBackend
import numpy as np
import pywren_ibm_cloud as pywren
import pickle
import time
import math
from random import randint

# ESPAI VARIABLES GLOBALS
# Dimensions de les matrius
fila = 300  # = m
columna = 300  # = n
columnaB = 300      # = l
# Numero de workers
workers = 5
# Seleccio d'exercici
# Si exercici es = 1: Es calcularan automaticament els workers necesaris
# Si exercici es = 2: S'utilitzaran els valors introduits a les variables globals
exercici = 1
# Tamany maxim que tindra un fitxer que conte files de A (No pot ser major que columnes / workers)
# Tamany maxim que tindra un fitxer que conte columnes de B (No pot ser major que el numero de Files)
divisio = 300
SubMA = 0  # Numero de submatrius A que tindrem, Estblim el tamany un cop haguem comprovat que la resta de valors son correctes
SubMB = 0  # Numero de Submatrius B que tindrem, Estblim el tamany un cop haguem comprovat que la resta de valors son correctes
# Variables Auxiliars per creacio de fitxers
filaStr = "Fila_"
colunnaStr = "Columna_"
# Nom del cos que s'utilitzara
nom_cos = 'ramonsd'


def inicialitzacio(files, columnes, columnesB, ibm_cos):

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

    # Per cada fila de A es crea un fitxer
    for i in range(SubMA):
        # Si la matriu no esta buida
        if np.size(A[i*divisio:((i+1)*divisio)]) != 0:
            fitxer = filaStr + str(i*divisio)
            ibm_cos.put_object(Bucket=nom_cos, Key=fitxer,
                               Body=pickle.dumps(A[i*divisio:((i+1)*divisio)], pickle.HIGHEST_PROTOCOL))

    # Per cada columna de B es crea un fitxer
    for i in range(SubMB):
        if np.size(B[:, i*divisio:((i+1)*divisio)]) != 0:
            fitxer = colunnaStr + str(i*divisio)
            ibm_cos.put_object(Bucket=nom_cos, Key=fitxer,
                               Body=pickle.dumps(B[:, i*divisio:((i+1)*divisio)], pickle.HIGHEST_PROTOCOL))

    # Pujem la matriu C per omplir-la al reduce
    ibm_cos.put_object(Bucket=nom_cos, Key='MatriuC.txt',
                       Body=pickle.dumps(C, pickle.HIGHEST_PROTOCOL))
    ibm_cos.put_object(Bucket=nom_cos, Key='MatriuA.txt',
                       Body=pickle.dumps(A, pickle.HIGHEST_PROTOCOL))
    ibm_cos.put_object(Bucket=nom_cos, Key='MatriuB.txt',
                       Body=pickle.dumps(B, pickle.HIGHEST_PROTOCOL))


def matrix_mul(fitxers, ibm_cos):

    resultats = []
    i = 0
    # A fitxers tenim els noms dels fitxer que hem de multiplicar entre si
    fit = fitxers.split()
    num_strings = len(fit)

    while i < num_strings:
        # Anem Agafant els fitxers de dos en dos
        fitxerA = fit[i]
        fitxerB = fit[i+1]
        # Descarreguem les matrius corresponents
        A_s = ibm_cos.get_object(Bucket=nom_cos, Key=fitxerA)['Body'].read()
        B_s = ibm_cos.get_object(Bucket=nom_cos, Key=fitxerB)['Body'].read()
        A = pickle.loads(A_s)
        B = pickle.loads(B_s)
        # Multipliquem les dues matrius
        C = A.dot(B)
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
    resultats = [0, 0, 0, 0, 0]
    pw = pywren.ibm_cf_executor()
    # Comprovem que hem escollit un exerci correcte
    if(exercici > 1) and (exercici < 2):
        exercici = 1
    if(exercici == 2):
        workers = math.ceil(fila/divisio)
        # Comprovem el valor de les variables referents a la creacio de submatrius Exercici 2
        if divisio > (fila/workers):
            divisio = math.ceil(fila/workers)
        if divisio > columna:
            divisio = columna
        if divisio <= 0:
            divisio = 1

    if(exercici == 1):
        # Calculs exercici 1
        workers = math.ceil(fila/divisio)
        print(workers)

    if(workers > 100):
        workers = 100
    # Calculem el numero de divisions que obtindrem
    SubMA = math.ceil(fila/divisio)
    SubMB = math.ceil(columnaB/divisio)
    operacions = SubMA * SubMB
    # Fem la comprovació de que els workers que s'han posat siguin acceptables
    if (workers <= operacions) and (workers > 0):
        for cosas in range(5):
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

            # Inicialitzem les matrius
            futures = pw.call_async(inicialitzacio, [fila, columna, columnaB])
            pw.wait(futures)
            cos = COSBackend()
            A = cos.get_object('sdurv', 'MatriuA.txt')
            B = cos.get_object('sdurv', 'MatriuB.txt')
            A = pickle.loads(A)
            B = pickle.loads(B)

            iterdata = []
            f_inici = 0
            c_inici = 0
            # Creem les dades per passar al map_reduce
            for i in range(workers):
                iterdata.append([])
                iterdata[i] = ""
                # Si hem acabat les operacions, coloquem les sobrants, (Si n'hi ha) Al ultim worker
                if (resten != 0) and (i == workers - 1):
                    operacions_worker = resten + operacions_worker
                # Per cada worker li
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
                    iterdata[i] = iterdata[i] + \
                        str(nom_f) + " " + str(nom_c) + " "
                    c_inici = c_inici + divisio

            # print(iterdata)
            # Iniciem el timer
            start_time = time.time()
            # Fem la crida al map_reduce
            futures = pw.map_reduce(matrix_mul, iterdata, reduce_function)
            pw.wait(futures)
            # Calculem el temps que ha passat
            elapsed_time = time.time() - start_time
            print('Matriu A:')
            print()
            print(A)
            print()
            print('Matriu B:')
            print()
            print(B)
            print(pw.get_result())
            print()
            print("EL TEMPS QUE HA TRIGAT ES:")
            print(elapsed_time)
            resultats[cosas] = elapsed_time
    else:
        print("ERROR: NUMERO DE WORKERS NO PERMÉS, HA DE SER SUPERIOR 0 I INFERIOR A " + str(operacions+1))
    print(resultats)