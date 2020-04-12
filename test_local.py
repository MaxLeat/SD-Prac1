import numpy as np
import pywren_ibm_cloud as pywren
import pickle
from random import randint
import math
# Columnes de a = files de b
fila = 10
columna = 10
# Tamany maxim que tindra un fitxer que conte files de A (No pot ser major que columnes / workers)
divisio = 2

SubMA = 0  # Numero de submatrius A que tindrem, Estblim el tamany un cop haguem comprovat que la resta de valors son correctes
SubMB = 0  # Numero de Submatrius B que tindrem, Estblim el tamany un cop haguem comprovat que la resta de valors son correctes
filaStr = "Fila_"
colunnaStr = "Columna_"
workerStr = "w"
# A=np.array([np.random.choice(5, 2),np.random.choice(5, 2),np.random.choice(5, 2)])
# B=np.array([np.random.choice(5, 2),[randint(0,10),randint(0,10)]])
# A=np.array([[1,2],[2,3],[3,4]])
# B=np.array([[1,2],[7,4]])
fitxers = dict()
# Inicialitzo les matrius buides
# A = np.empty((files, columnes))
# B = np.empty((columnes,files))
SubMA = math.ceil(fila/divisio)
SubMB = math.ceil(columna/divisio)
A = np.random.randint(3, size=(fila, columna))
B = np.random.randint(3, size=(columna, fila))
for i in range(fila):
    for j in range(columna):
        A[i, j] = randint(0, 3)

for i in range(columna):
    for j in range(fila):
        B[i, j] = randint(0, 3)

nom_variable = "fila_"
i = 1
nom_variable = nom_variable + str(i)
print(nom_variable)


f_inici = 0
c_inici = 0
workers = 4
resten = 0
operacions_worker = 2
for i in range(workers):
    data = dict()
    # Si hem acabat les operacions, coloquem les sobrants, (Si n'hi ha) Al ultim worker
    if (resten != 0) and (i == workers - 1):
        operacions_worker = resten + operacions_worker
    # Per cada operació que ha de fer el worker
    for j in range(operacions_worker):
        if f_inici < fila:
            # Mirem si hem arriba al final de la columna
            if c_inici >= columna:
                # Com hem arribat al final baixem una fila
                c_inici = 0
                f_inici = f_inici+divisio
        # Actualitzem el diccionari afegint les files necessaries per la operació (si ja estan no posem noves entrades)
        nom_f = filaStr + str(f_inici)
        nom_c = colunnaStr + str(c_inici)
        print("In")
        if np.size(A[f_inici:((f_inici+1))]) != 0:
                data.update({nom_f: A[f_inici:((f_inici+divisio))]})
                print(nom_f)
        if np.size(B[:, c_inici:((c_inici+1))]) != 0:
            data.update({nom_c: B[:, c_inici:((c_inici+divisio))]})
            print(nom_c)
        print("out")
        print(nom_f)
        print(nom_c)  
        c_inici = c_inici + divisio 
           
    # Pujem la data (un diccionari de matrius) amb el nom de w_x
    print(data)
print(A)
print(B)
c_inici=6
print(B[:, c_inici:((c_inici)+divisio)])

iterdata = []
f_inici = 0
c_inici = 0
workers = 4
resten = 0
operacions_worker = 2
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
            if c_inici >= columna:
                # Com hem arribat al final baixem una fila
                c_inici = 0
                f_inici = f_inici+divisio
        # Anem escribint les operacions que fara cada worker indicant quina fila i columna han d'operar
        nom_f = filaStr + str(f_inici)
        nom_c = colunnaStr + str(c_inici)
        iterdata[i] = iterdata[i] + str(nom_f) + " " + str(nom_c) + " "
        c_inici = c_inici + divisio
print(iterdata)

# Omplo les matrius amb valors aleatoris
# for i in range(files):
#   A[i]=np.random.choice(10,columnes)

# for j in range(columnes):
#    B[j]=np.random.choice(10,files)
