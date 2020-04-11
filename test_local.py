import numpy as np
import pywren_ibm_cloud as pywren
import pickle
from random import randint
import math
# Columnes de a = files de b
fila = 10
columna = 10
# Tamany maxim que tindra un fitxer que conte files de A (No pot ser major que columnes / workers)
divisio = 1

SubMA = 0  # Numero de submatrius A que tindrem, Estblim el tamany un cop haguem comprovat que la resta de valors son correctes
SubMB = 0  # Numero de Submatrius B que tindrem, Estblim el tamany un cop haguem comprovat que la resta de valors son correctes
filaStr = "Fila_"
colunnaStr = "Columna_"
#A=np.array([np.random.choice(5, 2),np.random.choice(5, 2),np.random.choice(5, 2)])
#B=np.array([np.random.choice(5, 2),[randint(0,10),randint(0,10)]])
# A=np.array([[1,2],[2,3],[3,4]])
# B=np.array([[1,2],[7,4]])
fitxers = dict()
# Inicialitzo les matrius buides
#A = np.empty((files, columnes))
#B = np.empty((columnes,files))
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

for i in range(SubMA):
    # Si la matriu no esta buida
    if np.size(A[i*divisio:((i+1)*divisio)]) != 0:
        fitxer = filaStr + str(i*divisio)
        matriu = A[i*divisio:((i+1)*divisio)]
        fitxers.update({fitxer: matriu})


print(fitxers)
print(fitxers['Fila_0'])

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
    # Per cada worker li
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
        print("Dins el for")
        print (nom_f)
        print (fitxers.get(nom_f))
        data.update({nom_f: fitxers.get(nom_f)})
        data.update({nom_c: fitxers.get(nom_c)})
        print(fitxers['Fila_0'])
        c_inici = c_inici + divisio
    # Pujem la data amb el nom de Worker_x


# Omplo les matrius amb valors aleatoris
# for i in range(files):
#   A[i]=np.random.choice(10,columnes)

# for j in range(columnes):
#    B[j]=np.random.choice(10,files)

print("Matriu A")
print(A)
print()
print("Matriu B")
print(B)
print()

FA = A[0]
CB = B[:, 0]
print(FA)
print(CB)

# Fem la multiplicació
C = FA.dot(CB)
print("Resultat")
print(C)

for i in range(SubMA):
    if np.size(A[i*divisio:((i+1)*divisio)]) != 0:
        fitxer = filaStr + str(i*divisio)
        print(fitxer)
        uno = (A[i*divisio:((i+1)*divisio)])
        print(uno)
    print("\n")
    if np.size(B[:, i*divisio:((i+1)*divisio)]) != 0:
        fitxer = colunnaStr + str(i*divisio)
        print(fitxer)
        dos = (B[:, i*divisio:((i+1)*divisio)])
        print(dos)
    X = uno.dot(dos)
    print("Resultat :")
    print(X)
    print("---------------------\n")

print("operacions = " + str(SubMB*SubMA))
