import numpy as np
import pywren_ibm_cloud as pywren
import pickle
from random import randint

# Columnes de a = files de b
fila = 5
columna = 5
# Tamany maxim que tindra un fitxer que conte files de A (No pot ser major que columnes / workers)
divisioFil = 2
# Tamany maxim que tindra un fitxer que conte columnes de B (No pot ser major que el numero de Files)
divisioCol = 2
SubMA = 0  # Numero de submatrius A que tindrem, Estblim el tamany un cop haguem comprovat que la resta de valors son correctes
SubMB = 0  # Numero de Submatrius B que tindrem, Estblim el tamany un cop haguem comprovat que la resta de valors son correctes
filaStr = "Fila_"
colunnaStr = "Columna_"
#A=np.array([np.random.choice(5, 2),np.random.choice(5, 2),np.random.choice(5, 2)])
#B=np.array([np.random.choice(5, 2),[randint(0,10),randint(0,10)]])
# A=np.array([[1,2],[2,3],[3,4]])
# B=np.array([[1,2],[7,4]])

# Inicialitzo les matrius buides
#A = np.empty((files, columnes))
#B = np.empty((columnes,files))
SubMA = fila//divisioFil + ((fila/divisioFil)-(fila//divisioFil))
SubMB = columna//divisioCol + 1
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
    if np.size(A[i*divisioFil:((i+1)*divisioFil)])!=0:
        fitxer = filaStr + str(i*divisioFil)
        print (fitxer)  
        uno = (A[i*divisioFil:((i+1)*divisioFil)]) 
        print (uno)
    print ("\n")
    if np.size(B[:, i*divisioCol:((i+1)*divisioCol)])!=0:
        fitxer = colunnaStr + str(i*divisioCol)
        print (fitxer)
        dos = (B[:, i*divisioCol:((i+1)*divisioCol)])
        print (dos)
    X=uno.dot(dos)
    print ("Resultat :")
    print (X)
    print ("---------------------\n")

print ("operacions = " + str(SubMB*SubMA))
   