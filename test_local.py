import numpy as np
import pywren_ibm_cloud as pywren
import pickle
from random import randint

#Columnes de a = files de b
files=3
columnes=2
#A=np.array([np.random.choice(5, 2),np.random.choice(5, 2),np.random.choice(5, 2)]) 
#B=np.array([np.random.choice(5, 2),[randint(0,10),randint(0,10)]])
#A=np.array([[1,2],[2,3],[3,4]]) 
#B=np.array([[1,2],[7,4]])

#Inicialitzo les matrius buides
#A = np.empty((files, columnes))
#B = np.empty((columnes,files))
A =np.random.randint(10, size=(files, columnes))
B =np.random.randint(10, size=(columnes, files))
for i in range(files):
    for j in range(columnes):
        A[i,j]=randint(0,10)

for i in range(columnes):
    for j in range(files):
        B[i,j]=randint(0,10)

nom_variable="fila_"
i=1
nom_variable=nom_variable + str(i)
print(nom_variable)

#Omplo les matrius amb valors aleatoris
#for i in range(files):
#   A[i]=np.random.choice(10,columnes)

#for j in range(columnes):
#    B[j]=np.random.choice(10,files)

print("Matriu A")
print(A)
print()
print("Matriu B")
print(B)
print()

FA=A[0]
CB=B[:,0]
print(FA)
print(CB)


#Fem la multiplicació
C=FA.dot(CB)
print("Resultat")
print(C)