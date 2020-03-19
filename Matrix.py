import numpy as np
import pywren_ibm_cloud as pywren
import pickle
from random import randint


def inicialitzacio(files, columnes, ibm_cos):

   #Columnes de a = files de b
   #Creem unes matrius random i les omplim un altre cop (perque el random.ranndint no m'anava bé)  
    A =np.random.randint(10, size=(files, columnes))
    B =np.random.randint(10, size=(columnes, files))
    for i in range(files):
        for j in range(columnes):
            A[i,j]=randint(0,10)

    for i in range(columnes):
        for j in range(files):
            B[i,j]=randint(0,10)
    
    #Seria més efectiu pujar les files i columnes separades
    fitxerA="Fila_"
    fitxerB="Columna_"
    #Per cada fila de A faig un fitxer
    for i in range(files):
        fitxer=fitxerA + str(i)
        ibm_cos.put_object(Bucket='sdurv', Key=fitxer, Body = pickle.dumps(A[i], pickle.HIGHEST_PROTOCOL))
    
    for i in range(columnes):
        fitxer=fitxerB + str(i)
        ibm_cos.put_object(Bucket='sdurv', Key=fitxer, Body = pickle.dumps(B[:,i], pickle.HIGHEST_PROTOCOL))


    #Esborro els fitxers anteriors per sobreescriure amb els nous
    ibm_cos.delete_object(Bucket='sdurv', Key='MatriuA.txt')
    ibm_cos.delete_object(Bucket='sdurv', Key='MatriuB.txt')
    ibm_cos.put_object(Bucket='sdurv', Key='MatriuA.txt', Body = pickle.dumps(A, pickle.HIGHEST_PROTOCOL))
    ibm_cos.put_object(Bucket='sdurv', Key='MatriuB.txt', Body = pickle.dumps(B, pickle.HIGHEST_PROTOCOL))

def matrix_mul(x, ibm_cos):

    #Agafem les dades per fer la multiplicació
    #A_s=ibm_cos.get_object(Bucket='sdurv', Key='MatriuA.txt')['Body'].read()
    #B_s=ibm_cos.get_object(Bucket='sdurv', Key='MatriuB.txt')['Body'].read()
    A_s=ibm_cos.get_object(Bucket='sdurv', Key='Fila_0')['Body'].read()
    B_s=ibm_cos.get_object(Bucket='sdurv', Key='Columna_0')['Body'].read()

    #Des-serialitzem les dades de la matriu
    A=pickle.loads(A_s)
    B=pickle.loads(B_s)

    #Fem la multiplicació
    C=A.dot(B)
    return C

if __name__ == '__main__':
    pw = pywren.ibm_cf_executor()
    pw.call_async(inicialitzacio, [3,2])
    pw.call_async(matrix_mul, 2 )
    print(pw.get_result())