import numpy as np
import pywren_ibm_cloud as pywren
import pickle


def inicialitzacio(files, columnes, ibm_cos):

   #Columnes de a = files de b

    #A=np.array([np.random.choice(5, 2),np.random.choice(5, 2),np.random.choice(5, 2)]) 
    #B=np.array([np.random.choice(5, 3),np.random.choice(5, 3)])
    #A=np.array([[1,2],[2,3],[3,4]]) 
    #B=np.array([[1,2],[2,6]])


    #Inicialitzo les matrius buides
    A=np.empty((files, columnes))
    B=np.empty((columnes,files))

    #Omplo les matrius amb valors aleatoris
    for i in range(files):
       A[i]=np.random.choice(10,columnes)
    
    for j in range(columnes):
        B[j]=np.random.choice(10,files)
    

    ibm_cos.put_object(Bucket='sdurv', Key='MatriuA.txt', Body = pickle.dumps(A, pickle.HIGHEST_PROTOCOL))
    ibm_cos.put_object(Bucket='sdurv', Key='MatriuB.txt', Body = pickle.dumps(B, pickle.HIGHEST_PROTOCOL))

def matrix_mul(x, ibm_cos):

    #Agafem les dades per fer la multiplicació
    A_s= ibm_cos.get_object(Bucket='sdurv', Key='MatriuA.txt')['Body'].read()
    B_s= ibm_cos.get_object(Bucket='sdurv', Key='MatriuB.txt')['Body'].read()

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