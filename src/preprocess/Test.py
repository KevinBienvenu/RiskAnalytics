'''
Created on 5 Apr 2016

@author: Kevin Bienvenu

Test Module containing function to apprehend the functioning of the file cameliaBalAG_extrait.csv
ATTENTION : preprocess functions, not used in the final versions 

'''

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
from chilkat import CkFtp2, CkByteData
from cStringIO import StringIO as cS
import StringIO
import time
import gzip
from ftplib import FTP_TLS
import DrawingTools


os.chdir('../../')


def extractBasicInformations(fileName, columnNames, sep='\t'):
    '''
    ATTENTION : preprocess function, not used in the final versions
    '''
    csvinput = pd.read_csv(fileName, sep = sep)
    for name in columnNames:
        print 'column:',name
        print 'maxi :', np.max(csvinput[name])
        print 'mini :', np.min(csvinput[name])
        print 'mean :', np.mean(csvinput[name])
     
def repartitionEntriesEntreprise(fileName = 'cameliaBalAG_extrait.csv', sep = '\t'):
    '''
    ATTENTION : preprocess function, not used in the final versions
    '''
    csvinput = pd.read_csv(fileName, sep=sep, parse_dates=['datePiece','dateEcheance','dateDernierPaiement'])
    
    column = csvinput['entrep_id']
    
    nbEntreprises = len(np.unique(column))
    nbEntries = len(column)
    
    print "nb d'entreprises : " , nbEntreprises
    print "nb moyen de facture par entreprise : ", nbEntries/nbEntreprises
    
    nbEntriesByEntreprises = [0]*nbEntreprises
    entrepriseToIdDict = {}
    
    compt = 0
    
    for entry in csvinput.values:
        entreprise = entry[0]
        if(not entrepriseToIdDict.has_key(entreprise)):
            entrepriseToIdDict[entreprise] = compt
            compt += 1
        nbEntriesByEntreprises[entrepriseToIdDict[entreprise]]+=1
    nbEntriesByEntreprises = np.array([a for a in nbEntriesByEntreprises])
    
    print "nombre max de factures: ",np.max(nbEntriesByEntreprises)
    print "nombre min de factures: ",np.min(nbEntriesByEntreprises)
        
    # ordo = np.sort(nbEntriesByEntreprises)
    # ordo[:] = ordo[::-1]
    # print ordo[:30]
    
    
    thresholdNbEntries = 100;
    print "nombre d'entreprises avec plus que",thresholdNbEntries,"factures :",len([a for a in nbEntriesByEntreprises if a>thresholdNbEntries])
    
    
    nbStepGraph = 201
    sizeStepGraph = 1
    vectorNbEntreprisesByEntries = [0 for a in range(nbStepGraph)]
    for i in range(nbStepGraph):
        vectorNbEntreprisesByEntries[i] = len([a for a in nbEntriesByEntreprises if a>i*sizeStepGraph])
    
    plt.plot(vectorNbEntreprisesByEntries)
    plt.title("nombre d'entreprises selon le nombre de factures")
    plt.xlabel("nombre de factures minimal")
    plt.ylabel("nombre d'entreprises")
    plt.show()

# repartitionEntriesEntreprise()




# d0 = "1996-11-20"
# d1 = "1997-11-20"
# d2 = "1995-11-20"
# d3 = datetime.datetime.strptime("1999-03-20",'%Y-%m-%d')
# 
# print Utils.nbMonthsBetweenDates(d0, d3)



# data = {'name': ['Jason', 'Molly', 'Tina', 'Jake', 'Amy'], 
#         'year': [2012, 2012, 2013, 2014, 2014], 
#         'reports': [4, 24, 31, 2, 3]}
# df = pd.DataFrame(data, index = ['Jason', 'Molly', 'Tina', 'Jake', 'Amy'])
# print df
# df = df.drop(df.index[[1,3]])
# print df

# d0 = datetime.datetime.strptime("1989-10-10","%Y-%m-%d").date()
# d1 = datetime.datetime.strptime("1992-12-11","%Y-%m-%d").date()
# d2 = datetime.datetime.strptime("1993-05-11","%Y-%m-%d").date()
# 
# print d0
# print str(d0)
# 
# print '%.3e' % 37**4

# print Utils.nbMonthsBetweenDates(d0, d1)
# print int(abs((d0-d1).days/30.45))
# 
# a = [(int)(5*np.random.rand()) for i in range(5)]
# b = [(int)(5*np.random.rand()) for i in range(5)]
# print map(add,a,b)
# A = np.array([[1,2,5],[4,2,1],[3,1,2]])
# 
# print A
# print np.min(A)
# print np.min(A,axis=1)
# print np.min(A,axis=0)


# v = [np.random.rand() for a in range(10)]
# 
# 
# plt.figure(1)                # the first figure
# plt.bar(range(10),v,color='r')
# plt.show()

# print Utils.checkIntFormat("2e3", True, True)

host = "137.194.54.103"
port = 34177
user = "KevinBienvenu"
password = "x0D4u#FBBYJ*&ZpXr^^M"

localfile = "../RiskAnalytics/cameliaLiensEntites.csv"

def connectFtplib():
    start_time = time.time()
    ftp = FTP_TLS()
#     ftp.passiveserver = True
    print ftp.connect(host,port)
    print ftp.login(user,password)
    print ftp.prot_p()

    sio = StringIO.StringIO()
    def handle_binary(more_data):
        sio.write(more_data)
    
    resp = ftp.retrbinary("RETR CameliaBalAG.csv.gz", callback=handle_binary)
    interval = time.time() - start_time  
    print 'Data downloaded :', interval, 'sec'
    sio.seek(0) # Go back to the start
    results = gzip.GzipFile(fileobj=sio)
    db = pd.read_csv(results,sep="\t")
    interval = time.time() - start_time  
    print 'Dataframe created :', interval, 'sec'
    

def connectCKFtp2():
    ftp = CkFtp2()
    ftp.UnlockComponent("Anything for 30-day trial")
    ftp.put_Passive(True)
    ftp.put_Hostname(host)
    ftp.put_Port(port)
    ftp.put_Username(user)
    ftp.put_Password(password)
    ftp.put_AuthTls(True)
    ftp.put_Ssl(False)
    ftp.put_SslProtocol("TLS 1.2")
    ftp.Connect()
    start_time = time.time() 
    data = CkByteData()
    ftp.GetRemoteFileBinaryData("cameliaBalAG.csv.gz",data)
    ftp.Disconnect()
    ftp.ClearDirCache()
    results = gzip.GzipFile(fileobj=cS(data.getData()))
    
    interval = time.time() - start_time  
    print 'Data downloaded :', interval, 'sec'
    data.clear()
    db = pd.read_csv(results,sep="\t")
    interval = time.time() - start_time  
    print 'Dataframe created :', interval, 'sec'
    
#     temp = ftp.getRemoteFileTextC("cameliaLiensEntites.csv","utf-8")
#     print temp


# connectCKFtp2()

# connectFtplib()

#     temp = bz2.decompress(ftp.getRemoteFileTextC("cameliaLiensEntites.csv.bz2","utf-8"))

# 
#     
# a = np.array([1, 2, 3])
# b = np.array([4, 3, 1])
# 
# print len(set(a).intersection(set(b)))



tab=[10,52,12,3,5,2,1,2]
with open("src/preprocess/test.txt",'w') as fichier:
    fichier.write("x:"+str(tab))

DrawingTools.drawHistogramFromFile("src/preprocess/test.txt")











