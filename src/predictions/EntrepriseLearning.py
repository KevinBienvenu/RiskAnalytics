# -*- coding: utf-8 -*-
'''
Created on 15 avr. 2016

@author: Kevin Bienvenu

== Module encore en chantier ==

'''

import csv
import datetime
import gc
import math
import os
import random
import time

from sklearn import kernel_ridge
from sklearn import linear_model
from sklearn import naive_bayes
from sklearn import svm

import numpy as np
import pandas as pd
from preprocess import CameliaBalAGPreprocess
from preprocess import Utils



def preprocessData(toExportCsv = False):
    '''
    function that imports all csv files, and computes the X and Y vectors to perform learning algorithm
    -- IN:
    toExportCsv : boolean that settles if a csv file must be computed and stored (boolean) default=False
    -- OUT:
    X : vector of features
    Y : vector of observations
    '''
    print "=!= Preprocessing Data =!="
    print ""
    # importing the BalAG file
    csvinput = CameliaBalAGPreprocess.importAndCleanCsv(toPrint=False, ftp=False, toSave=False)
    print "size :",len(csvinput)
    print ""
    del csvinput['montantLitige']
    del csvinput['devise']
    del csvinput['dateInsert']
    del csvinput['paidBill']
    gc.collect()
    log10abs = lambda x : math.log10(abs(x))
    csvinput['logMontant'] = csvinput.montantPieceEur.apply(log10abs)
    csvinput['echeance'] = (csvinput.dateEcheance - csvinput.datePiece).astype('timedelta64[D]')
    csvinput['Y'] = csvinput.dateDernierPaiement.notnull()
    csvinput['year'] = csvinput.datePiece.dt.year
    del csvinput['datePiece']
    del csvinput['dateEcheance']
    del csvinput['dateDernierPaiement']
    gc.collect()
    print "... done"
    print ""
    # importing the Etab file
    csvEtab = CameliaBalAGPreprocess.getAndPreprocessCsvEtab(csvinput)
    csvEtab['age'] = (datetime.datetime.today() - csvEtab.DCREN).astype('timedelta64[Y]')
    del csvEtab['DCREN']
    csvinput = pd.DataFrame.merge(csvinput, csvEtab, on='entrep_id', how='left')
    del csvEtab
    gc.collect()
    csvScore = CameliaBalAGPreprocess.getAndPreprocessCsvScore(csvinput)
    startTime = time.time()
    csvinput = pd.DataFrame.merge(csvinput, csvScore, on=['entrep_id','year'], how='left')
    csvinput.dropna(axis=0,how='any',inplace=True)
    del csvScore
    gc.collect()
    Utils.printTime(startTime)
    
    if toExportCsv:
#         csvinput.to_csv("preprocessedDataBalAG.csv", sep="\t", encoding="utf-8")
        with open("preprocessedDataBalAGsample.csv","w") as csvfile:
            writer = csv.writer(csvfile, delimiter='\t')
            compt = Utils.initProgress(csvinput, 1)
            for line in csvinput.itertuples():
                compt = Utils.updateProgress(compt)
                writer.writerow(line)
    Utils.printTime(startTime)
    
    return csvinput
       
def importPreprocessData(filename = "preprocessedDataBalAGclean.csv"):
    '''
    function that imports the file preprocessedDataBalAG or its clean version
    and put it into two arrays X and Y for the learning process.
    -- IN:
    filename : the name of the file to import (string) default = preprocessedDataBalAG.csv
    -- OUT:
    X : np.array containing features (np.array)
    Y : np.array containing observation (np.array)
    '''
    print "== Importing data"
    usecols = ['montantPieceEur','logMontant','echeance','year','age','scoreSolv','scoreZ','scoreCH','Y']
#     usecolsX = ['montantPieceEur','logMontant','echeance','year','age','scoreSolv','scoreZ','scoreCH']
#     usecolsX = ['logMontant']
    usecolsX = ['echeance']
#     usecolsX = ['logMontant','echeance','age','scoreSolv','scoreZ','scoreCH']
    usecolsY = ['Y']
    dtype = {}
    dtype['montantPieceEur'] = np.float64
    dtype['echeance'] = np.int16
    dtype['year'] = np.int16
    dtype['age'] = np.int16
    dtype['logMontant'] = np.float16
    dtype['scoreSolv'] = np.float16
    dtype['scoreZ'] = np.float16
    dtype['scoreCH'] = np.float16
    dtype['scoreAltman'] = np.float16
    dtype['Y'] = np.bool
    csv = pd.read_csv(filename,sep="\t",usecols=usecols, dtype=dtype)
    fY = lambda x : 1 if x==True else -1
    csv['Y'] = csv['Y'].apply(fY)
    csv.replace([np.inf,-np.inf], np.nan, inplace=True)
    csv.dropna(axis=0,how='any',inplace=True)
    print "   ...done"
    return (csv[usecolsX],csv[usecolsY])

def learning(csvX, csvY):
#     perm = np.random.permutation(csvX.index)
    print "== Learning algorithm"
    print ""
    print "using columns :",csvX.columns.values
    print ""
    X = np.array(csvX)
    Y = np.array(csvY.Y.values)
    perm = np.random.permutation(len(X))
    X = X[perm]
    Y = Y[perm]
    del csvX
    del csvY
    gc.collect()
    
    nbEntries = len(X)
    nbEntriesTraining = int(nbEntries*0.8)
    
    trainX = X[:nbEntriesTraining]
    testX = X[nbEntriesTraining:]
    
    trainY = Y[:nbEntriesTraining]
    testY = Y[nbEntriesTraining:]
    
#     print "length of the training set:", len(trainX)
#     print "length of the testing set:", len(testX)
    
    # list of models
    models = []
    models.append(linear_model.LinearRegression())
#     models.append(svm.SVC())
#     models.append(linear_model.Lasso(alpha = 0.1,max_iter=100000))
#     models.append(kernel_ridge.KernelRidge(alpha = 10))
#     models.append(naive_bayes.GaussianNB())
    
    # training models
    for model in models:
        model.fit(trainX,trainY)
    
    # predicting
    testYmodels = []
    for model in models:
        testYmodels.append(model.predict(testX))
        testYmodels[-1] = [1 if a>0 else -1 for a in testYmodels[-1]]
    
    errors = [[np.abs(testYm[i]-testY[i])/2 for i in range(len(testY))] for testYm in testYmodels]
    print "number of errors:",np.sum(errors,axis=1)
    print "mean error:",np.mean(errors,axis=1)
