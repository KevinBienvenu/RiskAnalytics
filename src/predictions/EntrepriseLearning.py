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
import time

from sklearn import linear_model
from sklearn import naive_bayes

import numpy as np
import pandas as pd
from preprocess import CameliaBalAGPreprocess, DrawingTools
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
       
def importPreprocessData(filename = "preprocessedDataBalAGSampleclean.csv"):
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
    usecolsX = ['logMontant','echeance','age','scoreSolv','scoreZ','scoreCH']
#     usecolsX = ['logMontant']
#     usecolsX = ['echeance']
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
    plus = 1
    moins = 1
    fY = lambda x : moins if x==True else -plus
    csv['Y'] = csv['Y'].apply(fY)
    csv.replace([np.inf,-np.inf], np.nan, inplace=True)
    csv.dropna(axis=0,how='any',inplace=True)
    print "   ...done"
    return (csv[usecolsX],csv[usecolsY])

def learning(X, Y, models, posThreshold=0, coefSizeSample=1, toPrint=False):
    '''
    #TODO: doc à rédiger
    '''
    
#     perm = np.random.permutation(csvX.index)
    if toPrint:
        print "== Learning algorithm"
        print "   using columns :",X.columns.values
        print ""
        print "  = preprocessing"
            
#     trainingSetSelection = "random"
    trainingSetSelection = "balanced"
    
    # NORMALIZATION STEP
    def normalizeSeries(serie):
        maxi = serie.max()
        mini = serie.min()
        fserie = lambda x : 2.0*(x-mini)/(maxi-mini)-1
        return serie.apply(fserie)
    
    X.logMontant = normalizeSeries(X.logMontant)
    X.age = normalizeSeries(X.age)
    X.echeance = normalizeSeries(X.echeance)
    X.scoreZ = normalizeSeries(X.scoreZ)
    X.scoreCH = normalizeSeries(X.scoreCH)
    X.scoreSolv = normalizeSeries(X.scoreSolv)
    
   
    if trainingSetSelection=="random":
        # used to perform a random sample for the training set
        perm = np.random.permutation(len(X))
#         invperm = np.argsort(perm)
        X = X.reindex(perm, copy=False)
        Y = Y.reindex(perm, copy=False)
    elif trainingSetSelection == "balanced":
        # creating a balanced training set
        X['selection'] = pd.Series([False]*len(X))
        # getting all the positive values
        X.loc[Y.Y==1,['selection']] = True
        # sampling the negative values
        indexNeg = Y.loc[Y.Y==-1].sample(int(len(Y.loc[Y.Y==1])*coefSizeSample)).index.values
        X.loc[indexNeg,['selection']] = True
    
    
    nbEntries = len(X)
    nbEntriesTraining = int(nbEntries*0.8)
        
    # training models
    if toPrint:
        print "  = training"
    for model in models:
        if trainingSetSelection == "random":
            # used to perform a random sample for the training set
            model.fit(X.head(nbEntriesTraining),Y.head(nbEntriesTraining).Y.values)
        elif trainingSetSelection == "balanced":
            # using the selection column of the dataframe
            model.fit(X.loc[X.selection==True],Y.loc[X.selection==True].Y.values)
            
    
    # predicting
    if toPrint:
        print "  = predicting"
    Yvec = Y.Y.values
    testYmodels = []
    for model in models:
        testYmodels.append(model.predict(X))
        testYmodels[-1] = [1 if a>posThreshold else -1 for a in testYmodels[-1]]

    errors = [[np.abs(Ym[i]-Yvec[i])/2 for i in range(len(Yvec))] for Ym in testYmodels]
    nbErrors = np.sum(errors,axis=1)
    meanErrors = np.mean(errors,axis=1)
    nbTP = [sum([Ym[i]==1 and Yvec[i]==1 for i in range(len(Yvec))]) for Ym in testYmodels]
    nbFP = [sum([Ym[i]==1 and Yvec[i]==-1 for i in range(len(Yvec))]) for Ym in testYmodels]
    nbFN = [sum([Ym[i]==-1 and Yvec[i]==1 for i in range(len(Yvec))]) for Ym in testYmodels]
    
    #returning the boolean series of errors
    if toPrint:
        print "  = computing error"
    
    if toPrint:
        data = {'Y':Yvec}
        i=0
        cols = []
        for model in models:
            s = str(model).split("(")[0][:6]
            data[s] = testYmodels[i]
            i+=1
            cols.append(s)
        df = pd.DataFrame(data=data)[cols]
        print df
    
    
    if toPrint:
        print "number of errors:",nbErrors
        print "mean error:",meanErrors
        print "True positive (correct answers) :",nbTP
        print "False positive (saying True instead of False) :",nbFP
        print "False negative (missed correct answer) :",nbFN
    
    
    return (nbErrors, meanErrors, nbTP, nbFP, nbFN)

def analyzeposThresAndSizeSample():
    '''
    #TODO: doc à rédiger
    '''
    
    (X,Y) = importPreprocessData()
    dicThres = {'start':-1.0,'end':1.0,'size':0.05}
    dicCoef = {'start':0.0,'end':3.0,'size':0.2}
    vecThres = np.arange(dicThres['start'],dicThres['end']+dicThres['size'],dicThres['size'])
    vecCoef = np.arange(dicCoef['start'],dicCoef['end']+dicCoef['size'],dicCoef['size'])
    nbIter = 5
    
    # list of models
    models = []
    models.append(linear_model.LinearRegression())
#     models.append(svm.SVC())
    models.append(linear_model.Lasso(alpha = 0.01,max_iter=100000))
#     models.append(kernel_ridge.KernelRidge(alpha = 10))
    models.append(naive_bayes.GaussianNB())
     
    arrayToDraw = [np.zeros((len(vecThres),len(vecCoef))) for _ in models]
    arrayToDrawPre = [np.zeros((len(vecThres),len(vecCoef))) for _ in models]
    arrayToDrawRec = [np.zeros((len(vecThres),len(vecCoef))) for _ in models]
    arrayToDrawScore = [np.zeros((len(vecThres),len(vecCoef))) for _ in models]
    
    compt=Utils.initProgress(range(len(vecThres)*len(vecCoef)*nbIter), 1)
    for x in range(len(vecThres)):
        for y in range(len(vecCoef)):
            posThreshold = vecThres[x]
            coefSizeSample = vecCoef[y]
            meanE = [0]*len(models)
            nbTPE = [0]*len(models)
            nbFPE = [0]*len(models)
            nbFNE = [0]*len(models)
            for _ in range(nbIter):
                compt=Utils.updateProgress(compt)
                (_, meanErrors, nbTP, nbFP, nbFN) = learning(X,Y,models,posThreshold,coefSizeSample)
                meanE = np.add(meanE, meanErrors)
                nbTPE = np.add(nbTPE, nbTP)
                nbFPE = np.add(nbFPE, nbFP)
                nbFNE = np.add(nbFNE, nbFN)
            meanE = 1.0*meanE/nbIter
            nbTPE = 1.0*nbTPE/nbIter
            nbFPE = 1.0*nbFPE/nbIter
            nbFNE = 1.0*nbFNE/nbIter
            for i in range(len(models)):
                arrayToDraw[i][x][y] = 100.0*meanE[i]
                arrayToDrawPre[i][x][y] = 100.0*nbTPE[i]/(nbTPE[i]+nbFPE[i])
                arrayToDrawRec[i][x][y] = 100.0*nbTPE[i]/(nbTPE[i]+nbFNE[i])
                arrayToDrawScore[i][x][y] = 2.0*(arrayToDrawPre[i][x][y]*arrayToDrawRec[i][x][y])/(arrayToDrawPre[i][x][y]+arrayToDrawRec[i][x][y])
    for i in range(len(models)):
        s = str(models[i]).split("(")[0]
        DrawingTools.createHistogram2DFromArray(arrayToDraw[i], 
                                                xbins = dicThres, 
                                                ybins = dicCoef, 
                                                zmin = 0, zmax=100, 
                                                xlabel = "positive threshold", 
                                                ylabel = "coef size sample", 
                                                name = "mean Error of model "+s, 
                                                filename = "hist2dmeanError"+s) 
        DrawingTools.createHistogram2DFromArray(arrayToDrawPre[i], 
                                                xbins = dicThres, 
                                                ybins = dicCoef, 
                                                zmin = 0, zmax=100, 
                                                xlabel = "positive threshold", 
                                                ylabel = "coef size sample", 
                                                name = "precision of model "+s, 
                                                filename = "hist2dPreError"+s)  
        DrawingTools.createHistogram2DFromArray(arrayToDrawRec[i], 
                                                xbins = dicThres, 
                                                ybins = dicCoef, 
                                                zmin = 0, zmax=100, 
                                                xlabel = "positive threshold", 
                                                ylabel = "coef size sample", 
                                                name = "recall of model "+s, 
                                                filename = "hist2dRecError"+s) 
        DrawingTools.createHistogram2DFromArray(arrayToDrawScore[i], 
                                                xbins = dicThres, 
                                                ybins = dicCoef, 
                                                zmin = 0, zmax=20, 
                                                xlabel = "positive threshold", 
                                                ylabel = "coef size sample", 
                                                name = "Global Score of the Model "+s, 
                                                filename = "hist2dScoreError"+s)        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    