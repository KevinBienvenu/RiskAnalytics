# -*- coding: utf-8 -*-
'''
Created on 15 avr. 2016

@author: Kevin Bienvenu
'''

import datetime
import math
import os
import csv
import random

from sklearn import kernel_ridge
from sklearn import linear_model
from sklearn import naive_bayes

import numpy as np
import pandas as pd
from preprocess import PaiementDataExtraction


def importEntreprise(csvinput, entrep_id):
    '''
    function that extracts all the entries from the database
    that have the matching id. 
    -- IN
    csvinput : panda dataframe that should have a column 'entrep_id' (dataframe)
    entrep_id : id of the entreprise to extract (int)
    -- OUT
    csvoutput : panda dataframe containing only matching entries (dataframe or None if an error occurs)
    '''
    print "Starting extraction of dataframe"
    # checking instance of inputs
    if not isinstance(csvinput, pd.DataFrame):
        print "error : csvinput must be an instance of pandas.Dataframe"
        return None
    # checking if 'entrep_id' is in columns
    if not 'entrep_id' in list(csvinput.columns.values):
        print "error : 'entrep_id not in columns of input dataframe"
        return None
    # checking if entrep_id is an int
    try:
        int(entrep_id)
    except:
        print "error : entrep_id must be an integer"
        return None
    # keeping only the selected rows
    csvoutput =  csvinput[csvinput['entrep_id']==entrep_id]
    print "Extraction completed : ",len(csvoutput),"rows selected"
    return csvoutput

def extractMostRepresentedEntreprise(csvinput):
    '''
    Function that go through the dataframe and return the most represented entreprise
    -- IN
    csvinput : panda dataframe that should have a column 'entrep_id' (dataframe)
    -- OUT
    entrep_id : id of the entreprise with the most entries in the dataframe (int)
        returns 0 if an error occurs
    '''
    print "Extracting the most represented entreprise"
    # checking instance of inputs
    if not isinstance(csvinput, pd.DataFrame):
        print "error : csvinput must be an instance of pandas.Dataframe"
        return 0
    # checking if 'entrep_id' is in columns
    if not 'entrep_id' in list(csvinput.columns.values):
        print "error : 'entrep_id not in columns of input dataframe"
        return 0
    print "progress",
    # dictionary that stores the id and occurences
    idOccursDict = {}
    # displaying progress
    stepSize = len(csvinput)/10.0
    step =  stepSize
    ind = 0
    for idIterator in csvinput['entrep_id'].values:
        if not idOccursDict.has_key(idIterator):
            idOccursDict[idIterator] = 0
        idOccursDict[idIterator] += 1
        if ind>step:
            step+=stepSize
            print".",
        ind += 1
    print "done"
    # looking for the maximal value
    maxOccurance = 0
    entrep_id = 0
    for key in idOccursDict.keys():
        if idOccursDict[key]>maxOccurance:
            maxOccurance = idOccursDict[key]
            entrep_id = key
    if entrep_id==0:
        "Warning in extractMostRepresentedEntreprise: no entreprise found in input"
    return entrep_id
  
def preprocessDates(csvinput):
    dateRef = datetime.datetime.strptime("2010-01-01","%Y-%m-%d").date()
    X = []
    Y = []
    j=0
    for line in csvinput.values:
        dayPiece = (datetime.datetime.strptime(line[1],"%Y-%m-%d").date()-dateRef).days
        dayEcheance = (datetime.datetime.strptime(line[2],"%Y-%m-%d").date()-dateRef).days
        dayDernierPaiement = (datetime.datetime.strptime(line[3],"%Y-%m-%d").date()-datetime.datetime.strptime(line[1],"%Y-%m-%d").date()).days
        monthPiece = datetime.datetime.strptime(line[1],"%Y-%m-%d").date().month
        X.append([dayPiece,dayEcheance,line[4],monthPiece])
#         X.append([dayPiece,line[4]])
        Y.append(dayDernierPaiement)
        j+=1
    return (X,Y)

def preprocessData(toExportCsv = False):
    '''
    function that imports all csv files, and computes the X and Y vectors to perform learning algorithm
    -- IN:
    toExportCsv : boolean that settles if a csv file must be computed and stored (boolean) default=False
    -- OUT:
    X : vector of features
    Y : vector of observations
    '''
    # initializing variables
    entrep_id = []
    dates = []
    X = []
    Y = []
    # importing the BalAG file
    csvinput = PaiementDataExtraction.importAndCleanCsv(toPrint=False, ftp=True)
    for line in csvinput[['entrep_id','datePiece','dateEcheance','dateDernierPaiement','montantPieceEur']].values:
        entrep_id.append(int(line[0]))
        dates.append(int(line[1][:4]))
        montant = int(line[4])
        logmontant = math.log10(montant)
        echeance = (datetime.datetime.strptime(line[2],"%Y-%m-%d").date()-datetime.datetime.strptime(line[1],"%Y-%m-%d").date()).days
        X.append([echeance,montant,logmontant])
        Y.append(-1 if line[3]=="0000-00-00" else 1)
    # importing the Etab file
    dicCsvEtab = PaiementDataExtraction.getAndPreprocessCsvEtab(csvinput)
    rowsToDrop = []
    # merging files and removing incomplete rows
    for i in range(len(X)):
        if not(entrep_id[i] in dicCsvEtab):
            rowsToDrop.append(i)
        for l in range(dicCsvEtab[entrep_id[i]]):
            X[i].append(dicCsvEtab[entrep_id[i]][l])
    print "missing information :",100.0*len(rowsToDrop)/len(X),"%"
    del X[rowsToDrop]
    del Y[rowsToDrop]
    del entrep_id[rowsToDrop]
    del dates[rowsToDrop]
    # importing score file
    dicCsvScore = PaiementDataExtraction.getAndPreprocessCsvScore(csvinput)
    del csvinput
    rowsToDrop = []
    # merging files and removing incomplete rows
    for i in range(len(X)):
        if not(entrep_id[i] in dicCsvScore):
            rowsToDrop.append(i)
        if dates[i] in dicCsvScore[entrep_id[i]]:
            X[i]+=dicCsvScore[entrep_id[i]][dates[i]]
        elif dates[i]-1 in dicCsvScore[entrep_id[i]]:
            X[i]+=dicCsvScore[entrep_id[i]][dates[i]-1]
        else:
            rowsToDrop.append(i)
    print "missing information :",100.0*len(rowsToDrop)/len(X),"%"
    del X[rowsToDrop]
    del Y[rowsToDrop]
    del entrep_id[rowsToDrop]
    del dates[rowsToDrop]
    
    # creating the csvfile
    columns = ['echeance','montant','logmontant', \
               'capital','dateCreation','effectif', \
               'scoreSolv','scoreZ','scoreCH','scoreAltman']
    if toExportCsv:
        with open("preprocessedDataBalAGX.csv","w") as csvfile:
            writer = csv.writer(csvfile, delimiter='\t')
            writer.writerow(columns)
            for i in range(len(X)):
                writer.writerow(X[i][1:])
        with open("preprocessedDataBalAGY.csv","w") as csvfile:
            writer = csv.writer(csvfile, delimiter='\t')
            writer.writerow(['Y'])
            for i in range(len(X)):
                writer.writerow(Y[i])
    return X,Y
       
def outOfNowhere(cumm,val):
    rd = random.random()
    return val[np.max([i for i in range(len(cumm)) if cumm[i]<rd]+[0])]+(int)(random.random()*5)
    
     


def learning():
    csvinput = PaiementDataExtraction.importAndCleanCsv(ftp=False)
    # entrep_id = extractMostRepresentedEntreprise(csvinput)
    # csvinput = importEntreprise(csvinput, entrep_id)
    
    # PaiementDataExtraction.analysingMontant(csvinput, False, False)
    # PaiementDataExtraction.analysingDates(csvinput, False, False)
    
    csvinput.reindex(np.random.permutation(csvinput.index))
    
    nbEntries = len(csvinput)
    nbEntriesTraining = int(nbEntries*0.8)
    (X,Y) = preprocessDates(csvinput)
    
    daydelai=[0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95,100]
    probdelai=[1981,20054,80690,146931,169801,177178,406872,410590,200216,178821,167917,164852,287406,186486,114643,77861,45019,29730,22581,15874,30020]
    
    total = np.sum(probdelai)
    probdelai = [1.0*i/total for i in probdelai]
    cummdelai = [np.sum([j for j in probdelai[:i+1]]) for i in range(len(probdelai))]
    
    print cummdelai
    
    trainX = X[:nbEntriesTraining]
    testX = X[nbEntriesTraining:]
    
    trainY = Y[:nbEntriesTraining]
    testY = Y[nbEntriesTraining:]
    
    print "length of the training set:", len(trainX)
    print "length of the testing set:", len(testX)
    
    # list of models
    models = []
    models.append(linear_model.LinearRegression())
    models.append(linear_model.Lasso(alpha = 0.1,max_iter=100000))
    models.append(kernel_ridge.KernelRidge(alpha = 10))
    models.append(naive_bayes.GaussianNB())
    
    # training models
    for model in models:
        model.fit(trainX,trainY)
    
    # predicting
    testYmodels = []
    for model in models:
        testYmodels.append(model.predict(testX))
        
    # power of random
    testYrandom = []
    testYrandom = [outOfNowhere(cummdelai, daydelai) for i in testX]
    
    
    errors = [[np.abs(testYm[i]-testY[i]) for i in range(len(testY))] for testYm in testYmodels]
    errors.append([np.abs(testYrandom[i]-testY[i]) for i in range(len(testY))])
    print "mean error:",np.mean(errors,axis=1)
    print "max error:",np.max(errors,axis=1)
    print "min error:",np.min(errors,axis=1)
