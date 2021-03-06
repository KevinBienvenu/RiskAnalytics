# -*- coding: utf-8 -*-
'''
Created on 5 Apr 2016

@author: Kévin Bienvenu

Module containing the constants of the preprocess part
Including threshold and behavior boolean
Convention : 
 = all boolean's names begin with 'b'
 = all corresponding values are without the 'b'
'''

import datetime
import numpy as np


### About creating the dataframe
# specifying the data type of columns in the file cameliaBalAG.csv
dtype = {"entrep_id":np.int32,
         "datePiece":np.str,
         "dateEcheance":np.str,
         "dateDernierPaiement":np.str,
         "montantPieceEur":np.int32,
         "montantLitige":np.int32,
         "devise":np.str,
         "dateInsert":np.str}


### About printing graphs
# label by month
labelByMonth = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

## About the new graphs
# boolean that settles if we store the graphs locally before printing them with plotly
bToStoreBeforeDrawing = True
#colors
colorBluePlotly = 'rgb(28,173,228)'
colorGreenPlotly = 'rgb(39,206,213)'
colorOrangePlotly = 'rgb(62,136,83)'

## About the old graphs
# static variables useful to print graphs in the analyzing functions
graphId = 1
figureId = 1
# size of the graph windows
figsize = (18,12)
# subplot shape 'RC0' where R is the number of row, C the number of columns. 
# R*C must always be strictly lower than 10
subplotShapeDates = 220
subplotShapeEnterprises = 210
subplotShapeMontants = 110
# spaces of the bars
barSpace = 0.40
# colors
colorBlue =(0.0,153.0/255,153.0/255.0)
colorGreen =(153.0/255.0,255.0/255,255.0/255.0)
colorOrange = (1.0,153.0/255,0.0)


### CLEANING ID
# clean according to being an int
bclnIdIntFormat = True
# clean according to the number of bills
bclnIdMinimalBillsNumber = False
clnIdMinimalBillsNumber = 10
# clean according to the value of the ID
bclnIdMinimalIdValue = True
clnIdMinimalIdValue = 1
bclnIdMaximalIdValue = False
clnIdMaximalIdValue = 100000000


### CLEANING DATES
# clean according to the date format
bclnDatePieceFormat = True
bclnDateEcheanceFormat = True
bclnDateDernierPaiementFormat = False
# clean according to the consistence of the dates (piece < echeance and piece < dernierpaiement)
bclnDateInconsistent = True
# clean according to a maximal gap in month between dates
bclnDateMonthDiff = False
clnDateMonthDiff = 36
# clean according to a minimal date
bclnDateMinimalDate = True
clnDateMinimalDate = "2010-01-01"
clnDateMinimalDate = datetime.datetime.strptime(clnDateMinimalDate,"%Y-%m-%d").date()
# clean according to a maximal date
bclnDateMaximalDate = False
clnDateMaximalDate = "2015-12-31"
clnDateMaximalDate = datetime.datetime.strptime(clnDateMaximalDate,"%Y-%m-%d").date()


### CLEANING MONTANTS
# clean according to being an int
bclnMontantIntFormat = True
# clean according to being a positive value
bclnMontantNonNegativeValue = True
# clean according to being an non-zero value
bclnMontantNonZeroValue = True
# clean according to a minimal value of the montant
bclnMontMinimalValue = True
clnMontMinimalValue = 1000
# clean according to a maximal value of the montant
bclnMontMaximalValue = True
clnMontMaximalValue = 500000


### CLEANING MONTANT LITIGE
# clean according to a zero-valued montantLitige
bclnMontantLitigeNonZero = False


### ANALYSING DATES
# boolean settling if we analyse mean, var and median or just mean (changes the computation time)
banaDateLargeAnalysis = False
# stepsize for the analysis over the years, the value is in month and by default settled to 12 (one year)
anaDateStepMonthGraph = 12
anaOtherStandardDate = datetime.datetime.strptime("2010-01-13","%Y-%m-%d").date()


### ANALYSING ENTERPRISES
# value of the step size for displaying bill number
anaIdStepSizeBillNumber = 10
# parameter for the vizualisation of the bill numbers
#    the higher it is the more columns there will be
anaIdLogCoefficientBillNumber = 3
# parameter for the vizualisation of the ids values
#    the higher it is the more columns there will be
anaIdLogCoefficientIds = 5.0


### ANALYSING MONTANTS
# parameter for the vizualisation of the montant values
#    the higher it is the more columns there will be
anaIdLogCoefficientMontants = 3


### ANALYSING COMPLETS
# step size for the distribution of the montant
anaCompletStepSizeDelay = 5
# max delay for the distribution of the montant
anaCompletMaximalDelay = 100





















