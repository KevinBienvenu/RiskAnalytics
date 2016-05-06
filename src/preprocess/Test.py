# -*- coding: utf-8 -*-
'''
Created on 5 Apr 2016

@author: Kévin Bienvenu

'''
import pandas as pd
import numpy as np
from PaiementDataExtraction import *


# printConfiguration(False)

# importAndAnalyseCsv(True,False,ftp=True)


# csvEtab = FTPTools.retrieveFtplib("cameliaScores.csv.bz2", compression = "bz2", toPrint=True)
# i=0
# for line in csvEtab.values:
#     print line
#     i+=1
#     if i>10:
#         break
# a = [0,1,np.nan,-3]
# print sum([1 if b>0 or b<0 else 0 for b in a])


# analyzingEntrepScore(False)
# printLastGraphs("scoreFile")

analyzingEntrepEtab(False)
# printLastGraphs("etabFile")


# p = pd.DataFrame(np.random.randint(0,10,size=(5,2)), columns=['1','2'])
# q = pd.DataFrame(np.random.randint(0,10,size=(5,2)), columns=['1','4'])
# 
# print p
# print q
# print p.merge(q, on='1')










