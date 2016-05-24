# -*- coding: utf-8 -*-
'''
Created on 13 mai 2016

@author: Kévin Bienvenu
'''

import EntrepriseLearning
from preprocess import CameliaBalAGPreprocess

# csvinput = CameliaBalAGPreprocess.importAndCleanCsv(False, ftp=True, toSave=False)
# 
# csvinput.sample(n=100000).to_csv("cameliaBalAGrandom.csv")

EntrepriseLearning.preprocessData(True)
    
# (X,Y) = EntrepriseLearning.importPreprocessData()
# 
# 
# EntrepriseLearning.learning(X, Y)


# with open("preprocessedDataBalAGclean.csv","r") as fichier:
#     local = fichier.readlines()
#         
# with open("preprocessedDataBalAGclean.csv","w") as fichier:
#     fichier.write("index\tentrep_id\tmontantPieceEur\tlogMontant\techeance\tY\tyear\tcapital\teffectif\tage\tscore\tscoreSolv\tscoreZ\tscoreCH\tscoreAltman\t\n")
#     flag= True
#     for line in local:
# #         fichier.write(line)
#         if flag:
#             flag = False
#         else:
#             fichier.write(line)

