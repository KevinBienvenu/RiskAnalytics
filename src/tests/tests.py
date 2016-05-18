# -*- coding: utf-8 -*-
'''
Created on 18 mai 2016

@author: Utilisateur
'''

import pandas as pd
import numpy as np
import math

df = pd.DataFrame({'A' : ['foo', 'bar', 'foo', 'bar',
                          'foo', 'bar', 'foo', 'foo'],
                   'B' : ['one', 'one', 'two', 'three',
                          'two', 'two', 'one', 'three'],
                   'C' : np.random.randn(8),
                   'D' : np.random.randn(8)})

print df

# grouped = df.groupby('A')
# 
# fmin = lambda x: pd.Series([x.min()]*len(x))
# fmax = lambda x: pd.Series([x.max()]*len(x))
# 
# df['C'] = grouped['C'].transform(fmin)
# df['D'] = grouped['D'].transform(fmax)
# 
# df = grouped.head(1)
# 
# print df

log10abs = lambda x : math.log10(abs(x))
df['E'] = df.C.apply(log10abs)

print df