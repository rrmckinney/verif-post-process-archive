#!/usr/bin python

"""
Created in 2023
@author: Reagan McKinney

Code to test out sklearn confusion_matrix package

"""

import os
import pandas as pd
import numpy as np
from datetime import timedelta
import sys
import math
import copy
from scipy import stats
import sqlite3
import warnings
from sklearn.metrics import jaccard_score
warnings.filterwarnings("ignore", category=RuntimeWarning)

N = int(100000)

tru = np.random.rand(N)
pred = np.random.rand(N)
print(tru)
print(pred)
threshold = np.percentile(tru, 66)

for i in range(len(tru)):
    if tru[i] < 0.2:
        tru[i] = 0
    elif tru[i] > 0.2 and tru[i] < threshold:
        tru[i] = 1
    elif tru[i] > threshold:
        tru[i] = 2
    else:
        tru[i] = None

for i in range(len(pred)):
    if pred[i] < 0.2:
        pred[i] = 0
    elif pred[i] > 0.2 and pred[i] < threshold:
        pred[i] = 1
    elif pred[i] > threshold:
        pred[i] = 2
    else:
        pred[i] = None
print(tru)
print(pred)
FP = np.logical_and(tru != pred, pred != 0).sum()
FN = np.logical_and(tru != pred, pred == 0).sum()
TP = np.logical_and(tru == pred, tru != 0).sum()
TN = np.logical_and(tru == pred, tru == 0).sum()

POD = TP/(TP+FN)
POFD = FP/(TN+FP)

PSS = (TP/(TP+FN)) - (FP/(FP+TN))
HSS = POD - POFD

CSI = TP/(TP + FP + FN)

C = (TP + FP)*(TP + FN)/N
GSS = (TP - C)/(TP + FP + FN - C)


print("TN: "+str(TN))
print("FN: "+str(FN))
print("TP: "+str(TP))
print("FP: "+str(FP))
print("POD: "+str(POD))
print("POFD: "+str(POFD))
print("PSS: "+str(PSS))
print("HSS: "+str(HSS))
print("CSI: "+str(CSI))
print("GSS: "+str(GSS))
