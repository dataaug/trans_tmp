 # -*- coding: utf-8 -*-
"""
Created on Wed Aug 28 14:28:54 2019

@author: Administrator
"""

# -*- coding: utf-8 -*-
"""
Created on Fri Jul 26 10:12:30 2019

@author: Administrator
"""

import numpy as np
import pandas as pd
import pyreadr
import lmdb
import os
import sys
import re

start_date = '2013-05'
end_date = '2019-01'
input_dir0 = '/data/pkudata/stock-raw-data/indicator_1m/'
input_dir1 = '/data/pkudata/stock-raw-data/indicator_1m_from_LEVEL2_3S/'
output_dir = '/data/pkudata/stock-raw-data/lmdb_1m_v2/'
filename_prefix = 'x'
lookback_n = 2
dbsize = 30* 1024*1024*1024

for arg in sys.argv:
    if re.match('-sd', arg):
        start_date = arg[3:]
    if re.match('-ed', arg):
        end_date = arg[3:]
    if re.match('-bn\d+$', arg):
        lookback_n = int(arg[3:])
    if re.match('-outputdir', arg):
        output_dir = arg[10:]
    if re.match('-size\d+$', arg):
        dbsize = int(arg[5:])
    if re.match('-fnpre', arg):
        filename_prefix = arg[6:]

assert lookback_n >= 0, 'Illegal lookback_n value!'
filelist0 = os.listdir(input_dir0)
start_loc = np.where(np.array(filelist0)>=start_date)[0][0]
filelist0 = filelist0[start_loc-lookback_n:]
filelist1 = os.listdir(input_dir1)
start_loc = np.where(np.array(filelist1)>=start_date)[0][0]
filelist1 = filelist1[start_loc-lookback_n:]
LogCol = ['SumActBuy1M', 'SumActSell1M', 'MeanActBuy1M', 'MeanActSell1M',
          'MaxActBuy1M', 'MaxActSell1M', 'MeanTrade1M', 'MaxTrade1M',
          'MeanB1Amount1M', 'MaxB1Amount1M', 'MeanS1Amount1M', 'MaxS1Amount1M',
          'MaxB1EndAmount1M', 'MaxS1EndAmount1M', 'MaxB1EndNewAmount1M',
          'MaxS1EndNewAmount1M', 'B1To5EndAmount1M', 'S1To5EndAmount1M',
          'MaxB1To5Amount1M', 'MaxS1To5Amount1M', 'B1To10EndAmount1M',
          'S1To10EndAmount1M', 'MaxB1To10Amount1M', 'MaxS1To10Amount1M',
          'MaxB1Order1M', 'MaxS1Order1M', 'MaxB1EndOrder1M', 'MaxS1EndOrder1M',
          'Volume1m', 'Amount1m', 'HighAmount1m', 'MedianAmount1m',
          'LowAmount1m']
    
def load_1ddata(filename0, filename1):
    assert filename0[:10] == filename1[:10], 'Different date in two dataset!'
    df0 = pyreadr.read_r('%s%s'%(input_dir0, filename0))['indicator_1m']
    df0['Symbol'] = [i[2:] for i in df0['Symbol']]
    df1 = pyreadr.read_r('%s%s'%(input_dir1, filename1))['data_1m'].iloc[:, :-4]
    df1['Time'] = df1['minute']
    del df1['minute']
    tdf = df1.merge(df0, left_on = ['Date', 'Time', 'Symbol'], right_on = ['Date', 'Time', 'Symbol'], how = 'inner')
    return tdf

def get_size(x):
    info = x.stat()
    return info['psize']*(info['depth']+info['branch_pages']+info['leaf_pages']+info['overflow_pages'])

df = pd.DataFrame()
nrow = []
date_list = []
for pos in range(lookback_n):
    tdf = load_1ddata(filelist0[pos], filelist1[pos])
    print(filelist0[pos][:10])
    tdf[LogCol] = np.log(1+tdf[LogCol])
    df = df.append(tdf)
    nrow.append(tdf.shape[0])
    date_list.append(tdf.Date.iloc[0])

if not os.path.exists(output_dir):
    os.makedirs(output_dir)
TimeList = ['%02d:%02d:00'%(i//60, i%60) for i in range(571, 691)] + ['%02d:%02d:00'%(i//60, i%60) for i in range(781, 901)]
for pos in range(lookback_n, len(filelist0)):
    if filelist0[pos] > end_date:
        break
    n_date = filelist0[pos][:10]
    print(n_date)
    tdf = load_1ddata(filelist0[pos], filelist1[pos])
    tdf[LogCol] = np.log(1+tdf[LogCol])
    df = df.append(tdf)
    nrow.append(tdf.shape[0])
    date_list.append(n_date)
    with lmdb.open('%s%s_%s'%(output_dir, filename_prefix, filelist0[pos][:7]), map_size = dbsize, subdir = False) as env:
        with env.begin(write = True) as txn:
            txn.replace(key = b'collist', value = bytes(','.join(df.columns[3:]), encoding = 'utf-8'))
            namelist = []
            sym_list = df.Symbol.drop_duplicates()
            for index, sym in enumerate(sym_list):
                if index & 7 == 0:
                    print("\r", index, "/", len(sym_list), end = ' ')
                n_sym = df[df.Symbol == sym]
                if n_sym.Date.iloc[-1] != n_date:
                    continue
                if n_sym.shape[0] <= lookback_n * 240:
                    v = pd.DataFrame({'Date': np.repeat(date_list[pos-lookback_n:pos+1], 240), 'Time': np.repeat(TimeList, lookback_n+1)}).merge(n_sym, on = ['Date', 'Time'], how = 'left').iloc[:,3:].fillna(0).values
                else:
                    v = n_sym.iloc[:,3:].fillna(0).values
                n_key = bytes('%s_%s'%(sym, n_date), encoding = 'utf-8')
                txn.put(key = n_key, value = v.astype(np.float16).tobytes())
                namelist.append(n_key)
            print("\r", len(sym_list), "/", len(sym_list))
            nl0 = txn.get(b'wholemarketlist', b'')
            txn.replace(key = b'wholemarketlist', value = nl0 + b',' + b','.join(namelist))
        env.set_mapsize(get_size(env))
    df = df.iloc[nrow[pos-lookback_n]:]
   