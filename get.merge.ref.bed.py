#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
***********************************************************
* @File    : get.merge.ref.bed.py
* @Author  : Zhou Wei                                     *
* @Date    : 2020/09/28 12:13:07                          *
* @E-mail  : welljoea@gmail.com                           *
* @Version : --                                           *
* You are using the program scripted by Zhou Wei.         *
* If you find some bugs, please                           *
* Please let me know and acknowledge in your publication. *
* Thank you!                                              *
* Best wishes!                                            *
***********************************************************
'''

# Please start your show!

import pandas as pd
import numpy as np

IN='/data/zhouwei/02production/20200721_17170/3T3-ATAC-sc4/ATAC/WorkShell/merge.peaks_bedtools.xls'
INdf=pd.read_csv(IN,sep="\t")
Ref_bed=pd.concat( [INdf.groupby('chr')['start'].min(), INdf.groupby('chr')['end'].max()],axis=1)
Ref_bed.to_csv('merge.peaks_REF.bed', sep='\t', header=False)

