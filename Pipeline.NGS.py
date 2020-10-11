#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
'''
***********************************************************
* Author  : Zhou Wei                                      *
* Date    : 2020/09/09 10:42:02                           *
* E-mail  : welljoea@gmail.com                            *
* Version : --                                            *
* You are using the program scripted by Zhou Wei.         *
* If you find some bugs, please                           *
* Please let me know and acknowledge in your publication. *
* Thank you!                                              *
* Best wishes!                                            *
***********************************************************
'''
import pandas as pd
import numpy as  np
import re
import os
import sys
import time
import traceback
from Logger  import Logger
from ArgsPipe import Args

'''
pd.set_option('display.max_rows', 100000)
pd.set_option('display.max_columns', 100000)
pd.set_option('display.width', 100000)
'''

from joblib import Parallel, delayed
class MakeFlow:
    def __init__(self, arg, log,  *array, **dicts):
        self.arg = arg
        self.log = log
        self.array  = array
        self.dicts  = dicts
        self.wdir = self.arg.outdir + '/WorkShell'
        os.makedirs(self.wdir, exist_ok=True)
        self.Pipeline={
            'ATAC': '/share/home/share/Pipeline/13SCATAC/ATACPipe',
            'SS2': '/share/home/share/Pipeline/12SCRNA/SmartSeq2Pipe',
            'PLASMID' : '/share/home/share/Pipeline/01NGSDNA/PlasmidPipe',
        }

    def mkinfo(self, d ):
        infors='''
SID={Sampleid}
RID={Uniqueid}
Lane={Lane}
Rep={Rep}
AID={AID}
TID={TID}
LID={LID}
AIDs=({AIDs})
TIDs=({TIDs})
LIDs=({LIDs})
Group={Group}
R1={R1}
R2={R2}
OUT={Outdir}
AUT={Outdir}/{Sampleid}/{Module}/
Species={Species}
Module={Module}
WORKFLOW_DIR={WORKFLOW_DIR}'''.format(**d).strip()
        f=open('%s/%s_%s.input'%(self.wdir, d['AID'], d['Module'] ), 'w')
        f.write(infors)
        f.close()


    def mkshell(self, df):
        allmf = []
        for _, _d in df[['Sampleid','Module','WORKFLOW_DIR','Outdir']].drop_duplicates().iterrows():
            os.system('sh %s/Create_Makeflow.sh %s %s %s'%(_d['WORKFLOW_DIR'], self.wdir, _d['Sampleid'], _d['Module']) )
            allmf.append( '{0}/{1}/{2}/WorkShell/{1}_{2}.mf'.format(_d.Outdir, _d.Sampleid, _d.Module))
        os.system( 'cat %s > %s/all_makeflow.mf'%(' '.join(allmf), self.wdir) )

        if self.arg.qsub:
            os.system( 'nohup makeflow -T sge %s/all_makeflow.mf &'%self.wdir )

    def mkflow(self):
        infodf = pd.read_csv(self.arg.input, sep='\t')
        infodf.columns = infodf.columns.str.capitalize()
        infodf['Outdir'] = infodf['Outdir'].fillna(self.arg.outdir)
        infodf['Module'] = infodf.Module.str.upper()
        infodf['WORKFLOW_DIR'] = infodf.Module.map(self.Pipeline)
        infodf[['R1','R2']] = infodf['Fastq'].str.split('[;,]',expand=True)
        infodf['AID'] = infodf['Sampleid'].str.cat( infodf[['Rep', 'Lane']].astype(str),sep='__')
        infodf['TID'] = infodf['Sampleid'].str.cat( infodf[['Rep' ]].astype(str),sep='__')
        infodf['LID'] = infodf['Sampleid'].str.cat( infodf[['Lane']].astype(str),sep='__')

        infodf = infodf\
                    .merge( infodf.groupby(['Sampleid', 'Rep'] )['AID'].apply(lambda x : ' '.join(x)).to_frame('LIDs').reset_index(),
                            on=['Sampleid','Rep'], how='outer')\
                    .merge( infodf.groupby('Sampleid')['AID'].apply(lambda x : ' '.join(x)).to_frame('AIDs').reset_index(), 
                            on='Sampleid', how='outer' )\
                    .merge( infodf.groupby('Sampleid')['TID'].unique().apply(lambda x : ' '.join(x)).to_frame('TIDs').reset_index(), 
                            on='Sampleid', how='outer' )
        infodf = infodf.astype(str).fillna('NA')

        Parallel( n_jobs=-1 )\
                ( delayed( self.mkinfo )(_d.to_dict()) for _, _d in infodf.iterrows() )
        self.mkshell(infodf)

class Pipeline():
    def __init__(self, arg, log,  *array, **dicts):
        self.arg = arg
        self.log = log
        self.array  = array
        self.dicts  = dicts

    def Pipe(self):
        if self.arg.commands in ['mkflow', 'Auto']:
            MakeFlow(self.arg, self.log).mkflow()

def Commands():
    info ='''
***********************************************************
* Author : Zhou Wei                                       *
* Date   : %s                       *
* E-mail : welljoea@gmail.com                             *
* You are using The scripted by Zhou Wei.                 *
* If you find some bugs, please email to me.              *
* Please let me know and acknowledge in your publication. *
* Sincerely                                               *
* Best wishes!                                            *
***********************************************************
'''%(time.strftime("%a %b %d %H:%M:%S %Y", time.localtime()))

    args = Args()
    Log = Logger( '%s/%s_log.log'%(args.outdir, args.commands) )
    os.makedirs( os.path.dirname(args.outdir) , exist_ok=True)

    Log.NI(info.strip())
    Log.NI("The argument you have set as follows:".center(59, '*'))
    for i,k in enumerate(vars(args),start=1):
        Log.NI('**%s|%-13s: %s'%(str(i).zfill(2), k, str(getattr(args, k))) )
    Log.NI(59 * '*')

    try:
        Pipeline(args, Log).Pipe()
        Log.CI('Success!!!')
    except Exception:
        Log.CW('Failed!!!')
        traceback.print_exc()
    finally:
        Log.CI('You can check your progress in log file.')
Commands()
