#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
'''
***********************************************************
* Author  : Zhou Wei                                      *
* Date    : 2020/09/08 11:04:28                           *
* E-mail  : welljoea@gmail.com                            *
* Version : --                                            *
* You are using the program scripted by Zhou Wei.         *
* If you find some bugs, please                           *
* Please let me know and acknowledge in your publication. *
* Thank you!                                              *
* Best wishes!                                            *
***********************************************************
'''
import pybedtools as bt
import pandas as pd
import numpy as  np
import re
import os
import sys
'''
pd.set_option('display.max_rows', 100000)
pd.set_option('display.max_columns', 100000)
pd.set_option('display.width', 100000)
'''

class MergePeaks():
    def __init__(self, arg, log, *array, **dicts):
        self.arg = arg
        self.log = log
        self.array = array
        self.dicts = dicts
        self.head_n = ['chr', 'start', 'end', 'name', 'score', 'strand', 'signalValue', 'pvalue', 'qvalue', 'peak']
        self.merg_n = ['chr', 'start', 'end', 'starts', 'ends', 'name', 'score', 'strand', 'signalValue', 'pvalue', 'qvalue', 'peak', 'sample']
        self.bedfiles = [i.strip() for i in re.split( '[,;@]', self.arg.bedfiles )]
        if self.arg.IDs == 'infer':
            self.IDs = [ '_'.join( os.path.basename(i).split('_')[:-1] ) for i in self.bedfiles ]
        else:
            self.IDs = [i.strip() for i in re.split( '[,;@]', self.arg.IDs )]
        if len(self.IDs) != len(self.bedfiles):
            self.log.CW("the bedfile length is not equal with the IDs's.")
            sys.exit(0)

    def BTmerge(self, peaktype='narrow'):
        Beddf = []
        for i in range(len(self.bedfiles)):
            k = pd.read_csv(self.bedfiles[i], sep='\t',header=None)
            k.insert(10, 10, self.IDs[i] )
            Beddf.append(k)
        Beddf = bt.BedTool.from_dataframe( pd.concat(Beddf, axis=0) )
        #Bedsort = bt.BedTool(self.bedfiles[0])
        #Bedsort = Bedsort.cat(*self.bedfiles[1:], postmerge=False)
        Beddf = Beddf.sort()\
                    .merge( c=','.join([ str(i) for i in range(2,12)]),
                            o=','.join(['collapse']*10))\
                    .to_dataframe(disable_auto_names=True, header=None,names=self.merg_n)
        Beddf.to_csv('%s/merge.peaks_bedtools.xls'%(self.arg.outdir), sep='\t', header=True,index=False)
        return Beddf

    def Peakpivot(self):
        pass


class Pipeline():
    'The pipeline used for machine learning models'
    def __init__(self, arg, log,  *array, **dicts):
        self.arg = arg
        self.log = log
        self.array  = array
        self.dicts  = dicts

    def Pipe(self):

        if self.arg.commands in ['mergepeak', 'Auto']:
            MergePeaks(self.arg, self.log).BTmerge()

import argparse
def Args():
    parser = argparse.ArgumentParser(
                formatter_class=argparse.RawDescriptionHelpFormatter,
                prefix_chars='-+',
                conflict_handler='resolve',
                description="",
                epilog="")

    parser.add_argument('-V','--version',action ='version',
                version='ATACtools version 0.1')

    subparsers = parser.add_subparsers(dest="commands",
                    help='models help.')
    P_Common = subparsers.add_parser('Common',conflict_handler='resolve', #add_help=False,
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                    help='The common parameters used for other models.')
    P_Common.add_argument("-i", "--input",type=str,
                    help='''the input file. You can use comma, semicolon or at to split multiple files''')
    P_Common.add_argument("-o", "--outdir",type=str,default=os.getcwd(),
                    help="output file dir, default=current dir.")
    P_Common.add_argument("-p", "--prefix",type=str,default='',
                    help="output file header, default=None.")

    P_mgpeak = subparsers.add_parser('mergepeak', conflict_handler='resolve', add_help=False)
    P_mgpeak.add_argument("-B", "--bedfiles", type=str,
                    help="Input bed files. You can use comma, semicolon or at to split multiple files")
    P_mgpeak.add_argument("-I", "--IDs", type=str, default='infer',
                    help="Input ID. The ID is in agreement with the input file by order and number. You can use comma, semicolon or at to split multiple files")
    P_mgpeak.add_argument("-mp", "--mergepara", type=str, default='',
                    help="the bedtools merge parameters.")
    P_mgpeak.add_argument("-ps", "--peaksoft", type=str, default='macs2', choices=['macs','genrich'],
                    help="the software of call peaks, sucn as macs, genrich.")
    P_mgpeak.add_argument("-pt", "--peaktype", type=str, default='narrow', choices=['narrow','broad'],
                    help="the type of call peaks, sucn as narrow, broad.")
    P_Mgpeak = subparsers.add_parser('mergepeak',conflict_handler='resolve',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                    parents=[P_Common,P_mgpeak],
                    help='merge peaks.')

    P_Autopipe = subparsers.add_parser('Auto', conflict_handler='resolve', prefix_chars='-+',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                    parents=[P_Common, P_mgpeak],
                    help='the auto-processing for all.')
    P_Autopipe.add_argument("+P", "++pipeline",nargs='+',
                    help="the auto-processing: standardization, feature selection, Fitting and/or Prediction.")
    P_Autopipe.add_argument('+M','++MODEL' , nargs='+', type=str, default=['Standard'],
                    help='''Chose more the one models from Standard, Fselect,Fitting and Predict used for DIY pipline.''')
    args  = parser.parse_args()
    return args


import logging
class DispatchingFormatter:
    def __init__(self, formatters, default_formatter):
        self._formatters = formatters
        self._default_formatter = default_formatter

    def format(self, record):
        formatter = self._formatters.get(record.name, self._default_formatter)
        return formatter.format(record)

class Logger:
    level_dict = {
        'NOTSET'  : logging.NOTSET,
        'DEBUG'   : logging.DEBUG,
        'INFO'    : logging.INFO,
        'WARNING' : logging.WARNING,
        'ERROR'   : logging.ERROR,
        'CRITICAL': logging.CRITICAL,
    }

    ChangeFrom = DispatchingFormatter(
            { 'c' : logging.Formatter( '[%(asctime)s] [%(levelname)-4s]: %(message)s', '%Y-%m-%d %H:%M:%S'),
              'p' : logging.Formatter( '[%(levelname)-4s]: %(message)s'),
              'n' : logging.Formatter( '%(message)s' ),
            }, 
            logging.Formatter('%(message)s')
     )

    def __init__(self, outpath, filemode='w',  clevel = 'INFO', Flevel = 'INFO'):

        logging.basicConfig(
            level    = Logger.level_dict[clevel] ,
            format   = '[%(asctime)s] [%(levelname)-4s]: %(message)s',
            datefmt  = '%Y-%m-%d %H:%M:%S',
            filename = None,
        )

        File = logging.FileHandler(outpath,  mode= filemode)
        File.setLevel(Logger.level_dict[Flevel])
        File.setFormatter(Logger.ChangeFrom)
        logging.getLogger().addHandler(File)

        self.R = logging
        self.C = logging.getLogger('c')
        self.P = logging.getLogger('p')
        self.N = logging.getLogger('n')
        self.CI = logging.getLogger('c').info
        self.NI = logging.getLogger('n').info
        self.CW = logging.getLogger('c').warning
        self.NW = logging.getLogger('n').warning

import os
import time
import traceback
def Commands():
    info ='''
>^o^<
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