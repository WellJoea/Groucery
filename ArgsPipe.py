#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
'''
***********************************************************
* Author  : Zhou Wei                                      *
* Date    : 2020/09/09 10:54:33                           *
* E-mail  : welljoea@gmail.com                            *
* Version : --                                            *
* You are using the program scripted by Zhou Wei.         *
* If you find some bugs, please                           *
* Please let me know and acknowledge in your publication. *
* Thank you!                                              *
* Best wishes!                                            *
***********************************************************
'''

import argparse
import os
def Args():
    parser = argparse.ArgumentParser(
                formatter_class=argparse.RawDescriptionHelpFormatter,
                prefix_chars='-+',
                conflict_handler='resolve',
                description="",
                epilog="")

    parser.add_argument('-V','--version',action ='version',
                version='NGS pipeline version 0.1')

    subparsers = parser.add_subparsers(dest="commands",
                    help='models help.')
    P_Common = subparsers.add_parser('Common',conflict_handler='resolve', #add_help=False,
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                    help='The common parameters used for other models.')
    P_Common.add_argument("-i", "--input",type=str,
                    help='''the input file. You can use comma, semicolon or at to split multiple files''')
    P_Common.add_argument("-o", "--outdir", type=str, default=os.getcwd(),
                    help="output file dir, default=current dir.")
    P_Common.add_argument("-p", "--prefix",type=str,default='',
                    help="output file header, default=None.")
    P_Common.add_argument("-T", "--jobway",type=str, default='sge',
                    help="select your desired batch system, default=sge.")
    P_Common.add_argument("-q", "--qsub", action='store_true' , default=False,
                    help='''whether submition the task.''')

    P_mkflow = subparsers.add_parser('mkflow', conflict_handler='resolve', add_help=False)
    P_mkflow.add_argument("-ff", "--flowfile", type=str,
                    help="Input makeflow files. You can use comma, semicolon or at to split multiple files")
    P_Mkflow = subparsers.add_parser('mkflow',conflict_handler='resolve',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                    parents=[P_Common,P_mkflow],
                    help='makeflow files.')


    P_Autopipe = subparsers.add_parser('Auto', conflict_handler='resolve', prefix_chars='-+',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                    parents=[P_Common, P_mkflow],
                    help='the auto-processing for all.')
    P_Autopipe.add_argument("+P", "++pipeline",nargs='+',
                    help="the auto-processing: standardization, feature selection, Fitting and/or Prediction.")
    P_Autopipe.add_argument('+M','++MODEL' , nargs='+', type=str, default=['Standard'],
                    help='''Chose more the one models from Standard, Fselect,Fitting and Predict used for DIY pipline.''')
    args  = parser.parse_args()
    return args