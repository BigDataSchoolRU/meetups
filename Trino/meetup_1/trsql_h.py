"""
Модуль "оберток" вокруг командной строки к trino
"""

import subprocess
import pandas as pd
from IPython.display import display, HTML
import os
import random
import datetime
from io import StringIO

pd.set_option('display.max_columns', None) # пусть будут показаны все колонки
dsn = """http://10.150.2.5:8080"""

def _getDsn():
    """ возвращает текущий dsn """
    
    return dsn

def _setDsn(d):
    """ устанавливает dsn """
    
    global dsn
    dsn = d

mySchema = "usr02"

def _getSchema():
    """ возвращает текущую установленную схему """
    
    return mySchema

def _setSchema(s):
    """ устанавливает mySchema """
    
    global mySchema
    mySchema = s
    

def _execStr(pStr,format="CSV_HEADER"):
    """ выполняет запрос через командную строку 
    возвращает stdOut, stdErr в виде строк
    """

    cmd = ["/opt/notebooks/bin/trino",dsn]
    # проверим необходимость работы с собственной схемой
    cmd.append(f"--output-format={format}")
    if mySchema:
        if mySchema.find(".")>0: # есть каталог и схема
            cat, sch = mySchema.split(".")
            cmd.append(f"--catalog={cat}")
            cmd.append(f"--schema={sch}")
        else: # только каталог
            cmd.append(f"--catalog={mySchema}")
    cmd.append("--execute="+pStr)
    #print(cmd)
    #print(env)
    res = subprocess.run(cmd,capture_output=True) # ,env=env)#, shell=True)
    
    pRes = res.stdout.decode("utf-8") 
    eRes = res.stderr.decode("utf-8")

    return (res.returncode, pRes, eRes)

def _psql(pStr, noPrint=False):
    """ выполняет запрос через командную строку """

    retCode, pRes, eRes = _execStr(pStr,"ALIGNED")
    if retCode>0:
        print(eRes)

    if noPrint:
        return pRes
    
    display(HTML('<pre style="white-space: pre !important;">{}</pre>'.format(pRes)))
    
def _sql(sqlStr,lines=10):
    """ возвращает датафрейм (первые lines строк) запроса, открывая и закрывая коннекшн """
    
    retCode, pRes, eRes = _execStr(sqlStr)
    if retCode>0:
        print(eRes)
        return None

    #print(pRes)
    return pd.read_csv(StringIO(pRes),nrows=lines) if len(pRes)>0 else None
