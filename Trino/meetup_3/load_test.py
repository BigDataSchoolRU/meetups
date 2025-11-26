import multiprocessing as mp
import time, datetime

def execQuery(q, msg):
    """ выполняет запрос и логгирует его результат с префиксом """

    from trsql_h import _psql
    
    stMom = time.time()
    stTs = str(datetime.datetime.now())
    res = _psql(q,noPrint=True)
    resOk = len(res.strip())!=0 # если запрос не отработал - будет пустой результат и выдача диагностики на экран
    endMom = time.time()
    endTs = str(datetime.datetime.now())
    with open("log_load.txt","a",1) as fp:
        fp.write(f"START::{msg}::{stTs}\n")
        fp.write(f"RES::{res}\n")
        fp.write(f"END::{msg}::{resOk}::{endMom-stMom}::{stTs}::{endTs}\n")       

def startQueries(q,n,name):
    """ стартует n запросов подготовленных q, возвращает список процессов """

    mp_context = mp.get_context('fork') # работаем в режиме "процессов"
    
    procList = []
    for i in range(n):
        qName = f"{name}_{i}"
        aQuery = f"-- {qName}\n"+q
        p = mp_context.Process(target=execQuery, args=(aQuery,qName))
        procList.append(p)
    for pp in procList:
        pp.start()

    return procList