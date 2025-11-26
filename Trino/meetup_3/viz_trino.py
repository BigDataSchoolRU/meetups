import json, math
from datetime import datetime,timedelta
import pandas as pd
import plotly.express as px

from trsql_h import _sql

RESDF = None # датафрейм для отображения в виде таблицы если нужно

def drawQuery(q,filt=None,useBlocked=False,useCpuTime=False):
    """ отрисовывает выполнившийся запрос """

    pref = "select query_id from system.runtime.queries where query not like '%system.runtime.queries%' and"
    if filt: # фильтруем по вхождению комментария
        qId = _sql(f"""{pref} query like '%{filt}%' order by "end" desc""",100).values.tolist()[0][0]
    else:
        qId = _sql(f"""{pref} query='{q}' order by "end" desc""",100).values.tolist()[0][0]
    
    print("QUERY ID:",qId)
    
    qDf = _sql(f"""
        select node_id, task_id, stage_id, raw_input_rows, output_rows, start, "end", split_blocked_time_ms, split_cpu_time_ms, splits 
        from system.runtime.tasks where query_id='{qId}'
        """,1000)

    return prepFig(qDf,useBlocked,useCpuTime)

hData = {
    'durSecs': True,
    'task_id': False,
    'raw_input_rows': ':,d',
    'output_rows': ':,d',
    'splits':  ':,d',
}
"""
    'start': False,
    'end': False,
    'duration': True,
    'durSecs': False,
    'execTime': ':,d',
    'inputBytes': ':,d',
    'outputBytes': ':,d',
    'shuffleReadBytes': ':,d',
    'shuffleWriteBytes': ':,d',
"""

def prepFig(pdDf,useBlocked=False,useCpuTime=False,colorParName="raw_input_rows"):
    """ возвращает Plotly Figure """

    global RESDF

    FMT = '%Y-%m-%d %H:%M:%S.%f UTC'
    
    # преобразуем сырые данные в вид, который хорош для отображения
    # node_id, task_id, stage_id, raw_input_rows, output_rows, start, end, split_blocked_time_ms, split_cpu_time_ms, splits
    newList = []
    qId = None
    for row in pdDf.values.tolist():
        # выдерем query_id
        if not qId:
            qId = row[1].split(".")[0]
        # сформируем имя таска
        stageNo = int(row[1].split(".")[1])
        taskNo = int(row[1].split(".")[2])
        taskStr = f"""{row[0]}_s{stageNo if stageNo>9 else "0"+str(stageNo)}_t{taskNo if taskNo>9 else "0"+str(taskNo)}"""
        # преоразуем таймстампы в даты
        st = datetime.strptime(row[5], FMT)
        if useBlocked: # не очень аккуратно: это сумма, но как ее разложить по всем драйверам... мы просто поделим на сплиты
            delayEstimate = int(row[7]) / int(row[9]) # колонка split_blocked_time_ms содержит суммарную задержку для всех сплитов таска
            secs = math.floor(delayEstimate/1000)
            msecs = int(delayEstimate) % 1000
            st += timedelta(seconds=secs,milliseconds=msecs)
        # st = row[5].replace(" UTC","").replace(" ","T")
        en = datetime.strptime(row[6], FMT)
        if useCpuTime: # здесь тоже пока не все ровно - при таком вычислении момента начала он получается раньше, чем факт (для длинных тасков)
            secs = math.floor(int(row[8])/1000)
            msecs = int(row[8]) % 1000
            st = en - timedelta(seconds=secs,milliseconds=msecs)
        durSecs = round((en - st).total_seconds(),2)
        newList.append((taskStr,row[3],row[4],st,en,durSecs,row[9]))

    RESDF = pd.DataFrame(newList, columns=["task_id", "raw_input_rows", "output_rows", "start", "end", "durSecs", "splits"])
    # df = df.sort_values(by=["task_id"])
    fig = px.timeline(
        RESDF, x_start="start", x_end="end", y="task_id", color=colorParName, 
        hover_name="task_id", hover_data=hData
    )

    fig.update_yaxes(categoryorder="category descending")

    return fig
