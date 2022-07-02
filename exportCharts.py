import datetime
import json
import pymssql
import os
import webbrowser
import pyecharts.options as opts
from pyecharts.charts import Line, Grid

MSSQL_HOST = "128.0.202.103"
MSSQL_USER = "tdsa"
MSSQL_PWD = "Qaz*963/852"
MSSQL_DBNAME = "DataStoreReport"

# 连接数据库：1-读取队列组，2-查询各队列的生产者、消费者的数据
def connect_mssql():
    conn = pymssql.connect(host=MSSQL_HOST, user=MSSQL_USER, password=MSSQL_PWD, database=MSSQL_DBNAME)
    cur = conn.cursor()
    groupSql = "select hosts,exchange,virtualhost,queuesname from [dbo].[RabbitMsInfo] where QueuesName in ('Td365_BillCommitData','Td365_Memberpoint') and virtualhost in ('Star','Yun') group by hosts,exchange,virtualhost,queuesname order by hosts,exchange,virtualhost,queuesname"
    detailSql = "select PublishDetails, ConsumerAckDetails,ReadyDetails from  [dbo].[RabbitMsInfo] where CreateTime > DATEADD(DAY,-14, GETDATE()) and hosts='%s' and Exchange='%s' and VirtualHost='%s' and QueuesName='%s'"
    cur.execute(groupSql)

    groupData = cur.fetchall()
    for d in groupData:
        cur.execute(detailSql % (d[0], d[1], d[2], d[3]))
        data = cur.fetchall()
        yield dataFormate("VHost=%s | QueueName=%s" % (d[2], d[3]), data)
    conn.commit()
    cur.close()
    conn.close()

# 获取到各队列组内查询生产者、消费者的数据后，将数据连接
def dataFormate(queueInfo: str, dicStr):
    publishDetails = []
    consumerDetails = []
    readyDetails = []
    for row in dicStr:
        if(row[0]!=None):
            publishDetail = json.loads(row[0])
            publishDetails.extend(publishDetail)
        if(row[1]!=None):
            consumerDetail = json.loads(row[1])
            consumerDetails.extend(consumerDetail)
        if(row[2]!=None):
            readyDetail=json.loads(row[2])
            readyDetails.extend(readyDetail)
    return queueInfo, publishDetails, consumerDetails,readyDetails

def writeToFile(listData):
    with open("./data.json", "w") as fp:
        fp.write(
            "-"*10+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+"-"*10+"\r\n")
        for data in listData:
            fp.write(str(data)+"\r\n")
        fp.write("\r\n")

def drawLineCharts(queueInfo: str, titlePosTop:str, xAxis: list, yAxis: list)->Line:
    line = Line(init_opts=opts.InitOpts(width="100%"))
    line.add_xaxis(xAxis)
    for item in yAxis:
        line.add_yaxis(item["title"], item["data"], is_smooth=False, label_opts=opts.LabelOpts(is_show=False))
    line.set_global_opts(title_opts=opts.TitleOpts(title=queueInfo,pos_top=titlePosTop), tooltip_opts=opts.TooltipOpts(axis_pointer_type='cross'), toolbox_opts=opts.ToolboxOpts(is_show=True), datazoom_opts=opts.DataZoomOpts,legend_opts=opts.LegendOpts(pos_top=titlePosTop))
    return line


def exportCharts():
    publishDic, consumerDic, readyDic = {}, {}, {}
    grid = Grid(init_opts=opts.InitOpts(width="100%",height="2500%"))
    i=0
    for queueInfo, publish, consumer,ready in connect_mssql():
        i+=1
        for pItem in publish:
            publishDic[datetime.datetime.strptime(pItem['time'], "%Y-%m-%dT%H:%M:%S%z").strftime("%Y-%m-%d %H:%M")] = round(pItem['rate'], 2)
        for cItem in consumer:
            consumerDic[datetime.datetime.strptime(cItem['time'], "%Y-%m-%dT%H:%M:%S%z").strftime("%Y-%m-%d %H:%M")] = round(cItem['rate'], 2)
        for rItem in ready:
            readyDic[datetime.datetime.strptime(rItem['time'], "%Y-%m-%dT%H:%M:%S%z").strftime("%Y-%m-%d %H:%M")] = round(rItem['count'], 2)            
        #字典内的键值对，按照key作为排序字段，重新生成新的列表
        publishResult = sorted(publishDic.items(), key=lambda x: x[0])
        # consumerResult= sorted(consumerDic.items(), key = lambda x:x[0])

        #将排序之后的publishDetail信息存入x轴和y轴数组
        x, publishY, consumerY, readyY = [], [], [], []
        for pItem in publishResult:
            x.append(pItem[0])
            publishY.append(pItem[1])
            #用publish的时间列表做x轴，防止consumber的时间列表数据与其不一致，这里直接从字典中取value
            consumerY.append(consumerDic.get(pItem[0]))
            readyY.append(readyDic.get(pItem[0]))

        y = [{"title": "消息生产速度[(count)/s]", "data": publishY},{"title": "消息消费速度[(count)/s]", "data": consumerY},{"title": "消息积压数量[(count)]", "data": readyY}]
        grid.add(drawLineCharts(queueInfo,"%dpx"%(100*i+500*(i-1)-50), x, y), grid_opts=opts.GridOpts(pos_top="%dpx"%(100*i+500*(i-1)),height="500px"))
        publishDic.clear()
        consumerDic.clear()
    grid.render("./wwwroot/pyecharts.html")


if __name__ == "__main__":
    exportCharts()