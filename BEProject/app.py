import os

import json
from flask import Flask,render_template
from flask import request
from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
from IPython.display import display

from flask import Markup
app = Flask(__name__)

def mapcomp(count1):
    df=pd.read_csv("/home/project/Downloads/SupData/carriers.csv")
    a=df['Code'].values.tolist()
    car=df['Description'].values.tolist()
    out=[]
    for i in count1:
        for j in range(len(a)):
            if i == a[j]:
                out.append(car[j])
    return out

def produce_pi(scale):
    spark = SparkSession.builder.appName("AirlineProject").getOrCreate()
    n = 100000 * scale

    def f(_):
        from random import random
        x = random()
        y = random()
        return 1 if x ** 2 + y ** 2 <= 1 else 0

    count = spark.sparkContext.parallelize(
        range(1, n + 1), scale).map(f).reduce(lambda x, y: x + y)
    spark.stop()
    pi = 4.0 * count / n
    return pi
def sparkconf():
    spark=SparkSession.builder.master("spark://ubuntu:7077").appName('airline').getOrCreate()
    df=spark.read.csv('file:///home/master/data/1987.csv',inferSchema=True,header=True)
    df.createOrReplaceTempView('Airline')      
    res2=spark.sql("select count(*) cnt,UniqueCarrier,Year from Airline group by UniqueCarrier,Year order by cnt desc limit 7")
    pdf1=res2.toPandas()
    cntg=pdf1['Year']
    labels=pdf1['UniqueCarrier'].values.tolist()
    values=pdf1['cnt'].values.tolist()
    colors = [ "#F7464A", "#46BFBD", "#FDB45C", "#FEDCBA","#ABCDEF"  ]
    str1=" ".join(str(x) for x in cntg )
    response = "Pi is roughly {}".format(cntg)
    posts=json.loads(pdf1.to_json(orient='records'))

    return render_template('ddf.html', set=zip(values, labels, colors))

@app.route("/")
def index():
    return render_template('home.html')


@app.route("/sparkpi")
def sparkpi():
    posts=sparkconf()
    return posts



@app.route("/check")
def check():
    labels = ["January","February","March","April","May","June","July","August"]
    values = [10,9,8,7,6,4,7,8]
    colors = [ "#F7464A", "#46BFBD", "#FDB45C", "#FEDCBA","#ABCDEF", "#DDDDDD", "#ABCABC"  ]
    return render_template('Chart.html', set=zip(values, labels, colors))

@app.route("/highcharts")
def highcharts():
    spark=SparkSession.builder.master("spark://slave1:7077").appName('airline').getOrCreate()
    df=spark.read.format("csv").option("header", "true").load("file:///home/project/Downloads/Data/*.csv")
    df.createOrReplaceTempView('Airline')      
    res2=spark.sql("select count(*) cnt,UniqueCarrier from Airline group by UniqueCarrier order by cnt desc limit 5")
    pdf1=res2.toPandas()
    print(pdf1)
    count=pdf1['cnt']
    count1=pdf1['UniqueCarrier']
    name1=count1.values.tolist()
    data=count.values.tolist()
    name=mapcomp(name1)
    return render_template('highcharts.html',set=zip(name,data))
@app.route("/bardia")
def bardia():
    spark=SparkSession.builder.master("spark://slave1:7077").appName('airline').getOrCreate()
    df=spark.read.format("csv").option("header", "true").load("file:///home/project/Downloads/Data/*.csv")
    df.createOrReplaceTempView('Airline')   
    res2=spark.sql("select ROUND(AVG(ArrDelay)) arrivalDelay,UniqueCarrier carrier from Airline Group by UniqueCarrier")
    rdv=res2.toPandas()
    delay=rdv['arrivalDelay'].values.tolist()
    carrv=rdv['carrier'].values.tolist()
    carr=mapcomp(carrv)
    return render_template('bardiag.html',set=zip(carr,delay))
@app.route("/trend")
def trend():
    spark=SparkSession.builder.master("spark://slave1:7077").appName('airline').getOrCreate()
    df=spark.read.format("csv").option("header", "true").load("file:///home/project/Downloads/Data/*.csv")
    df.createOrReplaceTempView('Airline')   
    res2=spark.sql("select AVG(ArrDelay) arrivalDelay,UniqueCarrier carrier ,Year from Airline group by Year,UniqueCarrier ORDER BY Year,UniqueCarrier limit 200")
    rdv=res2.toPandas()
    print(rdv)
    delay=rdv['arrivalDelay'].values.tolist()
    carr=rdv['carrier'].values.tolist()
    year=rdv['Year'].values.tolist()
    return render_template('trendn.html',delay=delay)
@app.route("/trendnew")
def trendnew():
    spark=SparkSession.builder.master("spark://slave1:7077").appName('airline').getOrCreate()
    df=spark.read.format("csv").option("header", "true").load("file:///home/project/Downloads/Data/*.csv")
    df.createOrReplaceTempView('Airline')   
    res2=spark.sql("select ROUND(AVG(ArrDelay)) arrivalDelay,UniqueCarrier carrier , Year from Airline group by Year,UniqueCarrier ORDER BY Year,UniqueCarrier limit 200")
    rdv=res2.toPandas()
    rpd=rdv.groupby('Year')['arrivalDelay'].sum().reset_index().rename(columns={'sum':'valuesum','Year' : 'YearT'})

    delay=rpd['arrivalDelay'].values.tolist()
    year=rpd['YearT'].values.tolist()
    carrnew=mapcomp(rdv['carrier'])
    rdv= rdv.drop('carrier', 1)
    rdv['carrier']=carrnew
    print("New carrier"+rdv['carrier'])
    carr=rdv[['carrier','Year','arrivalDelay']].values.tolist()
    return render_template('trendnew.html',set=zip(year,delay),year=year,carr=carr,delay=delay)
@app.route("/timebest")
def timebest():
    spark=SparkSession.builder.master("spark://slave1:7077").appName('airline').getOrCreate()
    df=spark.read.format("csv").option("header", "true").load("file:///home/project/Downloads/Data/*.csv")
    df.createOrReplaceTempView('Airline')  
    res2=spark.sql("select ROUND(DepTime/100) hourOfDay,AVG(ArrDelay)  arrivalDelay from Airline Group by ROUND(DepTime/100) Order by hourOfDay ")
    rdv=res2.toPandas()
    hourd=rdv['hourOfDay'].values.tolist()
    delay=rdv['arrivalDelay'].values.tolist()
    negdelay = [x for x in delay if str(x) != 'nan']
    hour = [x for x in hourd if str(x) != 'nan']
    newdelay=[abs(number) for number in negdelay]
    hr=[]
    for x in hour:
        if x < 12.0 :
            x=str(x)+"AM"
            hr.append(x)
        else:
            x=str(x)+"PM"
            hr.append(x)

    return render_template('travelday.html',set=zip(hr,newdelay))
@app.route("/dayofweek")
def dayofweek():
    spark=SparkSession.builder.master("spark://slave1:7077").appName('airline').getOrCreate()
    df=spark.read.format("csv").option("header", "true").load("file:///home/project/Downloads/Data/*.csv")
    df.createOrReplaceTempView('Airline')  
    res2=spark.sql("select DayOfWeek,AVG(ArrDelay)  arrivalDelay from Airline Group by DayOfWeek Order by DayOfWeek")
    rdv=res2.toPandas()
    week=['Monday','TuesDay','WednesDay','ThursDay','Friday','Saturday','Sunday']
    delay=rdv['arrivalDelay'].values.tolist()

    return render_template('week.html',set=zip(week,delay))

@app.route("/cancelflight")
def cancelflight():
    spark=SparkSession.builder.master("spark://slave1:7077").appName('airline').getOrCreate()
    df=spark.read.format("csv").option("header", "true").load("file:///home/project/Downloads/Data/*.csv")
    df.createOrReplaceTempView('Airline')
    res2=spark.sql("select Year,Cancelled,UniqueCarrier from Airline WHERE Cancelled == 1 ")
    res3=spark.sql("select Year,Cancelled,UniqueCarrier,CarrierDelay  from Airline WHERE CarrierDelay == 1 ")
    res3.show()
    res2.createOrReplaceTempView('cancel')
    can=spark.sql("select count(Year) cnt ,Year,UniqueCarrier carrier from cancel group by Year,UniqueCarrier order by Year,UniqueCarrier ")


    rpv=can.toPandas()
    rdv=rpv.groupby('Year')['cnt'].sum().reset_index().rename(columns={'sum':'valuesum','Year' : 'YearT'})
    #print(rdv)
    cancelf=rdv['cnt'].values.tolist()
    yer=rdv['YearT'].values.tolist()
    carrnew=mapcomp(rpv['carrier'])
    rpv= rpv.drop('carrier', 1)
    rpv['carrier']=carrnew
    #print("New carrier"+rpv['carrier'])
    carr=rpv[['carrier','Year','cnt']].values.tolist()
    #print(carr)
    return render_template('cancel.html',yer=yer,cancelf=cancelf,carr=carr,set=zip(yer,cancelf))
@app.route("/diverted")
def diverted():
    spark=SparkSession.builder.master("spark://slave1:7077").appName('airline').getOrCreate()
    df=spark.read.format("csv").option("header", "true").load("file:///home/project/Downloads/Data/*.csv")
    df.createOrReplaceTempView('Airline')
    res2=spark.sql("select Year,Diverted,UniqueCarrier from Airline WHERE Diverted  == 1 ")
    
    res2.createOrReplaceTempView('diverted')
    can=spark.sql("select count(Year) cnt ,Year,UniqueCarrier carrier from diverted group by Year,UniqueCarrier order by Year,UniqueCarrier ")


    rpv=can.toPandas()
    rdv=rpv.groupby('Year')['cnt'].sum().reset_index().rename(columns={'sum':'valuesum','Year' : 'YearT'})
    #print(rdv)
    cancelf=rdv['cnt'].values.tolist()
    yer=rdv['YearT'].values.tolist()
    carrnew=mapcomp(rpv['carrier'])
    rpv= rpv.drop('carrier', 1)
    rpv['carrier']=carrnew
    #print("New carrier"+rpv['carrier'])
    carr=rpv[['carrier','Year','cnt']].values.tolist()
    #print(carr)
    return render_template('diverted.html',yer=yer,cancelf=cancelf,carr=carr,set=zip(yer,cancelf))
@app.route("/depdelay")
def depdelay():
    spark=SparkSession.builder.master("spark://slave1:7077").appName('airline').getOrCreate()
    df=spark.read.format("csv").option("header", "true").load("file:///home/project/Downloads/Data/*.csv")
    df.createOrReplaceTempView('Airline')   
    res2=spark.sql("select ROUND(AVG(DepDelay)) depDelay , Year from Airline group by Year ORDER BY Year limit 200")
    rdv=res2.toPandas()
    year=rdv['Year'].values.tolist()
    depdel=rdv['depDelay'].values.tolist()
    return render_template('depdelay.html',year=year,depdel=depdel)
@app.route("/depdelaycomp")
def depdelaycomp():
    spark=SparkSession.builder.master("spark://slave1:7077").appName('airline').getOrCreate()
    df=spark.read.format("csv").option("header", "true").load("file:///home/project/Downloads/Data/*.csv")
    df.createOrReplaceTempView('Airline')   
    res2=spark.sql("select UniqueCarrier carrier,ROUND(AVG(DepDelay)) depdelay from Airline GROUP BY UniqueCarrier ORDER BY depdelay desc LIMIT 5")
    rcv=res2.toPandas()
    delcomp=rcv['depdelay'].values.tolist()
    carrnew=mapcomp(rcv['carrier'])
    rcv= rcv.drop('carrier', 1)
    rcv['carrier']=carrnew
    #print("New carrier"+rpv['carrier'])
    carr=rcv[['carrier','depdelay']].values.tolist()
    

    return render_template('depcomp.html',carr=carr,delcomp=delcomp,set=zip(carr,delcomp))
@app.route("/performance")
def performance():
    spark=SparkSession.builder.master("spark://slave1:7077").appName('airline').getOrCreate()
    df=spark.read.format("csv").option("header", "true").load("file:///home/project/Downloads/Data/*.csv")
    df.createOrReplaceTempView('Airline')   
    res2=spark.sql("select ROUND(Avg(DepDelay)) depdelay , UniqueCarrier carrier ,Year from Airline Group by Year,UniqueCarrier  HAVING UniqueCarrier='AA' or UniqueCarrier='UA' or  UniqueCarrier='AS' or  UniqueCarrier='DL'  ORDER by Year")
    #res3=spark.sql("select ROUND(Avg(DepDelay)) depdelay , UniqueCarrier carrier ,Year from Airline Group by Year,UniqueCarrier  HAVING UniqueCarrier='UA'  ORDER by Year")
    #res4=spark.sql("select ROUND(Avg(DepDelay)) depdelay , UniqueCarrier carrier ,Year from Airline Group by Year,UniqueCarrier  HAVING UniqueCarrier='AS'  ORDER by Year")
    #res5=spark.sql("select ROUND(Avg(DepDelay)) depdelay , UniqueCarrier carrier ,Year from Airline Group by Year,UniqueCarrier  HAVING UniqueCarrier='DL'  ORDER by Year")
    rpd=res2.toPandas()
    rpd2=rpd[rpd.carrier == 'AA']
    rpd3=rpd[rpd.carrier == 'UA']
    rpd4=rpd[rpd.carrier == 'AS']
    rpd5=rpd[rpd.carrier == 'DL']

    #rpd3=res3.toPandas()
    #rpd4=res4.toPandas()
    #rpd5=res5.toPandas()
    lis2=rpd2['depdelay'].values.tolist()
    lis3=rpd3['depdelay'].values.tolist()
    lis4=rpd4['depdelay'].values.tolist()
    lis5=rpd5['depdelay'].values.tolist()
    year=rpd5['Year'].values.tolist() 
    return render_template('performance.html',lis2=lis2,lis3=lis3,lis4=lis4,lis5=lis5,year=year)
@app.route("/monthwise")
def monthwise():
    spark=SparkSession.builder.master("spark://slave1:7077").appName('airline').getOrCreate()
    df=spark.read.format("csv").option("header", "true").load("file:///home/project/Downloads/Data/*.csv")
    df.createOrReplaceTempView('Airline')   
    res2=spark.sql("select ROUND(Avg(ArrDelay)) depdelay , UniqueCarrier carrier ,Month from Airline Group by Month,UniqueCarrier  HAVING UniqueCarrier='AA' or UniqueCarrier='UA' or  UniqueCarrier='AS' or  UniqueCarrier='DL'  ORDER by Month")
    rpd=res2.toPandas()
    print(rpd)
    rpd2=rpd[rpd.carrier == 'AA']
    rpd3=rpd[rpd.carrier == 'UA']
    rpd4=rpd[rpd.carrier == 'AS']
    rpd5=rpd[rpd.carrier == 'DL']
    lis2=rpd2['depdelay'].values.tolist()
    lis3=rpd3['depdelay'].values.tolist()
    lis4=rpd4['depdelay'].values.tolist()
    lis5=rpd5['depdelay'].values.tolist()
    year=rpd5['Month'].values.tolist() 
    return render_template('performonth.html',lis2=lis2,lis3=lis3,lis4=lis4,lis5=lis5,year=year)
if __name__ == "__main__":
    app.run(debug=True)


 

