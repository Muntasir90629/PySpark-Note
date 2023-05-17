#Importing Library

import pandas as pd
import numpy as np
import mysql.connector
import csv
from datetime import datetime
from datetime import date
from datetime import timedelta



#reading data 
yesterday = str(date.today()- timedelta(days = 1))
path="C:/Users/MuntasirulHoque/OneDrive - ADA Global/Desktop/Final Code/Google/robi-google-dashboard-robi-"
df=pd.read_csv(path + yesterday + ".csv", encoding="utf16",sep='\t',thousands=',',parse_dates=['Day'], dayfirst=True)
# df=pd.read_csv('C:/Users/FuadTanvir/Downloads/robi-google-090523.csv',encoding="utf16",sep='\t',thousands=',',parse_dates=['Day'], dayfirst=True)

#date format

df['Day'] = pd.to_datetime(df['Day']).dt.date

#Extracting PO

df['PO'] = df['Campaign'].str.split('|').str[1]
df['PO'] = df['PO'].str.strip()

#Extracting Planned Cost

df['Planned Cost'] = df['Campaign'].str.split('$').str[1]
df['Planned Cost'] = df['Planned Cost'].str.split('_').str[0]
df['Planned Cost'] = df['Planned Cost'].apply(lambda x: float(x.split()[0].replace(',', '')))

#Extracting Planned Result

df['Planned Result'] = df['Campaign'].str.split('$').str[2]
df['Planned Result'] = df['Planned Result'].str.split('_').str[1]
df['Planned Result'] = df['Planned Result'].str.replace(',','')

#DROPNA USING FOR NAMING ERROR WE HAVE TO REMOVE IT WHEN NAME IS PERFECT 

df=df.dropna()
df['Planned Result'] = df['Planned Result'].apply(lambda x: int(x.split()[0].replace(',', '')))

#Extracting Planned CPR

df['Planned CPR'] = df['Campaign'].str.split('$').str[2]
df['Planned CPR'] = df['Planned CPR'].str.split('_').str[0]
df['Planned CPR'] = df['Planned CPR'].astype('float')

#Extracting OBJECTIVE

conditions = [
    (df['Campaign'].str.contains('In app Action') == 1),
    (df['Campaign'].str.contains('UAC') == 1) ,
    (df['Campaign'].str.contains('Search') == 1),
    (df['Campaign'].str.contains('Trueview_In-Stream') == 1),
    (df['Campaign'].str.contains('GDN') == 1),
    (df['Campaign'].str.contains('Bumper') == 1),
    (df['Campaign'].str.contains('P.Max') == 1),
    (df['Campaign'].str.contains('SEM') == 1),
    (df['Campaign'].str.contains('Non-skippable In-Stream') == 1)

    ]
values = ['In App Action','UAC','Search','Trueview_In-stream','GDN','Bumper','Performance_Max','SEM','Non-skippable In-Stream']
df['Objective'] = np.select(conditions, values)

#Creating Demo Columns

df["Platform"]="Google"
df["Account"]="Robi - BD (USD)"
df["Brand"]="Robi"
df['Client'] = "Robi"

# Replaceing  -- value with zero 

df=df.replace(" --", 0)

# Creating Calculated Result column for extracting  Result accorting to objective

df["Calculated Results"] = np.nan
df["CPR_NEW"] = np.nan

# formating data columns according to order

df['Interaction rate'] = df['Interaction rate'].str.replace(r'%', '')
df['Interaction rate'] = df['Interaction rate'].astype(float)
df['Video played to 25%'] = df['Video played to 25%'].str.replace(r'%', '')
df['Video played to 25%'] = df['Video played to 25%'].astype(float)
df['Video played to 75%'] = df['Video played to 75%'].str.replace(r'%', '')
df['Video played to 75%'] = df['Video played to 75%'].astype(float)
df['Video played to 50%'] = df['Video played to 50%'].str.replace(r'%', '')
df['Video played to 50%'] = df['Video played to 50%'].astype(float)
df['Video played to 100%'] = df['Video played to 100%'].str.replace(r'%', '')
df['Video played to 100%'] = df['Video played to 100%'].astype(float)
df['CTR'] = df['CTR'].str.replace(r'%', '')
df['CTR'] = df['CTR'].astype(float)
df['Conv. rate'] = df['Conv. rate'].str.replace(r'%', '')
df['Conv. rate'] = df['Conv. rate'].astype(float)
df['Viewable CTR'] = df['Viewable CTR'].str.replace(r'%', '')
df['Viewable CTR'] = df['Viewable CTR'].astype(float)
df['Engagement rate'] = df['Engagement rate'].str.replace(r'%', '')
df['Engagement rate'] = df['Engagement rate'].astype(float)
df['Watch time'] = df['Watch time'].str.replace(r',', '')
df['Watch time'] = df['Watch time'].astype(float)
df['Watch time'] = df['Watch time'].replace(np.nan, 0)
df['Watch time'] = df['Watch time'].astype(int)
df['Max. CPV'] = df['Max. CPV'].astype(float)
df['Max. CPM'] = df['Max. CPV'].astype(float)
df['Avg. CPC'] = df['Avg. CPC'].astype(float)
df['Target CPA'] = df['Target CPA'].astype(float)
df['Target CPM'] = df['Target CPM'].astype(float)
df['Cost / Participated in-app action'] = df['Cost / Participated in-app action'].astype(float)
df['Avg. CPV'] = df['Avg. CPV'].astype(float)
df['Cost / In-app action'] = df['Cost / In-app action'].astype(float)
df['Avg. CPV'] = df['Avg. CPV'].astype(float)
df['Avg. watch time / impr.'] = df['Avg. watch time / impr.'].astype(float)
df['Cost / In-app action'] = df['Cost / In-app action'].astype(float)
df['CTR'] = (df['CTR'] /100).round(4)

#logic behind creating result 

for index in df.index:
  if df["Objective"][index]=="GDN":
    if df["Bid strategy type"][index]=="Manual CPM" :
      df["Calculated Results"][index]=df["Impr."][index]
    elif df["Bid strategy type"][index]=="Manual CPC" :
      df["Calculated Results"][index]=df["Clicks"][index]
    elif df["Bid strategy type"][index]=="Maximize clicks" :
      df["Calculated Results"][index]=df["Clicks"][index]
    elif df["Bid strategy type"][index]=="Target CPA" :
      df["Calculated Results"][index]=df["Conversions"][index]
    elif df["Bid strategy type"][index]=="Maximize Conversions" :
      df["Calculated Results"][index]=df["Conversions"][index]
    elif df["Bid strategy type"][index]=="Viewable CPM" :
      df["Calculated Results"][index]=df["Viewable impr."][index]
  elif df["Objective"][index]=="SEM":
    if df["Bid strategy type"][index]=="Manual CPM" :
      df["Calculated Results"][index]=df["Impr."][index]
    elif df["Bid strategy type"][index]=="Manual CPC" :
      df["Calculated Results"][index]=df["Clicks"][index]
    elif df["Bid strategy type"][index]=="Maximize Clicks" :
      df["Calculated Results"][index]=df["Clicks"][index]
    elif df["Bid strategy type"][index]=="Target CPA" :
      df["Calculated Results"][index]=df["Conversions"][index]
    elif df["Bid strategy type"][index]=="Maximize Conversions" :
      df["Calculated Results"][index]=df["Conversions"][index]
    elif df["Bid strategy type"][index]=="Viewable CPM" :
      df["Calculated Results"][index]=df["Viewable impr."][index]
  elif df["Objective"][index]=="Trueview_In-stream":
    if df["Bid strategy type"][index]=="Manual CPV" :
      df["Calculated Results"][index]=df["Views"][index]
    elif df["Bid strategy type"][index]=="Maximize CPM" :
      df["Calculated Results"][index]=df["Impr."][index]
    elif df["Bid strategy type"][index]=="Target CPM" :
      df["Calculated Results"][index]=df["Impr."][index]
  elif df["Objective"][index]=="R.Mix":
    if df["Bid strategy type"][index]=="Manual CPV" :
      df["Calculated Results"][index]=df["Views"][index]
    elif df["Bid strategy type"][index]=="Maximize CPM" :
      df["Calculated Results"][index]=df["Impr."][index]
    elif df["Bid strategy type"][index]=="Target CPM" :
      df["Calculated Results"][index]=df["Impr."][index]
  elif df["Objective"][index]=="Non-skippable In-Stream":
    if df["Bid strategy type"][index]=="Manual CPV" :
      df["Calculated Results"][index]=df["Views"][index]
    elif df["Bid strategy type"][index]=="Maximize CPM" :
      df["Calculated Results"][index]=df["Impr."][index]
    elif df["Bid strategy type"][index]=="Target CPM" :
      df["Calculated Results"][index]=df["Impr."][index]
  elif df["Objective"][index]=="UAC":
    df["Calculated Results"][index]=df["Installs"][index] 
  elif df["Objective"][index]=="In App Action":
    df["Calculated Results"][index]=df["In-app actions"][index]
  elif df["Objective"][index]=="Bumper":
      df["Calculated Results"][index]=df["Impr."][index]
  elif df["Objective"][index]=="Performance_Max":
    df["Calculated Results"][index]=df["Conversions"][index]
  elif df["Objective"][index]=="Search":
    df["Calculated Results"][index]=df["Clicks"][index]
  elif df["Objective"][index]=="P.Max":
    df["Calculated Results"][index]=df["Conversions"][index]
  else :
    df["Calculated Results"][index]=0

#creating CPR columns with logic

df["CPR"] = df["Cost"]/df["Calculated Results"]
df.replace([np.inf, -np.inf], 0, inplace=True)
for index in df.index:
  if df["Objective"][index]=="GDN":
    if df["Bid strategy type"][index]=="Manual CPM" :
      value=df["CPR"][index]
      value=value*1000
      df["CPR_NEW"][index]=value
    elif df["Bid strategy type"][index]=="Viewable CPM" :
      value=df["CPR"][index]
      value=value*1000
      df["CPR_NEW"][index]=value
    else:
      df["CPR_NEW"][index]=df["CPR"][index]
  elif df["Objective"][index]=="SEM":
    if df["Bid strategy type"][index]=="Manual CPM" :
      value=df["CPR"][index]
      value=value*1000
      df["CPR_NEW"][index]=value
    elif df["Bid strategy type"][index]=="Viewable CPM" :
      value=df["CPR"][index]
      value=value*1000
      df["CPR_NEW"][index]=value
    else:
      df["CPR_NEW"][index]=df["CPR"][index]
  elif df["Objective"][index]=="Trueview_In-stream":
    if df["Bid strategy type"][index]=="Maximize CPM" :
      value=df["CPR"][index]
      value=value*1000
      df["CPR_NEW"][index]=value
    elif df["Bid strategy type"][index]=="Target CPM" :
      value=df["CPR"][index]
      value=value*1000
      df["CPR_NEW"][index]=value
    else:
      df["CPR_NEW"][index]=df["CPR"][index]
  elif df["Objective"][index]=="R.Mix":
    if df["Bid strategy type"][index]=="Maximize CPM" :
      value=df["CPR"][index]
      value=value*1000
      df["CPR_NEW"][index]=value
    elif df["Bid strategy type"][index]=="Target CPM" :
      value=df["CPR"][index]
      value=value*1000
      df["CPR_NEW"][index]=value
    else:
      df["CPR_NEW"][index]=df["CPR"][index]
  elif df["Objective"][index]=="Non-skippable In-Stream":
    if df["Bid strategy type"][index]=="Maximize CPM" :    
      value=df["CPR"][index]
      value=value*1000
      df["CPR_NEW"][index]=value
    elif df["Bid strategy type"][index]=="Target CPM" :
      value=df["CPR"][index]
      value=value*1000
      df["CPR_NEW"][index]=value
    else:
      df["CPR_NEW"][index]=df["CPR"][index]
  elif df["Objective"][index]=="Bumper":
    value=df["CPR"][index]
    value=value*1000
    df["CPR_NEW"][index]=value
  else :
    df["CPR_NEW"][index]=df["CPR"][index]
df=df.drop(['CPR'], axis=1)
df.rename(columns = {'CPR_NEW':'CPR'}, inplace = True)
df= df[["Day",	"Ad group status",	"Ad group",	"Campaign",	"Status",	"Status reasons",	"Currency code",	"Default max. CPC",	"Max. CPV",	"Max. CPM",	"Target CPA",	"Target ROAS",	"Target CPM",	"Campaign type",	"Impr.",	"Currency code.1",	"Cost",	"Clicks",	"CTR",	"Conversions",	"Bid strategy type",	"Interactions",	"Interaction rate",	"Avg. cost",	"Cost / conv.",	"Conv. rate",	"Avg. CPV",	"Video played to 25%",	"Video played to 50%",	"Video played to 75%",	"Video played to 100%",	"Views",	"Viewable impr.",	"Engagements",	"Avg. watch time / impr.",	"Avg. CPM",	"Avg. CPC",	"Bid strategy",	"Engagement rate",	"Watch time",	"Installs",	"Cost / Install",	"In-app actions",	"Cost / In-app action",	"Viewable CTR",	"Avg. viewable CPM",	"Participated in-app actions",	"Cost / Participated in-app action",	"PO",	"Objective",	"Platform",	"Account",	"Brand","Client","Planned Cost","Planned Result","Planned CPR","Calculated Results","CPR"]]
df=df.fillna(0)
data_count=df.shape[0]

# Getting the date range 

latest_date=df["Day"].max()
earliest_date=df["Day"].min()

# Database Connection

db_host='powerbi.cerm6wsx61pw.us-east-1.rds.amazonaws.com'
db_user='munta_editor'
db_password='munta@&0908^7'
db_database='test_dashboard'

mydb=mysql.connector.connect(host=db_host, user=db_user, password=db_password, database=db_database)

if(mydb):
    print("connection successful")
else:
    print("connection unsuccessful")

mycursor=mydb.cursor()

# Data Delete

sql="Delete from google where DATE(Day) BETWEEN DATE(%s) AND DATE(%s)"
mycursor.execute(sql, (earliest_date, latest_date))
mydb.commit()
print("PREVIOUS DATA DELETED")

# Data insertion

sql= "INSERT INTO google(Day, Ad_group_status, Ad_group, Campaign, Status, Status_reasons, Currency_code, Default_max_CPC, Max_CPV, Max_CPM, Target_CPA, Target_ROAS, Target_CPM, Campaign_type, Impr, Currency_code1, Cost, Clicks, CTR, Conversions, Bid_strategy_type, Interactions, Interaction_rate, Avg_cost, Cost_conv, Conv_rate, Avg_CPV, Video_played_to_25, Video_played_to_50, Video_played_to_75, Video_played_to_100, Views, Viewable_impr, Engagements, Avg_watch_time_by_impr, Avg_CPM, Avg_CPC, Bid_strategy, Engagement_rate, Watch_time, Installs, Cost_by_Install, In_app_actions, Cost_by_In_app_action, Viewable_CTR, Avg_viewable_CPM, Participated_in_app_actions, Cost_by_Participated_in_app_action, PO, Objective, Platform, Account, Brand, Client, Planned_Cost, Planned_Result, Planned_CPR, Calculated_Results, CPR)\
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
count=1
for i,row in df.iterrows():
    mycursor.execute(sql, tuple(row))
    print(tuple(row))
    print("")
    print("Total DATA= " + str(data_count))
    print("DATA INSERTED = " + str(count))
    print("")
    count+=1
mydb.commit()

# Closing Database Connection

mycursor.close()
print("OPERATION SUCCESSFUL")
