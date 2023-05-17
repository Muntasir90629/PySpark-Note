# The obejct is to clean Facebook ads data and push it to dashboard database

import pandas as pd
import numpy as np
import mysql.connector 
import csv
from datetime import datetime
from datetime import date
from datetime import timedelta

#reading data 
yesterday = str(date.today()- timedelta(days = 1))
path="C:/Users/MuntasirulHoque/OneDrive - ADA Global/Desktop/Final Code/FB/robi-fb-dashboard-robi-"
df=pd.read_csv(path + yesterday+".csv", parse_dates=['Day', 'Reporting starts', 'Reporting ends'], dayfirst=True)

# Fixing date format

df['Day'] = pd.to_datetime(df['Day']).dt.date
df['Reporting starts'] = pd.to_datetime(df['Reporting starts']).dt.date
df['Reporting ends'] = pd.to_datetime(df['Reporting ends']).dt.date


#extracting PO

df['PO'] = df['Campaign name'].str.split('|').str[1]
df['PO'] = df['PO'].str.replace(" ", "")

#extracting Planned Cost

df['Planned Cost'] = df['Campaign name'].str.split('$').str[1]
df['Planned Cost'] = df['Planned Cost'].str.split('_').str[0]
df['Planned Cost'] = df['Planned Cost'].apply(lambda x: float(x.split()[0].replace(',', '')))
df['Planned Cost'] = df['Planned Cost'].astype('int')

#extracting Planned Result

df['Planned Result'] = df['Campaign name'].str.split('$').str[2]
df['Planned Result'] = df['Planned Result'].str.split('_').str[1]
df['Planned Result'] = df['Planned Result'].apply(lambda x: int(x.split()[0].replace(',', '')))

#extracting CPR

df['Planned CPR'] = df['Campaign name'].str.split('$').str[2]
df['Planned CPR'] = df['Planned CPR'].str.split('_').str[0]
df['Planned CPR'] = df['Planned CPR'].astype('float')

#Creating Objective Columns

conditions = [
    (df['Campaign name'].str.contains('_App Installs_') == 1),
    (df['Campaign name'].str.contains('_Conversions') == 1) ,
    (df['Campaign name'].str.contains('_Engagement_') == 1),
    (df['Campaign name'].str.contains('_Video Views_') == 1),
    (df['Campaign name'].str.contains('_Traffic') == 1),
    (df['Campaign name'].str.contains('Reach') == 1)
    ]

values = ['App Installs','Conversion','Engagement','Video Views','Traffic','Reach']
df['Objective'] = np.select(conditions, values)

# Calculating CPR

df["CPR"] = df["Amount spent (USD)"]/df["Results"]
df["CPR_NEW"]=np.nan

# Calculating Reach

df["Average Reach"] = df["Impressions"]/df["Frequency"]

# Making Columns according to need 
brand="Robi"

df['Brand'] = brand
df["Calculated Results"] = np.nan
df["Platform"]="Facebook"
df['Client'] = "Robi"

#Calculated Results Calculation

for index in df.index:
  if df["Objective"][index]=="Reach":
    df["Calculated Results"][index]=df["Average Reach"][index]
  elif df["Objective"][index]=="App Installs":
    df["Calculated Results"][index]=df["Mobile app installs"][index]
  elif df["Objective"][index]=="Conversion":
    df["Calculated Results"][index]=df["Website purchases"][index]
  elif df["Objective"][index]=="Engagement":
    df["Calculated Results"][index]=df["Post engagement"][index]
  elif df["Objective"][index]=="Video Views":
    df["Calculated Results"][index]=df["ThruPlays"][index]
  elif df["Objective"][index]=="Traffic":
    df["Calculated Results"][index]=df["Clicks (all)"][index]
  else :
    df["Calculated Results"][index]=0

#Calculated CPR for REACH objective

for index in df.index:
  if df["Objective"][index]=="Reach":
    value=df["CPR"][index]
    value=value*1000
    df["CPR_NEW"][index]=value
  else:
    df["CPR_NEW"][index]=df["CPR"][index]

df=df.drop(['CPR'], axis=1)
df.rename(columns = {'CPR_NEW':'CPR'}, inplace = True)
df.replace([np.inf, -np.inf], 0, inplace=True)

#VTR CALCULATION

df['100% VTR'] = (df['100% VTR'] * 100).round(2)
df['50% VTR'] = (df['50% VTR'] * 100).round(2)
df['75% VTR'] = (df['75% VTR'] * 100).round(2)
df['25% VTR'] = (df['25% VTR'] * 100).round(2)

# Final dataframe
df = df[["Day","Age","Ad set name","Ad name","Delivery status","Account name","Account ID","Reach","Impressions","Frequency","Result Type","Results","Amount spent (USD)","Cost per result","Link clicks","Website purchases","App Installs","Mobile app installs","ThruPlays","Landing page views", "Result rate","25% VTR","50% VTR","75% VTR","100% VTR","CTR (link click-through rate)","CPM (cost per 1,000 impressions)","Campaign delivery","Post engagement","Reporting starts","Reporting ends",	"Campaign name","Clicks (all)",	"CPC (cost per link click)","CPR","Average Reach","PO","Planned Cost","Planned CPR","Planned Result","Brand","Platform","Objective","Calculated Results","Client"]]
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

sql="Delete from fb where DATE(Day) BETWEEN DATE(%s) AND DATE(%s) AND Brand=%s"
mycursor.execute(sql, (earliest_date, latest_date, brand))
mydb.commit()
print("PREVIOUS DATA DELETED")

# Data insertion

sql= "INSERT INTO fb(Day, Age, Ad_set_name, Ad_name, Delivery_status, Account_name, Account_ID, Reach, Impressions, Frequency, Result_Type, Results, Amount_spent_USD, Cost_per_result, Link_clicks, Website_purchases, App_Installs, Mobile_app_installs, ThruPlays, Landing_page_views, Result_rate, 25_VTR, 50_VTR, 75_VTR, 100_VTR, CTR_link_click_through_rate, CPM_cost_per_1000_impressions, Campaign_delivery, Post_engagement, Reporting_starts, Reporting_ends, Campaign_name, Clicks_all, CPC_cost_per_link_click,CPR, Average_Reach, PO, Planned_Cost, Planned_CPR, Planned_Result, Brand, Platform, Objective, Calculated_Results, Client)\
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

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
