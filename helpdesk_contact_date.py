import json
import pandas as pd
import datetime
import  requests
from requests.auth import HTTPBasicAuth
from sqlalchemy import create_engine

r = requests.get('https://api.helpdesk.com/v1/tickets/?silo=tickets&pageSize=100&page=1&status=solved',\
                auth = HTTPBasicAuth('bd0f01d1-471d-46bc-9c9f-16dab26ddb06', 'dal:j4wvStMiQ7letbTzNW-xKSh8tvs'))

tot_pages  = int(r.headers.get('x-total-pages'))

df_collated = pd.DataFrame(columns=['lastMessageAt', 'subject', 'requester_email','agent_name'])

for i in range(tot_pages):
    r = requests.get(f"https://api.helpdesk.com/v1/tickets/?silo=tickets&pageSize=100&page={i+1}&status=solved",\
                auth = HTTPBasicAuth('bd0f01d1-471d-46bc-9c9f-16dab26ddb06', 'dal:j4wvStMiQ7letbTzNW-xKSh8tvs'))
    df = pd.DataFrame(r.json())
    requester_email = pd.json_normalize(df.requester)['email']
    df['requester_email'] = requester_email
    Assignment = pd.json_normalize(df.assignment)
    df['agent_name']=Assignment['agent.name']
    df_collated = pd.concat([df_collated,df[['lastMessageAt','subject','requester_email','agent_name']]])
    
df_collated['lastMessageAt'] = pd.to_datetime(df_collated['lastMessageAt']).dt.tz_convert('UTC')

df_collated['Contact_Date'] = pd.to_datetime(df_collated['lastMessageAt']).dt.date

Last_Contact_date = df_collated.groupby('requester_email').agg({'Contact_Date':'max'}).reset_index().rename(columns = {'Contact_Date' : 'Last_Contact_Date'})

engine = create_engine('postgresql://orpctbsqvqtnrx:530428203217ce11da9eb9586a5513d0c7fe08555c116c103fd43fb78a81c944@ec2-34-202-53-101.compute-1.amazonaws.com:5432/d46bn1u52baq92',\
                       echo = False)

Last_Contact_date.to_sql('last_contact_info', con = engine, if_exists='replace')