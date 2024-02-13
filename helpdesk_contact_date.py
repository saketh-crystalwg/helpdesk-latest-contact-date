import json
import pandas as pd
import datetime
import  requests
from requests.auth import HTTPBasicAuth
from sqlalchemy import create_engine
import smtplib,ssl
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders
import datetime as dt

date = dt.datetime.today()

date_1 = date.strftime("%m-%d-%Y")

def send_mail(send_from,send_to,subject,text,server,port,username='',password=''):
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = ', '.join(recipients)
    msg['Date'] = formatdate(localtime = True)
    msg['Subject'] = subject
    msg.attach(MIMEText(text))

    
    smtp = smtplib.SMTP_SSL(server, port)
    smtp.login(username,password)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.quit()

sender = "sakethg250@gmail.com"
recipients = ["sakethg250@gmail.com","marcos@crystalwg.com"]
password = "xjyb jsdl buri ylqr"

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
    df['agent_name'] = df['events'].apply(lambda events: [event.get('author','').get('name','') for event in events][0])
    df_collated = pd.concat([df_collated,df[['lastMessageAt','subject','requester_email','agent_name']]])
    
df_collated['lastMessageAt'] = pd.to_datetime(df_collated['lastMessageAt']).dt.tz_convert('UTC')

df_collated['Contact_Date'] = pd.to_datetime(df_collated['lastMessageAt']).dt.date

Last_Contact_date = df_collated.groupby('requester_email').agg({'Contact_Date':'max'}).reset_index().rename(columns = {'Contact_Date' : 'Last_Contact_Date'})

try:
    engine = create_engine('postgresql://orpctbsqvqtnrx:530428203217ce11da9eb9586a5513d0c7fe08555c116c103fd43fb78a81c944@ec2-34-202-53-101.compute-1.amazonaws.com:5432/d46bn1u52baq92',\
                           echo = False)

    Last_Contact_date.to_sql('last_contact_info', con = engine, if_exists='replace')
    
    subject = f'Helpdesk Data ingestion for {date_1} is Successful'
    body = f"Helpdesk data ingestion for {date_1} is Successful"
    send_mail(sender, recipients, subject,body, "smtp.gmail.com", 465,sender,password)
except Exception as ex:
    subject = f'Helpdesk Data ingestion for {date_1} is Failed'
    body = f"Helpdesk data ingestion for {date_1} is failed due to \n {str(ex)}"
    send_mail(sender, recipients, subject,body, "smtp.gmail.com", 465,sender,password)