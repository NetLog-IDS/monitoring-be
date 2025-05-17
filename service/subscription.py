from fastapi_mail import FastMail, MessageSchema, ConnectionConfig, MessageType
from dotenv import load_dotenv
import os
from service.email import *
from datetime import datetime, timezone

load_dotenv()

conf = ConnectionConfig(
    MAIL_USERNAME = os.environ.get("MAIL_USERNAME"),
    MAIL_PASSWORD = os.environ.get("MAIL_PASSWORD"),
    MAIL_FROM = os.environ.get("MAIL_FROM"),
    MAIL_PORT = int(os.environ.get("MAIL_PORT")),
    MAIL_SERVER = os.environ.get("MAIL_SERVER"),
    MAIL_STARTTLS = True,
    MAIL_SSL_TLS = False,
    TEMPLATE_FOLDER = './templates',
)

async def send_email(topic, dos_timestamp_start, dos_timestamp_end, ip_lst) -> None:
    """
    This function sends an email to all subscribers with a time difference of more than 1 minute from the last email sent. The 1 minute time difference is to ensure that the email is not sent too frequently and the email service think it is spam.

    Parameters:
    topic (str): The type of intrusion detected.
    dos_timestamp_start (int): The start time of the intrusion.
    dos_timestamp_end (int): The end time of the intrusion.
    ip_lst (list): A list of IP addresses involved in the intrusion.
    """
    
    emails_dict = await get_email_subscriptions()
    curr_time = int(datetime.now(timezone.utc).timestamp()) 
    email = []
    email_id=[]
    for emails in emails_dict:
        if curr_time - emails['last_sent'] > 60:
            email.append(emails['email'])
            email_id.append(emails['_id'])

    if len(email) > 0:
        body = {
                "prediction": topic,
                "timestamp": datetime.fromtimestamp(dos_timestamp_start).strftime('%Y-%m-%d %H:%M:%S'),
                "srcIp": " ,".join(ip_lst),
                "timestamp_end": datetime.fromtimestamp(dos_timestamp_end).strftime('%Y-%m-%d %H:%M:%S')
        }

        message = MessageSchema(
            subject="Intrusion Detected!",
            recipients=email,
            template_body=body,
            subtype=MessageType.html,
        )

        fm = FastMail(conf)
        await fm.send_message(message, template_name="email.html")
        await update_email_subscription(email_id)
    return