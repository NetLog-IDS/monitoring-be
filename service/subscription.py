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

async def send_email(topic, values) -> None:

    emails_dict = await get_email_subscriptions()
    curr_time = int(datetime.now(timezone.utc).timestamp()) 
    email = []
    email_id=[]
    for emails in emails_dict:
        if curr_time - emails['last_sent'] > 60:
            email.append(emails['email'])
            email_id.append(emails['_id'])

    if len(email) <= 0:
        return

    if len(email) > 0:
        if topic == "DOS":
            body = {
                "prediction": topic,
                "timestamp": datetime.fromtimestamp(values['TIMESTAMP_START']).strftime('%Y-%m-%d %H:%M:%S'),
                "srcIp": values['IP_DST'],
                "timestamp_end": datetime.fromtimestamp(values['TIMESTAMP_END']).strftime('%Y-%m-%d %H:%M:%S')
            }  
        elif topic == "PORT_SCAN":
            body = {
                "prediction": topic,
                "timestamp": datetime.fromtimestamp(values['TIMESTAMP_START']).strftime('%Y-%m-%d %H:%M:%S'),
                "srcIp": values['IP_SRC'],
                "timestamp_end": datetime.fromtimestamp(values['TIMESTAMP_END']).strftime('%Y-%m-%d %H:%M:%S')
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