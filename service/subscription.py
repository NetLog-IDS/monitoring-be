from fastapi_mail import FastMail, MessageSchema, ConnectionConfig, MessageType
from dotenv import load_dotenv
import os
from service.email import get_email_subscriptions

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
    emails_tuple = get_email_subscriptions()
    email = [e[0] for e in emails_tuple]
    body = {
        "prediction": topic,
        "timestamp": values['TIMESTAMP_START'],
        "srcIp":values['IP_SRC'],
        "timestamp_end":values['TIMESTAMP_END']
    }  
    message = MessageSchema(
        subject="Intrusion Detected!",
        recipients=email,
        template_body=body,
        subtype=MessageType.html,
        )

    fm = FastMail(conf)
    await fm.send_message(message, template_name="email.html")
    return