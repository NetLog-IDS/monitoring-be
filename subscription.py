from fastapi import Path
from fastapi_mail import FastMail, MessageSchema, ConnectionConfig, MessageType
from dotenv import load_dotenv
import os

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

async def send_email(details) -> None:    
    message = MessageSchema(
        subject="Intrusion Detected!",
        recipients=details.get("email"),
        template_body=details.get("body"),
        subtype=MessageType.html,
        )

    fm = FastMail(conf)
    print("sending email ...")
    await fm.send_message(message, template_name="email.html")
    print("finish sending email!")
    return