"""
About: helper function to send basic email incl. optional image attachment (modify with your own SMTP server & details)
Test: Create a new gmail and enable less secure apps: https://myaccount.google.com/lesssecureapps
"""
import os
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart


def send_mail(sender, receiver, subject, content, password, smtp_server, image_path=""):
    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = receiver

    text = MIMEText(content)
    msg.attach(text)

    if image_path != "":
        img_data = open(image_path, "rb").read()
        image = MIMEImage(img_data, name=os.path.basename(image_path))
        msg.attach(image)

    context = ssl.create_default_context()
    s = smtplib.SMTP_SSL(smtp_server, port, context=context)
    s.login(sender, password)
    s.sendmail(sender, receiver, msg.as_string())
    s.quit()


# test usage
sender = "xyz@gmail.com"
receiver = "xyz@hotmail.com"
password = "xyz"
smtp_server = "smtp.gmail.com"
port = 465
image_path = "signal_EngineSpeed.png"
subject = "[Subject line]"
content = "[Mail content text]"

send_mail(sender, receiver, subject, content, password, smtp_server, image_path)
