import smtplib
from pathlib import Path
from email.message import EmailMessage
from email.utils import formataddr


SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

GMAIL_USER = "stoverepmaria@gmail.com"
GMAIL_APP_PASSWORD = "zfdctsvsydtrddmv"  # Google 应用专用密码，不是登录密码



def send_gmail(
    to_emails,
    subject,
    text_body,
    html_body=None,
    attachments=None,
):
    if isinstance(to_emails, str):
        to_emails = [to_emails]

    attachments = attachments or []

    msg = EmailMessage()
    msg["From"] = formataddr(("Stoverparia", GMAIL_USER))
    msg["To"] = ", ".join(to_emails)
    msg["Subject"] = subject

    msg.set_content(text_body)

    # if html_body:
    #     msg.add_alternative(html_body, subtype="html")

    for file_path in attachments:
        path = Path(file_path)

        if not path.exists():
            raise FileNotFoundError(f"附件不存在：{file_path}")

        with open(path, "rb") as f:
            file_data = f.read()

        msg.add_attachment(
            file_data,
            maintype="application",
            subtype="octet-stream",
            filename=path.name,
        )

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            smtp.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            smtp.send_message(msg)

        print("邮件发送成功")

    except smtplib.SMTPAuthenticationError:
        print("认证失败：请确认 Gmail 已开启两步验证，并使用应用专用密码。")

    except smtplib.SMTPRecipientsRefused:
        print("收件人被拒绝，请检查邮箱地址。")

    except smtplib.SMTPException as e:
        print(f"SMTP 错误：{e}")

    except Exception as e:
        print(f"其他错误：{e}")


if __name__ == "__main__":
   
    send_gmail(
        to_emails=["ryan.chiu@tpv-tech.com"],
        subject="想要回报一件事",
        text_body="这是一封 Python 发送的邮件。\r\n\r\nBest regards,\r\nStoverparia",
        html_body="""
        <h2>测试邮件</h2>
        <p>这是一封 <b>Python</b> 发送的 Gmail 邮件。</p>
        <p>Best regards,<br>
        Stoverparia</p>
        """,
        attachments=[
            # "test.pdf",
            # "image.jpg",
        ],
    )