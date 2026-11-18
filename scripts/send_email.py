#!/usr/bin/env python3
"""Send an .eml written by the check job through SMTP (STARTTLS).

Dry run by default: prints what would be sent. Add --send to really send.
Settings come from environment variables: SMTP_HOST, SMTP_PORT (587), SMTP_USER, SMTP_PASSWORD.
"""
import argparse
import email
import os
import smtplib
import sys


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("eml")
    ap.add_argument("--send", action="store_true")
    a = ap.parse_args()
    with open(a.eml, "rb") as f:
        msg = email.message_from_binary_file(f)
    print(f"To: {msg['To']}\nSubject: {msg['Subject']}")
    if not a.send:
        print("dry run: add --send to deliver")
        return 0
    host = os.environ.get("SMTP_HOST")
    if not host:
        print("SMTP_HOST is not set", file=sys.stderr)
        return 1
    with smtplib.SMTP(host, int(os.environ.get("SMTP_PORT", "587")), timeout=20) as s:
        s.starttls()
        if os.environ.get("SMTP_USER"):
            s.login(os.environ["SMTP_USER"], os.environ.get("SMTP_PASSWORD", ""))
        s.send_message(msg)
    print("sent")
    return 0


if __name__ == "__main__":
    sys.exit(main())
