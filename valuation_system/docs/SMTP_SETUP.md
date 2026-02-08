# SMTP Email Setup Guide

## Gmail App Password Setup

Since the `../learn` directory doesn't have SMTP credentials, follow these steps to generate a Gmail App Password:

### Prerequisites
- Gmail account: medury@gmail.com
- 2-Factor Authentication (2FA) must be enabled on your Google account

### Steps

1. **Enable 2FA (if not already enabled)**
   - Go to: https://myaccount.google.com/security
   - Under "Signing in to Google", click "2-Step Verification"
   - Follow prompts to enable

2. **Generate App Password**
   - Go to: https://myaccount.google.com/apppasswords
   - Sign in if prompted
   - App name: "Valuation System"
   - Click "Create"
   - Copy the 16-character password (format: xxxx xxxx xxxx xxxx)

3. **Update .env file**
   ```bash
   # Edit the file
   nano /Users/ram/code/research/valuation_system/config/.env

   # Update these lines (around line 63-64):
   SMTP_USER=medury@gmail.com
   SMTP_PASSWORD=your_16_character_app_password_here
   ```

4. **Test email sending**
   ```bash
   cd /Users/ram/code/research/valuation_system
   python3 -c "
from notifications.email_sender import EmailSender
sender = EmailSender()
sender.send_test_email('medury@gmail.com')
   "
   ```

   You should receive a test email within 1-2 minutes.

## Alternative: Use Existing SMTP

If you have SMTP credentials from another service:

```bash
# Update .env with your SMTP details
SMTP_HOST=your.smtp.server
SMTP_PORT=587  # or 465 for SSL
SMTP_USER=your_email@domain.com
SMTP_PASSWORD=your_password
```

## Skipping SMTP (Not Recommended)

If you want to skip email alerts for now:
- Leave SMTP_USER and SMTP_PASSWORD empty in .env
- Email alerts will be disabled (as shown in test results)
- System will continue to work but won't send alerts

## Verification

After setup, run regression tests to verify:
```bash
python3 -m tests.regression_tests --category alerts
```

Look for: "Email sent successfully" instead of "Email sending disabled"
