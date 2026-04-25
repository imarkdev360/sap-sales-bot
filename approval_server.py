import json
import threading
import requests
from markupsafe import escape
from flask import Flask, request, render_template_string
from db_helper import DatabaseHandler
from sap_handler import SAPHandler
from config import SALES_BOT_TOKEN, APPROVAL_SERVER_PORT
from logger_setup import get_logger
from sap_security import ApprovalServerSecurity

logger = get_logger(__name__)

app = Flask(__name__)
db = DatabaseHandler()
sap = SAPHandler()

# --- Security: IP whitelist + rate limiting on approval endpoints ---
_security = ApprovalServerSecurity()

@app.before_request
def _enforce_security():
    _security.before_request()

REJECT_FORM_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Reject Order</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; text-align: center; padding: 20px; background-color: #f8f9fa; }
        .container { background: white; padding: 40px; border-radius: 12px; box-shadow: 0px 4px 20px rgba(0,0,0,0.1); max-width: 500px; margin: auto; }
        h2 { color: #e74c3c; margin-bottom: 10px; }
        textarea { width: 100%; height: 120px; padding: 12px; margin-top: 10px; border-radius: 8px; border: 1px solid #ccc; font-size: 14px; }
        button { background-color: #e74c3c; color: white; padding: 12px 25px; border: none; border-radius: 6px; cursor: pointer; font-size: 16px; margin-top: 20px; width: 100%; font-weight: bold; }
        button:hover { background-color: #c0392b; }
    </style>
</head>
<body>
    <div class="container">
        <h2>Reject Request #{{ db_id }}</h2>
        <p>Please provide a reason for rejection:</p>
        <form action="/reject_confirm" method="POST">
            <input type="hidden" name="token" value="{{ token }}">
            <textarea name="reason" placeholder="Type reason here..." required></textarea><br>
            <button type="submit">Send Rejection</button>
        </form>
    </div>
</body>
</html>
"""


@app.route('/approve', methods=['GET'])
def approve_order():
    token = request.args.get('token')
    row = db.conn.execute("SELECT * FROM pending_approvals WHERE token = ?", (token,)).fetchone()

    if not row: return "<h1>Invalid Link</h1>"
    if row['status'] != 'PENDING': return f"<h1>Already {row['status']}</h1>"

    # Process Approval
    db.update_status(row['id'], "APPROVED")
    logger.info("Order #%s approved via web", row['id'])

    # Create SAP Order
    try:
        # SECURITY FIX: Use json.loads instead of eval() to prevent code injection
        order_data = json.loads(row['order_data'])
        res = sap.create_sales_order(order_data['customer'], order_data['items'], order_data['ref'], row['discount'])

        if res['success']:
            msg = f"Approval Success! SAP Order: {res['id']}"
            resp_html = f"<h1 style='color:green'>Order Created: {int(res['id'])}</h1>"
        else:
            msg = f"Approved but SAP Failed: {res['error']}"
            resp_html = "<h1 style='color:orange'>Approved but SAP creation failed. Check logs.</h1>"

        # Notify
        db.add_notification(row['user_id'], msg)
        try:
            requests.post(f"https://api.telegram.org/bot{SALES_BOT_TOKEN}/sendMessage",
                          json={"chat_id": row['user_id'], "text": msg}, timeout=10)
        except requests.RequestException as e:
            logger.warning("Telegram notification failed for approval #%s: %s", row['id'], e)

        return resp_html

    except (json.JSONDecodeError, KeyError, requests.RequestException) as e:
        logger.error("Approval processing error for #%s: %s", row['id'], e, exc_info=True)
        return "<h1>Processing Error. Check server logs.</h1>"


@app.route('/reject_view', methods=['GET'])
def reject_view():
    token = request.args.get('token')
    row = db.conn.execute("SELECT id, status FROM pending_approvals WHERE token = ?", (token,)).fetchone()

    if not row: return "<h1>Invalid Link</h1>"
    if row['status'] != 'PENDING': return f"<h1>Already {escape(str(row['status']))}</h1>"
    # SECURITY: escape db_id to prevent SSTI
    return render_template_string(REJECT_FORM_HTML, token=escape(str(token)), db_id=int(row['id']))


@app.route('/reject_confirm', methods=['POST'])
def reject_confirm():
    token = request.form.get('token')
    reason = request.form.get('reason')
    row = db.conn.execute("SELECT * FROM pending_approvals WHERE token = ?", (token,)).fetchone()

    if not row or row['status'] != 'PENDING': return "<h1>Error</h1>"

    db.update_status(row['id'], "REJECTED")
    logger.info("Order #%s rejected via web. Reason: %s", row['id'], reason)

    msg = f"Request #{row['id']} Rejected.\nReason: {reason}"
    db.add_notification(row['user_id'], msg)
    try:
        requests.post(f"https://api.telegram.org/bot{SALES_BOT_TOKEN}/sendMessage",
                      json={"chat_id": row['user_id'], "text": msg}, timeout=10)
    except requests.RequestException as e:
        logger.warning("Telegram notification failed for rejection #%s: %s", row['id'], e)

    return "<h1 style='color:red'>Rejection Sent.</h1>"


def start_background_server():
    # SECURITY: Bind to localhost only — use a reverse proxy (nginx) for external access
    import os
    bind_host = os.environ.get("APPROVAL_SERVER_HOST", "127.0.0.1")
    t = threading.Thread(target=lambda: app.run(host=bind_host, port=APPROVAL_SERVER_PORT, debug=False, use_reloader=False))
    t.daemon = True
    t.start()
    logger.info("Approval server started on %s:%s", bind_host, APPROVAL_SERVER_PORT)
