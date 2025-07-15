from flask import Flask, request, redirect, url_for, session, render_template
from flask import g
import threading
import gradio as gr
from datetime import timedelta, datetime, timezone

from gradio_app_new import app as gradio_app  # This is your big dashboard script

app = Flask(__name__, template_folder="templates")
app.secret_key = "ashwinramv"  # Replace with env/secret manager in production

app.permanent_session_lifetime = timedelta(minutes=0.2)

USERNAME = "admin"
PASSWORD = "admin123"

@app.before_request
def check_session_expiry():
    session.permanent = True
    if "logged_in" in session:
        now = datetime.now(timezone.utc).replace(tzinfo=None)  # naive UTC
        if "last_seen" in session:
            try:
                last_seen = session["last_seen"]
                if isinstance(last_seen, str):
                    last_seen = datetime.fromisoformat(last_seen)
                elapsed = now - last_seen
                if elapsed > app.permanent_session_lifetime:
                    print("⌛ Session expired. Logging out.")
                    session.clear()
                    return redirect("/")
            except Exception as e:
                print("⚠️ Error checking session:", e)
                session.clear()
                return redirect("/")
        session["last_seen"] = now.isoformat()

@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if request.form["username"] == USERNAME and request.form["password"] == PASSWORD:
            session.permanent = True  # <--- Add this
            session["logged_in"] = True
            return redirect("/dashboard")
        else:
            return render_template("login.html", error="Invalid credentials")
    return render_template("login.html")


@app.route("/dashboard")
def dashboard():
    print("🔍 Checking login session for /dashboard...")
    if not session.get("logged_in"):
        print("❌ Not logged in! Redirecting to /")
        return redirect("/")
    print("✅ Logged in, rendering dashboard")
    return render_template("dashboard.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect("/")

# Launch Gradio on 127.0.0.1:7869 in the background
def run_gradio():
    gradio_app.launch(server_name="127.0.0.1", server_port=7869, show_error=True, share=False)

# Start Gradio in background when Flask loads
threading.Thread(target=run_gradio, daemon=True).start()

