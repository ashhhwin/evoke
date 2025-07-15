from flask import Flask, request, redirect, url_for, session, render_template
from flask import g
import threading
import gradio as gr
from datetime import timedelta, datetime, timezone
from google.cloud import secretmanager
from gradio_app_new import app as gradio_app  # This is your big dashboard script

app = Flask(__name__, template_folder="templates")
app.secret_key = "ashwinramv"  # Replace with env/secret manager in production

app.permanent_session_lifetime = timedelta(minutes=0.2)


#USERNAME = "admin"
#PASSWORD = "admin123"
def get_password_from_secret(secret_name: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    project_id = "tonal-nucleus-464617-n2"
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(name=secret_path)
    return response.payload.data.decode("UTF-8")

USERNAME_SECRET_NAME = "EvokeInterns"  # The actual name of the secret
#USERNAME = "Evoke Interns"
#PASSWORD = get_secret(PROJECT_ID,USERNAME_SECRET_NAME) 

@app.before_request
def check_session_expiry():
    session.permanent = True

    # Bypass session checks for login page, static files, favicon, etc.
    if request.path in ["/", "/favicon.ico"] or request.path.startswith("/static"):
        return

    if "logged_in" in session:
        now = datetime.utcnow()
        last_seen_str = session.get("last_seen")
        if last_seen_str:
            try:
                last_seen = datetime.fromisoformat(last_seen_str)
                if (now - last_seen) > app.permanent_session_lifetime:
                    print("⌛ Session expired. Logging out.")
                    session.clear()
                    return redirect("/")
            except Exception as e:
                print("⚠️ Failed to parse session timestamp:", e)
                session.clear()
                return redirect("/")
        session["last_seen"] = now.isoformat()
    else:
        # Not logged in at all
        return redirect("/")

@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        entered_username = request.form{USERNAME_SECRET_NAME}
        entered_password = request.form["password"]
        try:
            actual_password = get_password_from_secret(USERNAME_SECRET_NAME)
            if entered_password == actual_password:
                session.permanent = True
                session["logged_in"] = True
                session["last_seen"] = datetime.utcnow().isoformat()
                return redirect("/dashboard")
            else:
                return render_template("login.html", error="Invalid credentials")
        except Exception as e:
            return render_template("login.html", error="User not found or secret fetch error")

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

