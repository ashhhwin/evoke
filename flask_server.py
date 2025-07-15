from flask import Flask, request, redirect, url_for, session, render_template
import threading
import gradio as gr
from datetime import timedelta


from gradio_app_new import app as gradio_app  # This is your big dashboard script

app = Flask(__name__, template_folder="templates")
app.secret_key = "ashwinramv"  # Replace with env/secret manager in production

app.permanent_session_lifetime = timedelta(minutes=15)

USERNAME = "admin"
PASSWORD = "admin123"

@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if request.form["username"] == USERNAME and request.form["password"] == PASSWORD:
            session["logged_in"] = True
            return redirect("/dashboard")
        else:
            return render_template("login.html", error="Invalid credentials")
    return render_template("login.html")

@app.route("/dashboard")
def dashboard():
    if not session.get("logged_in"):
        return redirect("/")
    # Instead of redirecting to localhost:7869 (which works only inside the VM),
    # redirect the client browser to the full external IP and Gradio port
    return redirect("http://34.162.66.126:7869")  # ⬅️ IMPORTANT: use external IP

@app.route("/logout")
def logout():
    session.clear()
    return redirect("/")

# Launch Gradio on 127.0.0.1:7869 in the background
def run_gradio():
    gradio_app.launch(server_name="0.0.0.0", server_port=7869)

# Start Gradio in background when Flask loads
threading.Thread(target=run_gradio, daemon=True).start()

