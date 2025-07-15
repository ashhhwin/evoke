from flask import Flask, request, redirect, url_for, session, render_template
import threading
import gradio as gr
from datetime import timedelta


from gradio_app_new import app as gradio_app  # This is your big dashboard script

app = Flask(__name__, template_folder="templates")
app.secret_key = "ashwinramv"  # Replace with env/secret manager in production

app.permanent_session_lifetime = timedelta(minutes=3)

USERNAME = "admin"
PASSWORD = "admin123"

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
    if not session.get("logged_in"):
        return redirect("/")
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

