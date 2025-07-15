from flask import Flask, request, redirect, url_for, session, render_template
import gradio as gr
import threading
import os

from gradio_app_new import app as gradio_app  # Your full dashboard

app = Flask(__name__)
app.secret_key = "admin"  # Replace with a strong key

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
        return redirect(url_for("login"))
    return redirect("http://127.0.0.1:7869")

@app.route("/logout")
def logout():
    session.clear()
    return redirect("/")

# Run Gradio in a background thread
def run_gradio():
    gradio_app.launch(server_name="127.0.0.1", server_port=7869, share=False)

if __name__ == "__main__":
    threading.Thread(target=run_gradio).start()
    app.run(host="0.0.0.0", port=5000)
