from flask import Flask, render_template, request, redirect, session

app = Flask(__name__, template_folder="templates")
app.secret_key = "your-secret-key"

@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if request.form["username"] == "admin" and request.form["password"] == "admin123":
            session["logged_in"] = True
            return "Login successful!"
        else:
            return render_template("login.html", error="Invalid credentials")
    return render_template("login.html")

@app.route("/dashboard")
def dashboard():
    if not session.get("logged_in"):
        return redirect("/")
    return "Dashboard Loaded"
