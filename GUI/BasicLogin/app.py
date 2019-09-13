from flask import Flask, render_template, request, url_for
from werkzeug.utils import redirect

app = Flask(__name__)

# Home Page
@app.route('/')
def homepage():
    return render_template('homepage.html')

# Secret Page
@app.route('/access.html')
def welcome():
    return render_template('access.html')

# Login Page
@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        if request.form['username'] != 'Henry' or request.form['password'] != 'isthebomb':
            error = 'Wrong username / password. Usernames and passwords are case sensitive.'
        else:
            return render_template('/access.html')
    return render_template('login.html', error=error)


if __name__ == '__main__':
    app.run(use_reloader=True, debug=True)
