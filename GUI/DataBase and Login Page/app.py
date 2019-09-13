from flask import Flask, Response, redirect, url_for, request, abort, render_template, flash
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_required, login_user, logout_user, current_user
from contextlib import contextmanager
import datetime
import sys
import sqlite3
from datetime import datetime
from sqlite3 import *

app = Flask(__name__)

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# Creating the connection to the Database
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///C:\\sqlite3\\logininfo.db'  # The file path to where you want the db
db = SQLAlchemy(app)

# Secret Key for Sessions (just a placeholder for right now)
app.secret_key = 'secretkey'


# Models
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password = db.Column(db.String(80), unique=False, nullable=False)
    is_admin = db.Column(db.Boolean)
    registered_time = db.Column(db.DateTime)  # DateTime is in military time: (0:00 - 23:59)
    last_access = db.Column(db.DateTime)

    authenticated = False

    def is_active(self):
        return True

    def get_id(self):
        """Return the email address to satisfy Flask-Login's requirements."""
        return self.username

    def is_authenticated(self):
        """Return True if the user is authenticated."""
        return self.authenticated

    def is_anonymous(self):
        """False, as anonymous users aren't supported."""
        return False


db.create_all()


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = db.session
    try:
        yield session
        session.commit()

    except Exception as e:
        print(e)
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
        session.rollback()

        raise
    finally:
        session.close()


@login_manager.user_loader
def load_user(userid):
    return User.query.filter_by(username=userid).first()


# Home Page
@app.route('/')
def homepage():
    if current_user.is_authenticated:
        return redirect("/access")
    else:
        return render_template('homepage.html', user=current_user)

# Registration Page
@app.route('/registration', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        is_admin = False
        registered_time = datetime.datetime.today()
        last_access = datetime.datetime.today()

        if not request.form['password'] == request.form['psw-repeat']:
            flash('Please make sure your passwords match')
            return render_template('registration.html')

        with session_scope() as session:
            if session.query(User).filter_by(username=username).first() is None:
                new_user = User(username=username,
                                password=password,
                                is_admin=is_admin,
                                registered_time=registered_time,
                                last_access=last_access)

                session.add(new_user)
                session.flush()

                user = session.query(User).filter_by(username=username).first()
                login_user(user)
                user.is_authenticated()

                return redirect(url_for('access'))
            else:
                flash('Error: Username is already taken')
                return render_template('registration.html', error="Username is already taken")

    else:
        return render_template('registration.html')

# Logging in
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        with session_scope() as session:
            user = session.query(User).filter_by(username=username).first()
            if user:
                if password == user.password:
                    user.last_access = datetime.datetime.today()
                    login_user(user)
                    user.is_authenticated()
                    return redirect(url_for('access'))
            return render_template('login.html',
                                   user=current_user,
                                   error='Incorrect username or password.')

    else:
        return render_template('login.html', user=current_user)


# Logging Out
@app.route('/logout')
def logout():
    logout_user()
    flash('You just logged out!')
    return redirect(url_for('homepage'))

# The access page / where the user goes after logging in
@app.route('/access')
def access():
    if current_user.is_authenticated:
        return render_template("access.html", user=current_user)
    else:
        flash('Please Login First')
        return redirect('/')


if __name__ == '__main__':
    app.run(use_reloader=True, debug=True)
