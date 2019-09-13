from flask import Flask, render_template

app = Flask(__name__)


@app.route('/')
def homepage():
    return """
    <!DOCTYPE html>
    <head>
    <title> My First Webpage </title>
    </head>  
    <body>
    <body style="width: 880px; margin: auto;">
        <h1> I did this in PyCharm and run it off my own laptop</h1>
        <img src = https://i.redd.it/10t649ni0ohy.png>
        <h2> Here is a random image of a random number generator </h2>
        <p>Don't <a href="/welcome">click</a> this button </p>
    </body>
    """


@app.route('/welcome')
def welcome():
    return render_template('welcome.html')


if __name__ == '__main__':
    app.run(use_reloader=True, debug=True)
