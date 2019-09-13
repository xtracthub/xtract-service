
from flask import Flask, request
app = Flask(__name__)


@app.route('/extract', methods=['POST'])
def extract_mdata():

  req = request.get_data()

  print(req)

  return 'Hello from Tyler!'

if __name__ == '__main__':
  app.run()

