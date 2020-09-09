
import pickle
from google.auth.transport.requests import Request


with open('../examples/token.pickle', 'rb') as token:
    creds = pickle.load(token)
    print(creds.refresh_token)


if creds and creds.expired and creds.refresh_token:
    creds.refresh(Request())