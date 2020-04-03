



import sys
from exceptions import RemoteExceptionWrapper, XtractError

try:
    raise XtractError("Hello!")
except:
    v = RemoteExceptionWrapper(*sys.exc_info())

print(v.reraise())