'''
return apllication config dictionary 
'''
import json


# initialize the app config global variable
appConf = {}

def loadAppConfig(fName="secret/config.json"):
    # load config json into the global variable
    with open(fName) as f:
        global appConf
        appConf = json.load(f)
        return appConf
    
def getAppConfig():
    global appConf
    return appConf