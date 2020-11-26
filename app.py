import requests
from pprint import pprint
import mysql.connector

cnx = mysql.connector.connect(user='root', password='', host='127.0.0.1', database='vrm')
cursor = cnx.cursor()

query = ("SELECT vrmuser, vrmpass, vrmapibaseurl FROM generalinfo")

cursor.execute(query)

for (vrmuser, vrmpass, vrmapibaseurl) in cursor:
    print("{}, {}, {}".format(vrmuser, vrmpass, vrmapibaseurl))

    url = vrmapibaseurl + 'auth/login'
    account_info = {"username": vrmuser,  "password": vrmpass }

    token_userid = requests.post(url, json = account_info).json()
    user_id = token_userid["idUser"]
    token = token_userid["token"]

    print("user_id = " + str(user_id))
    print("token = " + token)

cursor.close()
cnx.close()