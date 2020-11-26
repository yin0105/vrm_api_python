import requests, json, re, threading
from pprint import pprint
import mysql.connector

json_data = {}
with open('data.json') as json_file:
    json_data = json.load(json_file)
sum_arr = ["batwatts", "pvcurrent", "pvpower", "yieldtoday", "maxchargetoday", "yieldyest", "maxchargeyest", "useryield", "useryielddelta"]
char_type_arr = ["invertorstate", "invertorerror", "overload", "temperaturealarm", "voltagealarm", "mppt1_error", "mppt2_error", "mppt3_error", "mppt4_error", "mppt5_error", "mppt6_error", "mppt7_error", "mppt1_mode", "mppt2_mode", "mppt3_mode", "mppt4_mode", "mppt5_mode", "mppt6_mode", "mppt7_mode"]



def retrieve():
    threading.Timer(300, retrieve).start()
    cnx = mysql.connector.connect(user='root', password='', host='127.0.0.1', database='vrm')
    cursor = cnx.cursor()
    
    query = ("SELECT vrmuser, vrmpass, vrmapibaseurl, vrmsiteid FROM generalinfo")
    cursor.execute(query)

    for (vrmuser, vrmpass, vrmapibaseurl, vrmsiteid) in cursor:
        print("{}, {}, {}, {}".format(vrmuser, vrmpass, vrmapibaseurl, vrmsiteid))

        url = vrmapibaseurl + 'auth/login'
        account_info = {"username": vrmuser,  "password": vrmpass }
        
        # Get token
        token_userid = requests.post(url, json = account_info).json()
        user_id = token_userid["idUser"]
        token = token_userid["token"]

        # print("user_id = " + str(user_id))
        # print("token = " + token)

        # Retrieve Live Data
        diagnostics_url = vrmapibaseurl + "installations/" + str(vrmsiteid) + "/diagnostics?count=1000"
        hed = {'X-Authorization': 'Bearer ' + token}
        diag_data = requests.get(diagnostics_url, headers=hed).json()

        retrieved_data = {}
        for record in diag_data["records"]:
            idDataAttribute = record["idDataAttribute"]
            instance = record["instance"]
            if str(idDataAttribute) in json_data.keys():
                parse_val =json_data[str(idDataAttribute)]
                parse_val = parse_val[str(instance)]
                var_name = parse_val["name"]
                var_name = var_name.strip()
                val = parse_val["value"]
                val = val.strip()
                retrieve_val = str(record[val])
                if var_name not in char_type_arr:
                    retrieve_val = re.search("^[-]?[0-9]+[.]?[0-9]*", retrieve_val)
                    retrieve_val = retrieve_val.group(0)
                retrieved_data[var_name] = retrieve_val

        # Caculate total
        for s in sum_arr:
            sum = 0
            for i in range(1,8):
                sum += float(retrieved_data["mppt" + str(i) + "_" + s])
            retrieved_data["mppt_tot_" + s] = str(sum)

        # Caculate Systemstatus
        soc = retrieved_data["soc"]
        query = ("Select id from loadmanagementprofiles where " + str(soc) + ">=socactivatefrom and " + str(soc) + "<=socactivate")
        cursor.execute(query)
        id = 0
        for (id,) in cursor:
            pass

        # Insert into database
        query = "INSERT INTO diagnosticdata (datetime_added"
        query_2 = "(NOW()"
        for s in retrieved_data:
            query += ", " + s
            query_2 += ", "
            if s == "record_datetime":
                query_2 += "FROM_UNIXTIME(" + retrieved_data[s] + ")"
            elif s in char_type_arr:
                query_2 += "'" + retrieved_data[s] + "'"
            else:
                query_2 += retrieved_data[s]
        query += ") VALUES " + query_2 + ")"
        query = (query)
        cursor.execute(query)
        cnx.commit()
        print("OK")

    cursor.close()
    cnx.close()

retrieve()