import requests
import json

# User credentials for authorization
username = "frothkoetter"  # Replace with your username
password = "SAX201linga"  # Replace with your password
api_token = "WldaaU9UWTFNREV0WlRsbE15MDBOV1E0TFdGbU9EWXROekJqWlRaaVpEWm1NRGxsOjpaall6TnpGaU1EVXRNemcyTmkwME4yVTNMV0pqWWpZdE1tSTVZV1JsWkRjNU1qWTE="
jwt_token = "eyJqa3UiOiJodHRwczovL3NlLWF3cy1lZGwtZ2F0ZXdheS5zZS1zYW5kYi5hNDY1LTlxNGsuY2xvdWRlcmEuc2l0ZS9zZS1hd3MtZWRsL2hvbWVwYWdlL2tub3h0b2tlbi9hcGkvdjIvandrcy5qc29uIiwia2lkIjoiYndXMVFMd2pyU2x0NUh1V0RBejk2OUVoYzdnTVZDcEMwNlI0WE9idE11MCIsInR5cCI6IkpXVCIsImFsZyI6IlJTMjU2In0.eyJzdWIiOiJmcm90aGtvZXR0ZXIiLCJhdWQiOiJjZHAtcHJveHktdG9rZW4iLCJqa3UiOiJodHRwczovL3NlLWF3cy1lZGwtZ2F0ZXdheS5zZS1zYW5kYi5hNDY1LTlxNGsuY2xvdWRlcmEuc2l0ZS9zZS1hd3MtZWRsL2hvbWVwYWdlL2tub3h0b2tlbi9hcGkvdjIvandrcy5qc29uIiwia2lkIjoiYndXMVFMd2pyU2x0NUh1V0RBejk2OUVoYzdnTVZDcEMwNlI0WE9idE11MCIsImlzcyI6IktOT1hTU08iLCJleHAiOjE3MzU3NDE0MjIsIm1hbmFnZWQudG9rZW4iOiJ0cnVlIiwia25veC5pZCI6ImVmYjk2NTAxLWU5ZTMtNDVkOC1hZjg2LTcwY2U2YmQ2ZjA5ZSJ9.B_3Z3cxYnwMHhmtZfVsHXh89iPYxhc4Wb0vP-pitzpxwTnvxaeOYZXYSb1RdVevmpins5K4VgE3hYrifUUFMeV_3jf9HK2bhnmw6VsFoX7flmlHFVEWbcdDILG_zNzBVazaGa4Ae7bDzgJPIBtcO92Da2zqndYwTX6sHHWfAqxAoHM-EfPAybrYnsGz8Or8n-zvwZ9my5oLvgGlAK-XE2Cm4enxf4xr7l5sYQXO6segvbrNOXrCydNrNZOBks3idlSX5Ur6oivriMB07o-lJ2bCbgd7U7trqp_NqxKj8mEcRA5wcBf8Et9ptmAKoiYeAzE8jEDG8vHWipsTj1U6yTQ"

# URL and headers from the curl command
url_atlas_api = "https://se-aws-edl-gateway.se-sandb.a465-9q4k.cloudera.site:443/se-aws-edl/cdp-proxy-token/atlas/api/atlas/v2/search/quick"
url_atlas_base = "https://se-aws-edl-gateway.se-sandb.a465-9q4k.cloudera.site:443/se-aws-edl/cdp-proxy-token/atlas/api/atlas/v2/entity/guid"
url = "https://se-aws-edl-gateway.se-sandb.a465-9q4k.cloudera.site/se-aws-edl/cdp-proxy/atlas/api/atlas/v2/search/quick"
headers = {
    "accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": f"Bearer {jwt_token}"  # Use the variable for the token
}

# Query parameters
params = {
    "excludeDeletedEntities": "true",
    "limit": 3,
    "offset": 0,
    "query": "airlinedata.planes_csv",
    "typeName": "hive_table"
}

# set classification tag
def set_tag (guid):
	classification_name = "qa_failed"
	# URL for the request
	url = f"{url_atlas_base}/{guid}/classifications"
	payload = [
  		{
    		 "entityGuid": f"{guid}",
    		 "propagate": "true",
        	 "typeName": f"{classification_name}"
		}
	]
	# Make the POST request to add the tag
	print (url)
	print (payload)
	response = requests.post(url, headers=headers, data=json.dumps(payload))
	# Check the response
	if response.status_code == 204:
    		print("Tag added successfully!")
	else:
                response_json = response.json()
                print(f"Failed to add tag with status code {response.status_code}: {response.text}")

	return response

# Make the GET request with authentication
response = requests.get(url_atlas_api, headers=headers, params=params)
# Handle the response
if response.status_code == 200:
    try:
        # Parse the response JSON
        response_json = response.json()
	# Extract the GUID
        guid = response_json["searchResults"]["entities"][0]["guid"] 
        # print(f"Extracted GUID: {guid}")
        response = set_tag(guid)
    except requests.exceptions.JSONDecodeError:
        print("Error: The response is not valid JSON")
else:
    print(f"Failed with status code {response.status_code}: {response.text}")

