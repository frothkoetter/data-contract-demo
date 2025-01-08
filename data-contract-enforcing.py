import requests
import json
import sys
import base64 

def enforce_data_contract(table_name, success):
    """
    Function to enforce data contract based on table_name and success flag.
    Args:
        table_name (str): Name of the table to enforce the contract on
        success (bool): Success flag to indicate whether the contract is successful
    """
    print(f"Enforcing data contract for table: {table_name}")
    print(f"Contract enforcement status: {'Success' if success else 'Failure'}")

    # Add your logic to enforce the data contract here
    # Example logic
    if success:
        print(f"Data contract for {table_name} is active.")
    else:
        print(f"Data contract for {table_name} is broken.")
 
# set classification tag
def set_tag (guid,classification_name,status):
        # URL for the request to update entitiy
        url = f"{url_atlas_update}/{guid}/classifications"
        payload = [
                 {
                 "entityGuid": f"{guid}",
                 "propagate": "true",
                 "typeName": f"{classification_name}",
                       "attributes": {
                         "Status": f"{status}"
              }
           }
        ]
        # Make the POST request to add the tag
        print (url)
        print (payload)
        # Update the tage status attribute
        response = requests.put(url, headers=headers, data=json.dumps(payload))
        # Check the response
        if response.status_code == 204:
                 print("Tag added successfully!")
        elif response.status_code == 400:
                 # asssign the tag and set status
                 response = requests.post(url, headers=headers, data=json.dumps(payload))
                 if response.status_code == 204:
                          print("Tag added successfully!")
        else:
             response_json = response.json()
             print(f"Failed to add tag with status code {response.status_code}: {response.text}")

        return response

if __name__ == "__main__":
    # Get parameters from command-line arguments
    table_name = sys.argv[1]
    success = bool(int(sys.argv[2]))  # Convert to boolean

#    enforce_data_contract(table_name, success)
# User credentials for authorization
username = "frothkoetter"  # Replace with your username
password = "SAX201linga"  # Replace with your password
credentials = f"{username}:{password}"
encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')

# URL and headers from the curl command
url_atlas_search = "https://se-aws-edl-gateway.se-sandb.a465-9q4k.cloudera.site:443/se-aws-edl/cdp-proxy-api/atlas/api/atlas/v2/search/quick"
url_atlas_update = "https://se-aws-edl-gateway.se-sandb.a465-9q4k.cloudera.site:443/se-aws-edl/cdp-proxy-api/atlas/api/atlas/v2/entity/guid"

url = "https://se-aws-edl-gateway.se-sandb.a465-9q4k.cloudera.site/se-aws-edl/cdp-proxy/atlas/api/atlas/v2/search/quick"
# url_atlas = "https://se-aws-edl-gateway.se-sandb.a465-9q4k.cloudera.site/se-aws-edl/cdp-proxy-api/atlas/api/atlas/"
headers = {
    "accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": f"Basic {encoded_credentials}" 
}

#    "Authorization": f"Bearer {jwt_token}"  # Use the variable for the token
# Query parameters
params = {
    "excludeDeletedEntities": "true",
    "limit": 1,
    "offset": 0,
    "query": table_name ,
    "typeName": "hive_table"
}

# Query Atlas for tablename and get GUID 
response = requests.get(url_atlas_search, headers=headers, params=params)

# Handle the response
if response.status_code == 200:
    try:
        # Parse the response JSON
        response_json = response.json()
	# Extract the GUID
        guid = response_json["searchResults"]["entities"][0]["guid"] 
        print(f"Extracted GUID: {guid}")
	# set the tag attributes status
        if success:
           response = set_tag(guid, "DataContract", "active")
        else:
           response = set_tag(guid, "DataContract", "broken")
    except requests.exceptions.JSONDecodeError:
        print("Error: The response is not valid JSON")
else:
    print(f"Failed with status code {response.status_code}: {response.text}")

