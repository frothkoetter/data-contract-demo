import requests
import json
import sys
import base64

def enforce_data_contract(table_name, success):
    """
    Enforce data contract based on table name and success flag.

    Args:
        table_name (str): Name of the table.
        success (bool): Indicates if the contract enforcement was successful.
    """
    print(f"Enforcing data contract for table: {table_name}")
    status_message = "Success" if success else "Failure"
    print(f"Contract enforcement status: {status_message}")

    if success:
        print(f"Data contract for {table_name} is active.")
    else:
        print(f"Data contract for {table_name} is broken.")

def set_tag(guid, classification_name, status):
    """
    Set or update the classification tag for an entity in Apache Atlas.

    Args:
        guid (str): The GUID of the entity.
        classification_name (str): The name of the classification.
        status (str): The status attribute for the classification.

    Returns:
        response: The response object from the API call.
    """
    url = f"{url_atlas_update}/{guid}/classifications"
    payload = [
        {
            "entityGuid": guid,
            "propagate": True,
            "typeName": classification_name,
            "attributes": {
                "Status": status
            }
        }
    ]

    print(f"URL: {url}")
    print(f"Payload: {json.dumps(payload, indent=2)}")

    response = requests.put(url, headers=headers, data=json.dumps(payload))
    
    if response.status_code == 204:
        print("Tag added successfully!")
    elif response.status_code == 400:
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        if response.status_code == 204:
            print("Tag added successfully!")
    else:
        print(f"Failed to add tag with status code {response.status_code}: {response.text}")

    return response

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python script.py <table_name> <success_flag>")
        sys.exit(1)

    table_name = sys.argv[1]
    success = bool(int(sys.argv[2]))

    username = "frothkoetter"  # Replace with actual username
    password = "SAX201linga"  # Replace with actual password
    credentials = f"{username}:{password}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')

    url_atlas_search = "https://se-aws-edl-gateway.se-sandb.a465-9q4k.cloudera.site:443/se-aws-edl/cdp-proxy-api/atlas/api/atlas/v2/search/quick"
    url_atlas_update = "https://se-aws-edl-gateway.se-sandb.a465-9q4k.cloudera.site:443/se-aws-edl/cdp-proxy-api/atlas/api/atlas/v2/entity/guid"

    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Basic {encoded_credentials}"
    }

    params = {
        "excludeDeletedEntities": "true",
        "limit": 1,
        "offset": 0,
        "query": table_name,
        "typeName": "hive_table"
    }

print ( params) 
response = requests.get(url_atlas_search, headers=headers, params=params)

if response.status_code == 200:
    try:
        response_json = response.json()
        print ( response_json)    
        # Check if 'entities' is present and contains results
        if 'entities' in response_json and response_json["searchResults"]:
            # Extract the GUID
            guid = response_json["searchResults"]["entities"][0]["guid"]
            print(f"Extracted GUID: {guid}")

            classification_status = "active" if success else "broken"
            set_tag(guid, "DataContract", classification_status)
        else:
            print("No entities found in the response. Check if the table name is correct or if it exists.")
    except (KeyError, requests.exceptions.JSONDecodeError) as e:
        print(f"Error parsing response: {e}")
else:
    print(f"Failed with status code {response.status_code}: {response.text}")
