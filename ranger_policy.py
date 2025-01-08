import requests
import json

# Ranger and Atlas API URLs and credentials
RANGER_BASE_URL = "https://se-aws-edl-gateway.se-sandb.a465-9q4k.cloudera.site/se-aws-edl/cdp-proxy-api/ranger"
RANGER_TAG_API = f"{RANGER_BASE_URL}/service/tags"
RANGER_ENTITY_API = f"{RANGER_BASE_URL}/service/tags/asset"
ATLAS_BASE_URL = "https://se-aws-edl-gateway.se-sandb.a465-9q4k.cloudera.site/se-aws-edl/cdp-proxy-api/atlas"
ATLAS_ENTITY_API = f"{ATLAS_BASE_URL}/api/atlas/v2/entity"
AUTH = ("frothkoetter", "SAX201linga")  # Replace with your credentials

# Function to get the GUID of a Hive table using Atlas
def get_hive_table_guid(database_name, table_name):
    try:
        search_url = f"{ATLAS_ENTITY_API}/search"
        params = {
            "typeName": "hive_table",
            "query": f"qualifiedName:{table_name}@{database_name}"
        }

        response = requests.get(search_url, auth=AUTH, params=params)
        if response.status_code != 200:
            print(f"Failed to search Hive table: {response.text}")
            return None

        results = response.json()
        if results.get("entities"):
            return results["entities"][0]["guid"]
        else:
            print(f"Table '{table_name}' not found in database '{database_name}'.")
            return None
    except Exception as e:
        print(f"Error fetching GUID: {e}")
        return None

# Function to add a tag to an entity
def add_tag_to_entity(entity_guid, tag_name):
    tag_payload = {
        "type": "tag",
        "name": tag_name,
        "attributes": {},
        "typeDefName": "classification"
    }

    try:
        # Check if the tag exists or create it
        tag_response = requests.post(RANGER_TAG_API, auth=AUTH, json=tag_payload)
        if tag_response.status_code not in [200, 201]:
            print(f"Failed to create or retrieve tag: {tag_response.text}")
            return

        # Extract tag ID from the response
        tag_data = tag_response.json()
        tag_guid = tag_data.get("guid")

        # Attach tag to entity
        attach_payload = {
            "entityGuid": entity_guid,
            "tagGuid": tag_guid
        }
        attach_response = requests.post(RANGER_ENTITY_API, auth=AUTH, json=attach_payload)
        if attach_response.status_code not in [200, 201]:
            print(f"Failed to attach tag: {attach_response.text}")
        else:
            print(f"Tag '{tag_name}' added successfully to entity with GUID {entity_guid}.")

    except Exception as e:
        print(f"Error: {e}")

# Function to remove a tag from an entity
def remove_tag_from_entity(entity_guid, tag_name):
    try:
        # Retrieve entity details to find associated tags
        entity_response = requests.get(f"{RANGER_ENTITY_API}/{entity_guid}", auth=AUTH)
        if entity_response.status_code != 200:
            print(f"Failed to retrieve entity: {entity_response.text}")
            return

        entity_data = entity_response.json()
        tags = entity_data.get("tags", [])
        tag_guid = None

        # Find the tag GUID for the given tag name
        for tag in tags:
            if tag.get("name") == tag_name:
                tag_guid = tag.get("guid")
                break

        if not tag_guid:
            print(f"Tag '{tag_name}' not found on entity with GUID {entity_guid}.")
            return

        # Detach the tag from the entity
        detach_response = requests.delete(f"{RANGER_ENTITY_API}/{entity_guid}/tags/{tag_guid}", auth=AUTH)
        if detach_response.status_code != 200:
            print(f"Failed to remove tag: {detach_response.text}")
        else:
            print(f"Tag '{tag_name}' removed successfully from entity with GUID {entity_guid}.")

    except Exception as e:
        print(f"Error: {e}")

# Example usage
DATABASE_NAME = "airlinedata"
TABLE_NAME = "planes_csv"
TAG_NAME = "confidential"

# Step 1: Get the GUID of the Hive table
table_guid = get_hive_table_guid(DATABASE_NAME, TABLE_NAME)

if table_guid:
    # Step 2: Add a tag to the table
    add_tag_to_entity(table_guid, TAG_NAME)

    # Step 3: Remove a tag from the table (optional)
    remove_tag_from_entity(table_guid, TAG_NAME)

