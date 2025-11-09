import passiogo
import requests
import json
def sendApiRequest(url, body):
	
	# Send Request
	response = requests.post(url, json = body)
	
	try:
		# Handle JSON Response
		response = response.json()
	except Exception as e:
		raise Exception(f"Error converting API response to JSON! Here is the response received: {response}")
		return None
	
	
	# Handle API Error
	if(
		"error" in response and 
		response["error"] != ""
	):
		raise Exception(f"Error in Response! Here is the received response: {response}")
	
	return(response)

def get_all_active_buses(system_id):
    """
    Finds and prints the ID and Name for ALL currently active buses
    on a system.
    """
    
    # 1. Get the transportation system object
    system = passiogo.getSystemFromID(system_id)
    if not system:
        print(f"Error: System with ID {system_id} not found.")
        return

    # 2. Get all currently active vehicles for that system
    all_vehicles = system.getVehicles()
    
    # 3. Check if any vehicles were returned
    if not all_vehicles:
        print(f"No active vehicles found for system {system_id}.")
        return
            
    # 4. Print the ID and Name for every active bus
    print(f"--- Active Buses for System {system_id} ({system.name}) ---")
    for bus in all_vehicles:
        print(f"  ID: {bus.id}, Name: {bus.name}")
        
def get_bus_data(system_id, bus_id, bus_name):
    """
    Finds and prints all data for a specific active bus.
    
    You must provide a system_id, bus_id, AND bus_name.
    The bus must match BOTH parameters to be found.
    """
    
    # 1. Get the transportation system object
    system = passiogo.getSystemFromID(system_id)
    if not system:
        print(f"Error: System with ID {system_id} not found.")
        return

    # 2. Get all currently active vehicles for that system
    all_vehicles = system.getVehicles()
    if not all_vehicles:
        print("No active vehicles found for this system.")
        return

    # 3. Loop through the vehicles to find the one you want
    target_bus = None
    for bus in all_vehicles:
        # Check if BOTH the ID and Name match
        if bus.id == bus_id and bus.name == bus_name:
            target_bus = bus
            break
            
    # 4. Print all data for the found bus
    if target_bus:
        print(f"--- Data for Bus: {target_bus.name} (ID: {target_bus.id}) ---")
        
        # Use __dict__ to access all attributes of the Vehicle object
        # Use default=str to handle non-serializable objects like 'system'
        print(json.dumps(target_bus.__dict__, default=str, indent=2))
        
    else:
        print(f"Bus not found matching ID '{bus_id}' AND Name '{bus_name}'. It may be inactive.")


import requests
import json

def get_raw_bus_data(system_id):
    """
    Makes a direct API call to get the raw vehicle data for a system
    and prints the full, unfiltered JSON response.
    """
    
    # 1. Define the API endpoint and parameters
    # This is the same URL and parameters from your getVehicles function
    base_url = "https://passiogo.com"
    api_url = f"{base_url}/mapGetData.php?getBuses=2"
    
    # This is the payload their script sends
    body = {
        "s0" : str(system_id),
        "sA" : 1
    }
    
    print(f"--- Fetching raw data from {api_url} for System ID: {system_id} ---")
    
    try:
        # 2. Make the POST request
        response = requests.post(api_url, json=body)
        
        # Raise an error if the request failed
        response.raise_for_status() 
        
        # 3. Get the JSON response
        raw_data = response.json()
        
        # 4. Print the raw data, nicely formatted
        print("--- Full Raw API Response ---")
        print(json.dumps(raw_data, indent=2))

        # 5. Point out where the bus data is
        if "buses" in raw_data and raw_data["buses"]:
            print(f"\n--- Note: Bus details are inside the 'buses' key. ---")
        else:
            print("\n--- Note: No 'buses' key found or list is empty. ---")

    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
    except json.JSONDecodeError:
        print("Failed to decode API response. Here is the raw text:")
        print(response.text)
        
def get_eta_data_v2(
    eta_version: int,
    device_id: str,
    stop_id: str,
    route_id: str,
    position: int,
    user_id: int,
    route_ids_list: list
):
    """
    Gets ETA data based on the new (undocumented) endpoint URL format.
    
    This follows the pattern of the getSystems() function, sending all
    parameters in the URL query string and a 'None' body.
    """
    
    # 1. Format the route_ids_list into a comma-separated string
    # E.g., ['54543', '54550'] -> "54543,54550"
    route_ids_str = ",".join(route_ids_list)
    
    # 2. Build the full URL
    # This structure matches your working example
    url = (
        f"{passiogo.BASE_URL}/mapGetData.php"
        f"?eta={eta_version}"
        f"&deviceId={device_id}"
        f"&stopIds={stop_id}"
        f"&routeId={route_id}"
        f"&position={position}"
        f"&userId={user_id}"
        f"&routeIds={route_ids_str}"
    )
    
    print(f"Calling API (via sendApiRequest): {url}")
    
    # 3. Call the API using the existing helper function
    try:
        eta_data = sendApiRequest(url, None)
        return eta_data
    except Exception as e:
        print(f"Error getting ETA data: {e}")
        return None

# --- Place this at the very end of your file ---

# Use the example ID from your code's getVehicles() documentation
SYSTEM_ID = 1268      # Example: University of Chicago
BUS_ID = 15188       # Example: Bus ID from the documentation
BUS_NAME = "0129" # Example: Bus Name from the documentation

#print("\n--- Testing New ETA Function (v2) ---")

# These are the values from the URL you found:
eta_info = get_eta_data_v2(
    eta_version=3,
    device_id="25820292",
    stop_id="10036",
    route_id="54543",
    position=9,
    user_id=1268,
    route_ids_list=["54543", "54550"]
)

if eta_info:
    print("--- Raw ETA Response ---")
    print(json.dumps(eta_info, indent=2))
else:
    print("Failed to get ETA info. Check example IDs or network error.")
#get_all_active_buses(SYSTEM_ID)
# Call the function with both required arguments
#get_bus_data(system_id=SYSTEM_ID, bus_id=BUS_ID, bus_name=BUS_NAME)
#get_raw_bus_data(SYSTEM_ID)



# -----------------------------------------------------------------
# --- ADD THIS CODE TO THE END OF YOUR EXISTING SCRIPT ---
# -----------------------------------------------------------------

def print_all_route_names(system):
    """
    Gets all routes for the system and prints ONLY their names.
    """
    print(f"--- 2. Fetching All Route Names for {system.name} ---")
    
    try:
        routes = system.getRoutes()
        
        if not routes:
            print("No routes found for this system.")
            return
            
        print(f"Found {len(routes)} total routes:")
        for route in routes:
            print(f"  - {route.name} (ID: {route.id}, MyID: {route.myid})")
            
    except Exception as e:
        print(f"An error occurred: {e}")
        
    print("-" * 40 + "\n")


def print_data_samples(system):
    """
    Prints a sample of stops and active buses.
    """
    
    # --- Helper to print objects as JSON ---
    def print_as_json(title, objects_list, limit=3):
        print(f"--- {title} (Showing first {limit}) ---")
        
        if not objects_list:
            print(f"No {title.lower()} found.")
            print("-" * 40 + "\n")
            return

        data_to_print = [obj.__dict__ for obj in objects_list[:limit]]
        print(json.dumps(data_to_print, indent=2, default=str))
        print(f"(Total {title.lower()}: {len(objects_list)})")
        print("-" * 40 + "\n")

    # Get Stops
    stops = system.getStops()
    print_as_json("3. All Stops", stops, limit=1) # Just show 1 to keep it short
    
    # Get Vehicles (Buses)
    vehicles = system.getVehicles()
    print_as_json("4. Active Vehicles (Buses)", vehicles, limit=1)


# --- This is the main part that runs ---
if __name__ == "__main__":
    
    print("--- 1. Finding Rutgers University System ID ---")
    
    all_systems = passiogo.getSystems()
    rutgers_system = None
    
    for system in all_systems:
        if "rutgers" in system.name.lower():
            rutgers_system = system
            break
    
    if not rutgers_system:
        print("Error: Could not find a system with 'Rutgers' in the name.")
    else:
        print(f"Found system: {rutgers_system.name} (ID: {rutgers_system.id})\n")
        
        # --- Run the functions ---
        print_all_route_names(rutgers_system)
        print_data_samples(rutgers_system)