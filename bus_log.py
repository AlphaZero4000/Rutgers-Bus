import json
import requests
import sqlite3
import sys
import time
import math
import asyncio
import aiohttp
import ssl
import certifi
# ==================================================================
# --- 1. ALL ORIGINAL PASSIOGO CLASSES (Slightly modified) ---
# ==================================================================

BASE_URL = "https://passiogo.com"
VERBOSE = False
def toIntInclNone(toInt):
    if toInt is None:
        return toInt
    try:
        return int(toInt)
    except (ValueError, TypeError):
        return None

def sendApiRequest(url, body):
    try:
        response = requests.post(url, json=body)
        response.raise_for_status()
        response_json = response.json()
    except Exception as e:
        print(f"Error in sendApiRequest for {url}: {e}")
        return None

    if "error" in response_json and response_json["error"] != "":
        print(f"API Error in Response: {response_json}")
        return None
    
    return response_json
def haversine_distance_feet(lat1, lon1, lat2, lon2):
    """
    Calculates the distance between two lat/lon coordinates in feet
    using the Haversine formula.
    """
    # Earth's radius in feet
    EARTH_RADIUS_FEET = 20902000 
    
    # Convert degrees to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    
    # Haversine formula
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    distance = EARTH_RADIUS_FEET * c
    return distance

def find_arrived_stop(conn, bus_lat, bus_lon):
    """
    Finds the first stop (if any) that the bus is currently inside.
    """
    if bus_lat is None or bus_lon is None:
        return None
        
    try:
        # --- THIS IS THE FIX ---
        # Convert the bus's lat/lon strings to floats for calculation
        bus_lat_float = float(bus_lat)
        bus_lon_float = float(bus_lon)
        # --- END OF FIX ---
        
        c = conn.cursor()
        # This assumes your 'Stops' table has a 'radius' column
        c.execute("SELECT stop_id, latitude, longitude, radius FROM Stops")
        all_stops = c.fetchall()
        
        for (stop_id, stop_lat, stop_lon, radius) in all_stops:
            if stop_lat is None or stop_lon is None or radius is None:
                continue
                
            # Calculate distance using the new float values
            distance = haversine_distance_feet(bus_lat_float, bus_lon_float, stop_lat, stop_lon)
            
            # Check if we are inside the radius
            if distance <= radius:
                return stop_id # Return the ID of the stop we're at
                
        # If we loop through all stops and find no match
        return None
        
    except (ValueError, TypeError) as e:
        # This will catch the error if float(bus_lat) fails
        print(f"  > Error converting bus lat/lon to float in find_arrived_stop: {e}", file=sys.stderr)
        return None
    except sqlite3.OperationalError as e:
        print(f"  > CRITICAL ERROR: Could not find 'radius' column in 'Stops' table.")
        print(f"  > You must update your metadata script to include the 'radius' field.")
        print(f"  > Error: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"  > Error in find_arrived_stop: {e}", file=sys.stderr)
        return None
    
class TransportationSystem:
    def __init__(self, id: int, name: str = None, **kwargs):
        self.id = id
        self.name = name
        self.goAgencyName = kwargs.get('goAgencyName')
        self.homepage = kwargs.get('homepage')
        # ... (other attributes not needed for this script) ...

    def getRoutes(self) -> list["Route"]:
        url = BASE_URL + f"/mapGetData.php?getRoutes=1"
        body = {"systemSelected0": str(self.id), "amount": 1}
        routes = sendApiRequest(url, body)
        
        if routes is None: return []
        if "all" in routes: routes = routes["all"]
        
        allRoutes = []
        for route in routes:
            allRoutes.append(Route(system=self, **route))
        return allRoutes

    def getStops(self) -> list["Stop"]:
        url = BASE_URL + "/mapGetData.php?getStops=2"
        body = {"s0": str(self.id), "sA": 1}
        stops = sendApiRequest(url, body)
        
        if stops is None: return []
        if stops.get("routes") == []: stops["routes"] = {}
        if stops.get("stops") == []: stops["stops"] = {}

        routesAndStops = {}
        for routeId, route in stops.get("routes", {}).items():
            routesAndStops[routeId] = [stop[1] for stop in route[2:] if stop != 0]

        allStops = []
        for id, stop in stops.get("stops", {}).items():
            routesAndPositions = {}
            for routeId, stop_ids in routesAndStops.items():
                if stop.get("id") in stop_ids:
                    routesAndPositions[routeId] = [i for i, x in enumerate(stop_ids) if x == stop.get("id")]
            allStops.append(Stop(system=self, routesAndPositions=routesAndPositions, **stop))
        return allStops

    def getVehicles(self) -> list["Vehicle"]:
        url = BASE_URL + "/mapGetData.php?getBuses=2"
        body = {"s0": str(self.id), "sA": 1}
        vehicles = sendApiRequest(url, body)
        
        if vehicles is None or "buses" not in vehicles:
            return []
        
        allVehicles = []
        for vehicleId, vehicle_data in vehicles["buses"].items():
            if vehicleId == '-1' or not vehicle_data:
                continue
            
            vehicle = vehicle_data[0]
            # Rename API keys to match class attributes
            vehicle['id'] = vehicle.pop('busId', None)
            vehicle['name'] = vehicle.pop('busName', None)
            vehicle['type'] = vehicle.pop('busType', None)
            vehicle['routeName'] = vehicle.pop('route', None)
            vehicle['paxLoad'] = vehicle.pop('paxLoad100', None)

            allVehicles.append(Vehicle(system=self, **vehicle))
        return allVehicles

def getSystems() -> list["TransportationSystem"]:
    url = f"{BASE_URL}/mapGetData.php?getSystems=2&sortMode=1&credentials=1"
    systems = sendApiRequest(url, None)
    if systems is None: return []
    
    allSystems = []
    for system in systems.get("all", []):
        system['id'] = toIntInclNone(system.get('id'))
        system['name'] = system.get('fullname')
        if system['id'] is not None:
            allSystems.append(TransportationSystem(**system))
    return allSystems

class Route:
    def __init__(self, system: TransportationSystem, **kwargs):
        self.system = system
        self.id = kwargs.get('id')
        self.myid = kwargs.get('myid')
        self.systemId = toIntInclNone(kwargs.get('userId'))
        self.name = kwargs.get('name')
        self.shortName = kwargs.get('shortName')
        self.groupColor = kwargs.get('groupColor')

class Stop:
    def __init__(self, system: TransportationSystem, **kwargs):
        self.system = system
        self.id = kwargs.get('id')
        self.systemId = toIntInclNone(kwargs.get('userId'))
        self.name = kwargs.get('name')
        self.latitude = kwargs.get('latitude')
        self.longitude = kwargs.get('longitude')
        self.routesAndPositions = kwargs.get('routesAndPositions', {})

class Vehicle:
    def __init__(self, system: TransportationSystem, **kwargs):
        self.system = system
        self.id = toIntInclNone(kwargs.get('id'))
        self.name = kwargs.get('name')
        self.type = kwargs.get('type')
        self.routeId = toIntInclNone(kwargs.get('routeId')) # This is the route_myid
        self.routeName = kwargs.get('routeName')
        self.latitude = kwargs.get('latitude')
        self.longitude = kwargs.get('longitude')
        self.speed = kwargs.get('speed')
        self.paxLoad = kwargs.get('paxLoad')
        self.outOfService = toIntInclNone(kwargs.get('outOfService'))
        self.tripId = toIntInclNone(kwargs.get('more')) # 'more' field seems to be tripId
        self.calculatedCourse = kwargs.get('calculatedCourse')


# ==================================================================
# --- 2. ETA & DATABASE SCRIPT ---
# ==================================================================

DB_FILE = "rutgers_buses.db"

def create_connection(db_file):
    """ Create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = ON")
        print(f"Connected to SQLite database: {db_file}")
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}", file=sys.stderr)
        return None

# --- MODIFIED: This is the new, normalized Bus_Logs table ---
def create_bus_log_table(conn):
    """ Creates the new, simplified Bus_Logs table """
    sql_statement = """
    CREATE TABLE IF NOT EXISTS Bus_Logs (
        log_id             INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp          INTEGER,
        bus_id             INTEGER,
        route_myid         INTEGER,
        latitude           REAL,
        longitude          REAL,
        pax_load           REAL,
        arrived_stop_id    INTEGER, 
        
        FOREIGN KEY (bus_id) REFERENCES Buses (bus_id),
        FOREIGN KEY (route_myid) REFERENCES Routes (route_myid),
        FOREIGN KEY (arrived_stop_id) REFERENCES Stops (stop_id)
    );
    """
    try:
        c = conn.cursor()
        c.execute(sql_statement)
        conn.commit()
        print("Bus_Logs table created or already exists.")
    except Exception as e:
        print(f"Error creating Bus_Logs table: {e}", file=sys.stderr)

# --- NEW: This is the table for storing all ETAs ---
def create_eta_log_table(conn):
    """ Creates the new table to store all ETAs """
    sql_statement = """
    CREATE TABLE IF NOT EXISTS ETA_Logs (
        log_id             INTEGER,
        stop_id            INTEGER,
        eta_seconds        INTEGER,
        sort_order         INTEGER,
        
        PRIMARY KEY (log_id, sort_order),
        FOREIGN KEY (log_id) REFERENCES Bus_Logs (log_id) ON DELETE CASCADE,
        FOREIGN KEY (stop_id) REFERENCES Stops (stop_id)
    );
    """
    try:
        c = conn.cursor()
        c.execute(sql_statement)
        conn.commit()
        print("ETA_Logs table created or already exists.")
    except Exception as e:
        print(f"Error creating ETA_Logs table: {e}", file=sys.stderr)

def parse_pax_load(pax_load_str):
    """
    Converts a paxLoadS string (e.g., '117%') into a float (e.g., 117.0).
    """
    if pax_load_str is None:
        return None
    try:
        # Remove '%' and convert to float
        return float(pax_load_str.replace('%', ''))
    except (ValueError, TypeError):
        return None

def get_eta_data(system_id: int, route_id: int, stop_id: int):
    """
    Gets ETA data for a specific stop on a route.
    (kept for backward-compatibility; synchronous single-call fallback)
    """
    
    # Build the simple, working URL
    url = (
        f"{BASE_URL}/mapGetData.php"
        f"?eta=3"
        f"&stopIds={stop_id}"
        f"&routeId={route_id}"
    )
    
    # Call the API using requests.get()
    try:
        response = requests.get(url)
        response.raise_for_status()  # Check for HTTP errors
        
        eta_data = response.json()
        
        if "error" in eta_data and eta_data["error"] != "":
            print(f"  > API Error in Response: {eta_data}")
            return None
            
        return eta_data
    
    except Exception as e:
        print(f"  > Error getting ETA data: {e}")
        return None
    
async def _fetch_eta(session: aiohttp.ClientSession, system_id: int, route_id: int, stop_id: int, sem: asyncio.Semaphore):
    url = f"{BASE_URL}/mapGetData.php?eta=3&stopIds={stop_id}&routeId={route_id}"
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "User-Agent": "python-requests/2.0 (aiohttp-mimic)"
    }
    async with sem:
        try:
            async with session.get(url, timeout=10, headers=headers) as resp:
                txt = await resp.text()
                if resp.status != 200:
                    if VERBOSE:
                        print(f"  > Async ETA non-200 stop {stop_id}: {resp.status} snippet={txt[:200]!r}")
                    return stop_id, None
                # robust JSON parse: prefer resp.json(), otherwise json.loads(txt)
                try:
                    data = await resp.json()
                except Exception:
                    try:
                        data = json.loads(txt)
                    except Exception:
                        # server returned HTML or invalid JSON
                        print(f"  > Async ETA invalid JSON for stop {stop_id}; content-type={resp.headers.get('content-type')} snippet={txt[:200]!r}")
                        return stop_id, None
                if isinstance(data, dict) and data.get("error"):
                    print(f"  > Async ETA API error for stop {stop_id}: {data.get('error')}")
                    return stop_id, None
                return stop_id, data
        except Exception as ex:
            print(f"  > Exception fetching async ETA for stop {stop_id}: {ex}")
            return stop_id, None
        
async def fetch_etas_for_stops(system_id: int, route_id: int, stop_ids: list[int], concurrency: int = 10):
    """
    Concurrently fetch ETA payloads for the given stop_ids.
    Returns a dict mapping stop_id (int) -> eta_data (dict or None).
    """
    sem = asyncio.Semaphore(concurrency)
    timeout = aiohttp.ClientTimeout(total=15)
    results = {}

    # Use certifi CA bundle to avoid SSLCertVerificationError
    ssl_ctx = ssl.create_default_context(cafile=certifi.where())
    connector = aiohttp.TCPConnector(ssl=ssl_ctx)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tasks = [_fetch_eta(session, system_id, route_id, sid, sem) for sid in stop_ids]
        for fut in asyncio.as_completed(tasks):
            sid, data = await fut
            results[str(sid)] = data
    # debug: list failed stops
    failed = [k for k,v in results.items() if v is None]
    if failed:
        print(f"  > Async fetch: {len(failed)} stops failed or returned no data: {failed[:10]}")
    return results

def get_stops_for_route(conn, route_myid):
    """
    Gets all unique stop_ids for a given route from the DB.
    """
    c = conn.cursor()
    try:
        # Get all unique stop_ids for this route
        c.execute(
            """
            SELECT DISTINCT stop_id 
            FROM Route_Stops 
            WHERE route_id_from_stop = ?
            """,
            (route_myid,) # Pass the integer ID
        )
        return c.fetchall() # Returns a list of tuples like [(10052,), (10039,), ...]
    except Exception as e:
        print(f"  > Error getting stops from DB: {e}", file=sys.stderr)
        return []

# --- MODIFIED: Renamed and returns ALL ETAs + paxload ---
async def get_all_etas_and_paxload(conn, bus: Vehicle, system_id: int):
    """
    Finds ALL ETAs for a single bus by calling the ETA API
    for every stop on its route.
    """
    bus_id = bus.id
    route_myid = bus.routeId
    
    if not route_myid:
        print(f"  > Bus {bus_id} has no routeId. Skipping.")
        return [], None # Return empty list, no paxload
    
    stops_on_route = get_stops_for_route(conn, route_myid)
    if not stops_on_route:
        print(f"  > Could not find stop list for route {route_myid} in DB.")
        return [], None
    if VERBOSE:
        print(f"  > Found {len(stops_on_route)} unique stops for route {route_myid}. Fetching ETAs...")
    
    # We will store (stop_id, eta_seconds, pax_load_string)
    eta_results = [] 

    # Build list of stop ids (ints) and fetch their ETA payloads concurrently
    stop_ids = [int(sid_tuple[0]) for sid_tuple in stops_on_route]
    try:
        start_time = time.time()
        eta_map = await fetch_etas_for_stops(system_id=system_id, route_id=route_myid, stop_ids=stop_ids, concurrency=len(stop_ids))
        if VERBOSE:
            print(f"  > Fetched all ETA payloads in {time.time() - start_time:.2f} seconds.")
    except Exception as e:
        print(f"  > Async ETA fetch failed: {e}")
        eta_map = {}

    for (stop_id,) in stops_on_route:
        
        eta_data = eta_map.get(str(stop_id))
        if eta_data is None:
            # fallback: single synchronous request
            eta_data = get_eta_data(
                system_id=system_id,
                route_id=route_myid,
                stop_id=stop_id
            )
        
        if not eta_data:
            eta_results.append((stop_id, 9999, None))
            continue

        #print(eta_data) 
        
        etas_for_stop = eta_data.get('ETAs', {}).get(str(stop_id), [])
        
        bus_eta_obj = None
        for e in etas_for_stop:
            # 1. Get the busId from either location
            eta_bus_id = e.get('busId') # Format 1 (Shallow)
            if eta_bus_id is None:
                eta_bus_id = e.get('solidEta', {}).get('busId') # Format 2 (Deep)
            
            # 2. Check if it's the bus we're looking for
            if str(eta_bus_id) == str(bus_id):
                bus_eta_obj = e
                break # Found our bus
        
        if bus_eta_obj:
            try:
                # If the API explicitly marks this ETA as '--' treat it as missing/invalid.
                raw_eta_label = bus_eta_obj.get('eta')
                if raw_eta_label is None:
                    raw_eta_label = (bus_eta_obj.get('solidEta') or {}).get('eta')

                if raw_eta_label is not None and str(raw_eta_label).strip() == '--':
                    # mark as invalid so we fall back to 9999
                    eta_seconds = None
                else:
                    # 3. Get the ETA in seconds from either location (only secondsSpent per policy)
                    eta_seconds = bus_eta_obj.get('secondsSpent')
                    if eta_seconds is None:
                        eta_seconds = (bus_eta_obj.get('solidEta') or {}).get('duration', None)

                    # attempt safe int conversion
                    try:
                        eta_seconds = int(eta_seconds) if eta_seconds is not None else None
                    except (ValueError, TypeError):
                        eta_seconds = None

                # 4. Get the PaxLoad string from either location
                pax_load_str = bus_eta_obj.get('paxLoadS')
                if pax_load_str is None:
                    pax_load_str = (bus_eta_obj.get('solidEta') or {}).get('paxLoadS')

                if eta_seconds is not None and eta_seconds >= 0:
                    eta_results.append((stop_id, eta_seconds, pax_load_str))
                else:
                    eta_results.append((stop_id, 9999, None))
            except (ValueError, TypeError):
                eta_results.append((stop_id, 9999, None))

    if not eta_results:
        print("  > No ETA results found.")
        return [], None
        
    # Sort by eta_seconds (index 1)
    sorted_etas = sorted(eta_results, key=lambda x: x[1])

    if VERBOSE:
        print(f"  > Sorted ETAs: {sorted_etas}")

    # Find the first valid pax_load value in the sorted list
    parsed_pax_load = None
    for (_sid, _eta, pax_str) in sorted_etas:
        if pax_str is None:
            continue
        p = parse_pax_load(pax_str)
        if p is not None:
            parsed_pax_load = p
            break
            
    # Return the full list and the best paxload value found
    return sorted_etas, parsed_pax_load

# --- MODIFIED: This function now logs to BOTH tables ---
def log_bus_data(conn, bus: Vehicle, all_etas_list: list, arrived_id: int):
    """
    Inserts one row into Bus_Logs and multiple rows into ETA_Logs.
    """
    try:
        c = conn.cursor()

        # --- Step 1: Insert the main Bus Log ---
        c.execute(
            """
            INSERT OR IGNORE INTO Buses (bus_id, name, type) 
            VALUES (?, ?, ?)
            """,
            (bus.id, bus.name, bus.type)
        )

        c.execute(
            """
            INSERT INTO Bus_Logs (
                timestamp, bus_id, route_myid, 
                latitude, longitude, pax_load, arrived_stop_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(time.time()), bus.id, bus.routeId,
                bus.latitude, bus.longitude, bus.paxLoad,
                arrived_id
            )
        )
        
        # Get the new primary key we just created
        new_log_id = c.lastrowid
        if not new_log_id:
            print("  > Error: Could not get new log_id.")
            conn.rollback()
            return

        # --- Step 2: Insert all the ETAs ---
        etas_to_insert = []
        # We loop through the list to get the stop_id, eta, and index (for sort_order)
        for i, (stop_id, eta_seconds, pax_str) in enumerate(all_etas_list):
            if eta_seconds != 9999: # Only log valid ETAs
                etas_to_insert.append(
                    (new_log_id, stop_id, eta_seconds, i) # (log_id, stop_id, eta, sort_order)
                )

        if etas_to_insert:
            c.executemany(
                """
                INSERT INTO ETA_Logs (log_id, stop_id, eta_seconds, sort_order)
                VALUES (?, ?, ?, ?)
                """,
                etas_to_insert
            )
        
        conn.commit()
        if VERBOSE:
            print(f"  > Successfully logged data for bus {bus.id} (Log ID: {new_log_id}) with {len(etas_to_insert)} ETAs.")
    except Exception as e:
        print(f"  > Error logging bus data to DB: {e}", file=sys.stderr)
        conn.rollback() # Rollback on error


def check_log_data(conn):
    """ Prints the last row from the Bus_Logs table to verify """
    if VERBOSE:
        print("\n--- Verifying Data in Bus_Logs ---")
    try:
        c = conn.cursor()
        c.execute("SELECT * FROM Bus_Logs ORDER BY timestamp DESC LIMIT 1")
        row = c.fetchone()
        if row:
            if VERBOSE:
                print("Most recent log entry (Bus_Logs):")
                print(row)
            
            # Also show the corresponding ETAs
            c.execute("SELECT * FROM ETA_Logs WHERE log_id = ? ORDER BY sort_order ASC", (row[0],))
            eta_rows = c.fetchall()
            if VERBOSE:
                print(f"  > Found {len(eta_rows)} corresponding ETAs in ETA_Logs.")
                for eta in eta_rows[:2]: # Print first 2
                    print(f"    - {eta}")

        else:
            print("Bus_Logs table is empty.")
    except Exception as e:
        print(f"Error checking log data: {e}", file=sys.stderr)


# ==================================================================
# --- 3. MAIN EXECUTION (Looping) ---
# ==================================================================

# --- MODIFIED: This is the main async wrapper ---
async def main():
    
    # 1. Find the Rutgers System (Run Once)
    print("--- 1. Finding Rutgers University System ID ---")
    all_systems = getSystems()
    rutgers_system = None
    for system in all_systems:
        if "rutgers" in system.name.lower():
            rutgers_system = system
            break
    
    if not rutgers_system:
        print("Error: Could not find 'Rutgers' system. Exiting.", file=sys.stderr)
        sys.exit(1)
        
    print(f"Found system: {rutgers_system.name} (ID: {rutgers_system.id})\n")
    
    # 2. Connect to DB and create table (Run Once)
    conn = create_connection(DB_FILE)
    if conn is None:
        sys.exit(1)
    
    # --- MODIFIED: Create both tables ---
    create_bus_log_table(conn)
    create_eta_log_table(conn) # <-- NEW
    
    SECONDS_PER_CYCLE = 10 # How often to log data (in seconds)
    # --- 3. Start the infinite loop ---
    total_time_per_cycle = 0
    average_time_per_cycle = 0
    try:
        loop_count = 1
        while True:
            timer = time.time()
            if VERBOSE:
                print(f"\n=================================================")
                print(f"--- STARTING LOGGING LOOP #{loop_count} ---")
                print(f"=================================================")
            
                print("\n--- Fetching Active Buses ---")
            active_buses = rutgers_system.getVehicles()
            if not active_buses:
                print("No active buses found. Waiting for next cycle.")
                await asyncio.sleep(60) # Async sleep
                loop_count += 1
                continue # Skip the rest of the loop
            if VERBOSE:
                print(f"Found {len(active_buses)} active buses.")
            # 5. --- PROCESS ALL ACTIVE BUSES --- (inside loop)
            if VERBOSE:
                print(f"--- Processing all {len(active_buses)} buses ---")
            
            for bus_to_log in active_buses:
                start_time = time.time()
                # --- THIS IS YOUR NEW CHECK ---
                if bus_to_log.outOfService == 1:
                    print(f"\n--- Skipping Bus ID: {bus_to_log.id} (Name: {bus_to_log.name}) - Out of Service ---")
                    continue # Skip to the next bus
                # --- END OF NEW CHECK ---

                if VERBOSE:
                    print(f"\n--- Processing Bus ID: {bus_to_log.id} (Name: {bus_to_log.name}) ---")
                try:
                    # --- MODIFIED: Call new function ---
                    sorted_etas, parsed_paxload = await get_all_etas_and_paxload(
                        conn, 
                        bus_to_log, 
                        rutgers_system.id
                    )
                    
                    # Overwrite the bad paxload from getVehicles()
                    bus_to_log.paxLoad = parsed_paxload
                    
                    arrived_id = find_arrived_stop(conn, bus_to_log.latitude, bus_to_log.longitude)
                    
                    if arrived_id is not None:
                        if VERBOSE:
                            print(f"  > STATUS: Bus has arrived at Stop ID: {arrived_id}")
                    
                    # Find the first valid ETA to print a success message
                    first_valid_eta = next((eta for eta in sorted_etas if eta[1] != 9999), None)
                    
                    if first_valid_eta:
                        eta_sec = first_valid_eta[1]
                        if VERBOSE:
                            print(f"  > SUCCESS: Next Stop ID: {first_valid_eta[0]}, ETA: {eta_sec // 60}m {eta_sec % 60}s")
                    else:
                        print("  > Could not determine next stops (API returned no ETA for this bus).")

                    # --- MODIFIED: Log all data ---
                    log_bus_data(conn, bus_to_log, sorted_etas, arrived_id)
                        
                except Exception as e:
                    print(f"\nAn error occurred during processing for bus {bus_to_log.id}: {e}", file=sys.stderr)
                    conn.rollback() # Rollback on error for this bus
                if VERBOSE:
                    print(f"  > Processing time for bus {bus_to_log.id}: {time.time() - start_time:.2f} seconds")
            
            # 6. Verify by checking the DB (inside loop)
            if VERBOSE:
                print("\n--- Loop complete. Verifying last log entry. ---")
            check_log_data(conn)

            # 7. Wait for the next cycle
            loop_count += 1
            sleep_duration = SECONDS_PER_CYCLE - (time.time() - timer)
            if sleep_duration > 0:
                if VERBOSE:
                    print(f"\n--- Waiting {sleep_duration:.2f} seconds for next cycle ---")
                await asyncio.sleep(sleep_duration)
            current_cycle_time = time.time() - timer
            total_time_per_cycle += current_cycle_time
            average_time_per_cycle = total_time_per_cycle / (loop_count - 1)
            print(f"--- Cycle #{loop_count - 1} complete in {current_cycle_time:.2f} seconds. Average time: {average_time_per_cycle:.2f} seconds. ---")
            

    except KeyboardInterrupt:
        print("\n\nKeyboardInterrupt (Ctrl+C) detected. Stopping script.")
    
    except Exception as e:
        print(f"\nA critical error occurred: {e}", file=sys.stderr)

    finally:
        # 8. Close connection (Run Once at end)
        if conn:
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"Critical error in main: {e}")