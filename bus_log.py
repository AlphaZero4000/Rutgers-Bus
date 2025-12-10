"""
NOTE: Not all of this script is my code. Some of it uses the unofficial PassioGo API wrapper on github which can be found 
here https://github.com/athuler/PassioGo
"""
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
import passiogo as pg
from passiogo import Vehicle

PASSIO_GO_URL = "https://passiogo.com"
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
        print(f"API error in Response: {response_json}")
        return None
    
    return response_json

#This is a function I found online to find distance between two lat and lognitude points using haversine formula
def get_distance(lat1, lon1, lat2, lon2):
    EARTH_RADIUS_FEET = 20902000 
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = EARTH_RADIUS_FEET * c
    return distance

def find_arrived_stop(conn, bus_lat, bus_lon):
    if bus_lat is None or bus_lon is None:
        return None
    try:
        bus_lat_float = float(bus_lat)
        bus_lon_float = float(bus_lon)
        c = conn.cursor()
        c.execute("SELECT stop_id, latitude, longitude, radius FROM Stops")
        all_stops = c.fetchall()
        for (stop_id, stop_lat, stop_lon, radius) in all_stops:
            if stop_lat is None or stop_lon is None or radius is None:
                continue
            distance = get_distance(bus_lat_float, bus_lon_float, stop_lat, stop_lon)
            if distance <= radius:
                return stop_id 
        return None
    except Exception as e:
        print(f"Error in find arrived stop: {e}")
        return None

DB_FILE = "rutgers_buses.db"

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = ON")
        print(f"We connected to: {db_file}")
        return conn
    except Exception as e:
        print(f"Error connecting: {e}", file=sys.stderr)
        return None

def getVehicles(self) -> list["Vehicle"]:
        url = PASSIO_GO_URL + "/mapGetData.php?getBuses=2"
        body = {"s0": str(self.id), "sA": 1}
        vehicles = sendApiRequest(url, body)
        
        if vehicles is None or "buses" not in vehicles:
            return []
        
        allVehicles = []
        for vehicleId, vehicle_data in vehicles["buses"].items():
            if vehicleId == '-1' or not vehicle_data:
                continue
            
            vehicle = vehicle_data[0]
            vehicle['id'] = vehicle.pop('busId', None)
            vehicle['name'] = vehicle.pop('busName', None)
            vehicle['type'] = vehicle.pop('busType', None)
            vehicle['routeName'] = vehicle.pop('route', None)
            vehicle['paxLoad'] = vehicle.pop('paxLoad100', None)
            allVehicles.append(Vehicle(system=self, **vehicle))
            
        return allVehicles
   
#creating the Bus_Logs
def create_bus_log_table(conn):
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
        print(f"Error creating Bus_Logs table: {e}")

#Creating the eta tables that we will use to store an eta per stop inside eahc bus log entry
def create_eta_log_table(conn):
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

#String parsing pax load
def parse_pax_load(pax_load_str):
    if pax_load_str is None:
        return None
    try:
        return float(pax_load_str.replace('%', ''))
    except (ValueError, TypeError):
        return None

# Function to get ETA data
def get_eta_data(system_id: int, route_id: int, stop_id: int):
    url = (
        f"{PASSIO_GO_URL}/mapGetData.php"
        f"?eta=3"
        f"&stopIds={stop_id}"
        f"&routeId={route_id}"
    )
    
    try:
        response = requests.get(url)
        response.raise_for_status() 
        
        eta_data = response.json()
        
        if "error" in eta_data and eta_data["error"] != "":
            print(f"  > API Error in Response: {eta_data}")
            return None
            
        return eta_data
    
    except Exception as e:
        print(f"  > Error getting ETA data: {e}")
        return None
    
async def _fetch_eta(session: aiohttp.ClientSession, system_id: int, route_id: int, stop_id: int, sem: asyncio.Semaphore):
    url = f"{PASSIO_GO_URL}/mapGetData.php?eta=3&stopIds={stop_id}&routeId={route_id}"
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
                try:
                    data = await resp.json()
                except Exception:
                    try:
                        data = json.loads(txt)
                    except Exception:
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
    sem = asyncio.Semaphore(concurrency)
    timeout = aiohttp.ClientTimeout(total=15)
    results = {}
    ssl_ctx = ssl.create_default_context(cafile=certifi.where())
    connector = aiohttp.TCPConnector(ssl=ssl_ctx)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tasks = [_fetch_eta(session, system_id, route_id, sid, sem) for sid in stop_ids]
        for fut in asyncio.as_completed(tasks):
            sid, data = await fut
            results[str(sid)] = data
    failed = [k for k,v in results.items() if v is None]
    if failed:
        print(f"  > Async fetch: {len(failed)} stops failed or returned no data: {failed[:10]}")
    return results

def get_stops_for_route(conn, route_myid):
    c = conn.cursor()
    try:
        c.execute(
            """
            SELECT DISTINCT stop_id 
            FROM Route_Stops 
            WHERE route_id_from_stop = ?
            """,
            (route_myid,)
        )
        return c.fetchall() 
    except Exception as e:
        print(f"Error getting stops: {e}", file=sys.stderr)
        return []

async def get_all_etas_and_paxload(conn, bus: Vehicle, system_id: int):
    bus_id = bus.id
    route_myid = bus.routeId
    
    if not route_myid:
        print(f"  > Bus {bus_id} has no routeId. Skipping.")
        return [], None 
    
    stops_on_route = get_stops_for_route(conn, route_myid)
    if not stops_on_route:
        print(f"Couldn't find stop list for route {route_myid} in DB.")
        return [], None
    
    eta_results = [] 

    stop_ids = [int(sid_tuple[0]) for sid_tuple in stops_on_route]
    try:
        start_time = time.time()
        eta_map = await fetch_etas_for_stops(system_id=system_id, route_id=route_myid, stop_ids=stop_ids, concurrency=len(stop_ids))
        if VERBOSE:
            print(f"Fetched all ETA payloads in {time.time() - start_time:.2f} seconds.")
    except Exception as e:
        print(f"Async ETA fetch failed: {e}")
        eta_map = {}

    for (stop_id,) in stops_on_route:
        
        eta_data = eta_map.get(str(stop_id))
        if eta_data is None:
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
            eta_bus_id = e.get('busId') 
            if eta_bus_id is None:
                eta_bus_id = e.get('solidEta', {}).get('busId') 
            
            if str(eta_bus_id) == str(bus_id):
                bus_eta_obj = e
                break 
        
        if bus_eta_obj:
            try:
                raw_eta_label = bus_eta_obj.get('eta')
                if raw_eta_label is None:
                    raw_eta_label = (bus_eta_obj.get('solidEta') or {}).get('eta')

                if raw_eta_label is not None and str(raw_eta_label).strip() == '--':
                    eta_seconds = None
                else:
                    eta_seconds = bus_eta_obj.get('secondsSpent')
                    if eta_seconds is None:
                        eta_seconds = (bus_eta_obj.get('solidEta') or {}).get('duration', None)
                    try:
                        eta_seconds = int(eta_seconds) if eta_seconds is not None else None
                    except (ValueError, TypeError):
                        eta_seconds = None
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
        print("No ETA results")
        return [], None
        
    sorted_etas = sorted(eta_results, key=lambda x: x[1])

    if VERBOSE:
        print(f"Sorted ETAs: {sorted_etas}")

    parsed_pax_load = None
    for (_sid, _eta, pax_str) in sorted_etas:
        if pax_str is None:
            continue
        p = parse_pax_load(pax_str)
        if p is not None:
            parsed_pax_load = p
            break
            
    return sorted_etas, parsed_pax_load

def log_bus_data(conn, bus: Vehicle, all_etas_list: list, arrived_id: int):
    """
    Inserts one row into Bus_Logs and multiple rows into ETA_Logs.
    """
    try:
        c = conn.cursor()

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
        
        new_log_id = c.lastrowid
        if not new_log_id:
            print("  > Error: Could not get new log_id.")
            conn.rollback()
            return

        etas_to_insert = []
        for i, (stop_id, eta_seconds, pax_str) in enumerate(all_etas_list):
            if eta_seconds != 9999: 
                etas_to_insert.append(
                    (new_log_id, stop_id, eta_seconds, i) 
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
            print(f"Successfully logged data for bus {bus.id} (Log ID: {new_log_id}) with {len(etas_to_insert)} ETAs")
    except Exception as e:
        print(f"Error logging bus data to DB: {e}")
        conn.rollback() 
        
async def main():
    print("--- 1. Finding Rutgers University System ID ---")
    all_systems = pg.getSystems()
    rutgers_system = None
    for system in all_systems:
        if "rutgers" in system.name.lower():
            rutgers_system = system
            break
    
    if not rutgers_system:
        print("Error: Could not find  system")
        sys.exit(1)
        
    print(f"Found system: {rutgers_system.name} (ID: {rutgers_system.id})\n")
    conn = create_connection(DB_FILE)
    if conn is None:
        sys.exit(1)
    
    create_bus_log_table(conn)
    create_eta_log_table(conn) 
    
    SECONDS_PER_CYCLE = 10
    total_time_per_cycle = 0
    average_time_per_cycle = 0
    try:
        loop_count = 1
        while True:
            timer = time.time()
            active_buses = rutgers_system.getVehicles()
            if not active_buses:
                print("No active buses found. Waiting for next cycle.")
                await asyncio.sleep(60) 
                loop_count += 1
                continue 
            if VERBOSE:
                print(f"Found {len(active_buses)} active buse")
            if VERBOSE:
                print(f"Processing all {len(active_buses)} buses")
            
            for bus_to_log in active_buses:
                start_time = time.time()
                if bus_to_log.outOfService == 1:
                    continue

                if VERBOSE:
                    print(f"\nProcessing Bus ID: {bus_to_log.id} (Name: {bus_to_log.name})")
                try:
                    sorted_etas, parsed_paxload = await get_all_etas_and_paxload(
                        conn, 
                        bus_to_log, 
                        rutgers_system.id
                    )
                    
                    bus_to_log.paxLoad = parsed_paxload
                    
                    arrived_id = find_arrived_stop(conn, bus_to_log.latitude, bus_to_log.longitude)
                    
                    if arrived_id is not None:
                        if VERBOSE:
                            print(f"STATUS: Bus has arrived at Stop ID: {arrived_id}")
                    
                    first_valid_eta = next((eta for eta in sorted_etas if eta[1] != 9999), None)
                    
                    if first_valid_eta:
                        eta_sec = first_valid_eta[1]
                        if VERBOSE:
                            print(f" SUCCESS: Next Stop ID: {first_valid_eta[0]}, ETA: {eta_sec // 60}m {eta_sec % 60}s")
                    else:
                        print(" Could not determine next stops (API returned no ETA for this bus).")

                    log_bus_data(conn, bus_to_log, sorted_etas, arrived_id)
                        
                except Exception as e:
                    print(f"\nAn error occurred during processing for bus {bus_to_log.id}: {e}", file=sys.stderr)
                    conn.rollback()
                if VERBOSE:
                    print(f"Processing time for bus {bus_to_log.id}: {time.time() - start_time:.2f} seconds")
            
            if VERBOSE:
                print("\nLoop complete. Verifying last log entry.")

            loop_count += 1
            sleep_duration = SECONDS_PER_CYCLE - (time.time() - timer)
            if sleep_duration > 0:
                if VERBOSE:
                    print(f"\nWaiting {sleep_duration:.2f} seconds for next cycle")
                await asyncio.sleep(sleep_duration)
            current_cycle_time = time.time() - timer
            total_time_per_cycle += current_cycle_time
            average_time_per_cycle = total_time_per_cycle / (loop_count - 1)
            print(f"Cycle #{loop_count - 1} complete in {current_cycle_time:.2f} seconds. Average time: {average_time_per_cycle:.2f} seconds.")
            

    except KeyboardInterrupt:
        print("\n\nWe are stopping the script")
    
    except Exception as e:
        print(f"\nA error occurred: {e}")

    finally:
        if conn:
            conn.close()
            print("Closing database connection")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"Error in main: {e}")