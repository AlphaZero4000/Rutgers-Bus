import sqlite3
import sys
import passiogo

DB_FILE = "rutgers_buses.db"

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        
        conn.execute("PRAGMA foreign_keys = ON")
        
        print(f"Connected to SQLite database: {db_file}")
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}", file=sys.stderr)
        return None

def create_tables(conn):
    sql_statements = [
        """
        CREATE TABLE IF NOT EXISTS Systems (
            system_id        INTEGER PRIMARY KEY,
            name             TEXT,
            agency_name      TEXT,
            homepage         TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Routes (
            route_myid       INTEGER PRIMARY KEY, 
            route_id         INTEGER,
            system_id        INTEGER,
            name             TEXT,
            short_name       TEXT,
            color            TEXT,
            FOREIGN KEY (system_id) REFERENCES Systems (system_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Buses (
            bus_id           INTEGER PRIMARY KEY,
            system_id        INTEGER,
            name             TEXT,
            type             TEXT,
            FOREIGN KEY (system_id) REFERENCES Systems (system_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Stops (
            stop_id          INTEGER PRIMARY KEY,
            system_id        INTEGER,
            name             TEXT,
            latitude         REAL,
            longitude        REAL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Route_Stops (
            route_id_from_stop INTEGER,
            stop_id            INTEGER,
            position_on_route  INTEGER,
            
            PRIMARY KEY (route_id_from_stop, stop_id, position_on_route),
            FOREIGN KEY (stop_id) REFERENCES Stops (stop_id),
            FOREIGN KEY (route_id_from_stop) REFERENCES Routes (route_myid) 
        );
        """
    ]
    
    try:
        c = conn.cursor()
        for statement in sql_statements:
            c.execute(statement)
        conn.commit()
    except Exception as e:
        print(f"Error creating table: {e}")

def insert_system_data(conn, system):
    print(f"Adding System: {system.name}")
    c = conn.cursor()
    c.execute(
        "INSERT OR IGNORE INTO Systems (system_id, name, agency_name, homepage) VALUES (?, ?, ?, ?)",
        (system.id, system.name, system.goAgencyName, system.homepage)
    )
    conn.commit()

def insert_routes_into_db(conn, system):
    c = conn.cursor()
    routes = system.getRoutes()
    for route in routes:
        try:
            c.execute(
                """
                INSERT OR IGNORE INTO Routes (route_myid, route_id, system_id, name, short_name, color)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                # Cast IDs to INTEGER
                (int(route.myid), int(route.id), int(route.systemId), 
                 route.name, route.shortName, route.groupColor)
            )
        except (ValueError, TypeError):
            print(f"Skipping route insert because invalid ID. Data: {route.myid}, {route.id}")
    conn.commit()
    print(f"Added {len(routes)} routes.")

def insert_bus_stops_and_routes(conn, system):
    c = conn.cursor()
    stops = system.getStops()
    route_stop_count = 0
    fk_errors = 0
    
    for stop in stops:
        try:
            c.execute(
                "INSERT OR IGNORE INTO Stops (stop_id, system_id, name, latitude, longitude, radius) VALUES (?, ?, ?, ?, ?, ?)",
                (int(stop.id), int(stop.systemId), stop.name, 
                 stop.latitude, stop.longitude, stop.radius)
            )
            if stop.routesAndPositions:
                for route_id_str, positions_list in stop.routesAndPositions.items():
                    for pos in positions_list:
                        try:
                            c.execute(
                                "INSERT OR IGNORE INTO Route_Stops (route_id_from_stop, stop_id, position_on_route) VALUES (?, ?, ?)",
                                (int(route_id_str), int(stop.id), int(pos))
                            )
                            route_stop_count += 1
                        
                        except sqlite3.IntegrityError as e:
                            if "FOREIGN KEY constraint failed" in str(e):
                                print(f"foreign key error.")
                            else:
                                print(f"Skipping Route_Stop insert due to error: {e}")
                        except (ValueError, TypeError):
                             print(f"Skipping Route_Stop insert due to invalid ID. Data: {route_id_str}, {stop.id}")
        
        except (ValueError, TypeError):
            print(f"Skipping stop insert because invalid ID. Data: {stop.id}")
    
    conn.commit()
    print(f"Added {len(stops)} stops.")


def insert_buses_into_db(conn, system):
    c = conn.cursor()
    vehicles = system.getVehicles()
    for bus in vehicles:
        try:
            c.execute(
                "INSERT OR IGNORE INTO Buses (bus_id, system_id, name, type) VALUES (?, ?, ?, ?)",
                # Cast bus_id to INTEGER
                (int(bus.id), int(system.id), bus.name, bus.type)
            )
        except (ValueError, TypeError):
             print(f"Skipping bus because invalid ID. Data: {bus.id}")
    conn.commit()

if __name__ == "__main__":
    all_systems = passiogo.getSystems()
    rutgers_system = None
    for system in all_systems:
        if "rutgers" in system.name.lower():
            rutgers_system = system
            break
    
    if not rutgers_system:
        print("Error: Could not find system.")
        sys.exit(1)
        
    print(f"Found system: {rutgers_system.name} (ID: {rutgers_system.id})\n")
    
    conn = create_connection(DB_FILE)
    if conn is None:
        sys.exit(1)
        
    create_tables(conn)
    
    try:
        insert_bus_stops_and_routes(conn, rutgers_system) 
        
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()