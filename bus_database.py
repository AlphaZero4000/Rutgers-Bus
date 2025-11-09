import sqlite3
import sys
import passiogo
# --- Database Setup ---

DB_FILE = "rutgers_buses.db"

def create_connection(db_file):
    """ Create a database connection to a SQLite database """
    conn = None
    try:
        # This will create or open the file
        conn = sqlite3.connect(db_file)
        
        # This line is important to enforce foreign key constraints
        conn.execute("PRAGMA foreign_keys = ON")
        
        print(f"Connected to SQLite database: {db_file}")
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}", file=sys.stderr)
        return None

def create_tables(conn):
    """ Create the normalized tables for our bus data """
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
            
            -- This is the foreign key we are testing
            FOREIGN KEY (route_id_from_stop) REFERENCES Routes (route_myid) 
        );
        """
    ]
    
    try:
        c = conn.cursor()
        for statement in sql_statements:
            c.execute(statement)
        conn.commit()
        print("All metadata tables created successfully.")
    except Exception as e:
        print(f"Error creating tables: {e}", file=sys.stderr)

# --- Data Population Functions ---

def populate_system(conn, system):
    """ Populates the Systems table """
    print(f"Populating System: {system.name}")
    c = conn.cursor()
    c.execute(
        "INSERT OR IGNORE INTO Systems (system_id, name, agency_name, homepage) VALUES (?, ?, ?, ?)",
        (system.id, system.name, system.goAgencyName, system.homepage)
    )
    conn.commit()

def populate_routes(conn, system):
    """ Populates the Routes table """
    print("Populating Routes...")
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
            print(f"  > Warning: Skipping route with invalid ID. Data: {route.myid}, {route.id}")
    conn.commit()
    print(f"Populated {len(routes)} routes.")

def populate_stops_and_junction(conn, system):
    """ Populates the Stops table AND the Route_Stops junction table """
    print("Populating Stops and Route_Stops junction...")
    c = conn.cursor()
    stops = system.getStops()
    route_stop_count = 0
    fk_errors = 0
    
    for stop in stops:
        try:
            # 1. Insert into Stops table
            c.execute(
                "INSERT OR IGNORE INTO Stops (stop_id, system_id, name, latitude, longitude, radius) VALUES (?, ?, ?, ?, ?, ?)",
                # Cast stop_id to INTEGER
                (int(stop.id), int(stop.systemId), stop.name, 
                 stop.latitude, stop.longitude, stop.radius)
            )
            #print(int(stop.id), int(stop.systemId), stop.name, 
            #     stop.latitude, stop.longitude, stop.radius)
            
            # 2. Insert into Route_Stops junction table
            if stop.routesAndPositions:
                for route_id_str, positions_list in stop.routesAndPositions.items():
                    for pos in positions_list:
                        try:
                            # Attempt to insert into the junction table
                            c.execute(
                                "INSERT OR IGNORE INTO Route_Stops (route_id_from_stop, stop_id, position_on_route) VALUES (?, ?, ?)",
                                # Cast all IDs to INTEGER
                                (int(route_id_str), int(stop.id), int(pos))
                            )
                            route_stop_count += 1
                        
                        # --- THIS IS THE ERROR CATCHING ---
                        except sqlite3.IntegrityError as e:
                            if "FOREIGN KEY constraint failed" in str(e):
                                fk_errors += 1
                                # This error is expected, we'll just log the first one
                                if fk_errors == 1:
                                    print(f"  > REPORT: Caught expected FOREIGN KEY error.")
                                    print(f"    > Details: Route ID '{route_id_str}' from stop '{stop.id}' does not exist in Routes table.")
                            else:
                                # Report any other, unexpected integrity errors
                                print(f"  > Warning: Skipping Route_Stop insert due to IntegrityError: {e}")
                        except (ValueError, TypeError):
                             print(f"  > Warning: Skipping Route_Stop with invalid ID. Data: {route_id_str}, {stop.id}")
        
        except (ValueError, TypeError):
            print(f"  > Warning: Skipping stop with invalid ID. Data: {stop.id}")
    
    conn.commit()
    print(f"Populated {len(stops)} stops.")
    print(f"Attempted to populate {route_stop_count} route-stop associations.")
    if fk_errors > 0:
        print(f"*** Reported {fk_errors} total FOREIGN KEY constraint failures (as expected). ***")

def populate_buses(conn, system):
    """ Populates the Buses metadata table """
    print("Populating Buses (metadata)...")
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
             print(f"  > Warning: Skipping bus with invalid ID. Data: {bus.id}")
    conn.commit()
    print(f"Populated metadata for {len(vehicles)} currently active buses.")

# --- Main Execution ---

if __name__ == "__main__":
    
    # 1. Find the Rutgers System
    print("--- 1. Finding Rutgers University System ID ---")
    all_systems = passiogo.getSystems()
    rutgers_system = None
    for system in all_systems:
        if "rutgers" in system.name.lower():
            rutgers_system = system
            break
    
    if not rutgers_system:
        print("Error: Could not find 'Rutgers' system. Exiting.", file=sys.stderr)
        sys.exit(1)
        
    print(f"Found system: {rutgers_system.name} (ID: {rutgers_system.id})\n")
    
    # 2. Connect to and create DB tables
    conn = create_connection(DB_FILE)
    if conn is None:
        sys.exit(1)
        
    create_tables(conn)
    
    # 3. Populate all metadata tables
    print("\n--- Populating All Metadata Tables ---")
    try:
        #populate_system(conn, rutgers_system)
        #populate_routes(conn, rutgers_system)
        # This function will now report errors instead of crashing
        populate_stops_and_junction(conn, rutgers_system) 
        #populate_buses(conn, rutgers_system)
        
        print("\n--- Database Population Complete ---")
        print(f"You can now inspect the file: {DB_FILE}")
        
    except Exception as e:
        print(f"\nAn error occurred during population: {e}", file=sys.stderr)
        conn.rollback() # Roll back any changes if an error occurs
    finally:
        conn.close()
        print("Database connection closed.")