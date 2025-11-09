import passiogo
import time
import os
import csv  # Import the CSV library

# The system ID for Rutgers University
RUTGERS_SYSTEM_ID = 1268

# --- CSV Configuration ---
# This is the file that will be created in the same folder
CSV_FILENAME = 'rutgers_bus_data.csv'

# These are the column headers for our CSV
# We add 'timestamp' and exclude the 'system' object
FIELDNAMES = [
    'timestamp', 'id', 'name', 'type', 'calculatedCourse', 
    'routeId', 'routeName', 'color', 'created', 'latitude', 
    'longitude', 'speed', 'paxLoad', 'outOfService', 'more', 'tripId'
]
# --- End of CSV Configuration ---

def main():
    print(f"Connecting to Rutgers system (ID: {RUTGERS_SYSTEM_ID})...")
    
    try:
        # Get the transportation system object
        rutgers_system = passiogo.getSystemFromID(RUTGERS_SYSTEM_ID)
        print("Connection successful! Starting data logging...")
        
        # Check if the file already exists before we start the loop
        # This tells us if we need to write the headers
        file_exists = os.path.exists(CSV_FILENAME)
        
        if file_exists:
            print(f"File '{CSV_FILENAME}' found. Appending to existing data.")
        else:
            print(f"File '{CSV_FILENAME}' not found. Will create new file with headers.")
        
        time.sleep(1)

        # This loop will run forever until you press Ctrl+C
        while True:
            try:
                # 1. Get the current time for this batch of data
                capture_time = time.strftime('%Y-%m-%d %H:%M:%S')

                # 2. Get a list of all active vehicle objects
                active_buses = rutgers_system.getVehicles()

                if not active_buses:
                    print(f"{capture_time} - No active buses found.")
                    time.sleep(1)
                    continue # Skip to the next loop iteration

                # 3. Open the CSV file in 'append' mode
                with open(CSV_FILENAME, 'a', newline='', encoding='utf-8') as f:
                    
                    # Use DictWriter to easily write our bus dictionaries
                    # 'extrasaction='ignore'' will safely ignore any fields
                    # in our dictionary that aren't in FIELDNAMES (like 'system')
                    writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction='ignore')
                    
                    # 4. Write headers ONLY if the file didn't exist
                    if not file_exists:
                        writer.writeheader()
                        file_exists = True # Set flag so we don't write headers again
                    
                    # 5. Loop through buses and write data to the file
                    buses_logged = 0
                    for bus in active_buses:
                        # Get the bus data as a dictionary
                        row_data = bus.__dict__
                        
                        # Add our custom timestamp to the dictionary
                        row_data['timestamp'] = capture_time
                        
                        # Write the row to the CSV
                        writer.writerow(row_data)
                        buses_logged += 1
                
                # 6. Print a status message to the terminal
                print(f"{capture_time} - Logged {buses_logged} bus locations to CSV.")
            
            except Exception as e:
                # Catch errors (like a network drop) without crashing
                print(f"An error occurred while fetching vehicles: {e}")

            # Wait for 1 second before fetching again
            time.sleep(1)

    except KeyboardInterrupt:
        # This triggers when you press Ctrl+C
        print("\nScript stopped by user. Data saved in '{CSV_FILENAME}'.")
    except Exception as e:
        print(f"A critical error occurred during setup: {e}")

if __name__ == "__main__":
    main()