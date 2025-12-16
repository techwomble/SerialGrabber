import os
import re
import pandas as pd
import sys
import warnings
from dateutil import parser

# Suppress timezone warnings
warnings.filterwarnings("ignore", category=UserWarning)

def parse_inventory_files(folder_path):
    extracted_data = []

    # --- 1. Regex Patterns ---
    
    # Matches: "HOSTNAME#show inventory" AND "HOSTNAME# show inventory"
    hostname_pattern = re.compile(r'^(.*?)#\s*show inventory', re.MULTILINE)
    
    # Matches: ...#show clock followed by the time on the next line
    clock_pattern = re.compile(r'.*?#\s*show clock\s*\n\s*(.*)', re.MULTILINE)

    # Matches: NAME: "...", DESCR: "..."
    name_descr_pattern = re.compile(r'NAME:\s*"(.*?)",\s*DESCR:\s*"(.*?)"')
    
    # Matches: PID: ..., VID: ..., SN: ...
    pid_sn_pattern = re.compile(r'PID:\s*([^,]*)\s*,\s*VID:.*,\s*SN:\s*(.*)')

    print(f"Scanning files in: {folder_path}...\n")

    if not os.path.exists(folder_path):
        print(f"Error: The folder '{folder_path}' does not exist.")
        return

    files_processed = 0

    # --- 2. Iterate through files ---
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)

        if not os.path.isfile(file_path):
            continue

        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                
                # --- Extract File-Level Info ---
                host_match = hostname_pattern.search(content)
                if host_match:
                    current_hostname = host_match.group(1).strip()
                else:
                    current_hostname = filename

                clock_match = clock_pattern.search(content)
                raw_time_string = clock_match.group(1).strip() if clock_match else None

                current_clock_dt = None
                if raw_time_string:
                    try:
                        dt = parser.parse(raw_time_string, fuzzy=True)
                        current_clock_dt = dt.replace(tzinfo=None)
                    except:
                        current_clock_dt = None

                # --- Extract Inventory Items ---
                lines = content.splitlines()
                current_name = None
                current_descr = None
                
                files_processed += 1

                for line in lines:
                    line = line.strip()

                    nd_match = name_descr_pattern.search(line)
                    if nd_match:
                        current_name = nd_match.group(1)
                        current_descr = nd_match.group(2)
                        continue 

                    if current_name and line.startswith("PID:"):
                        ps_match = pid_sn_pattern.search(line)
                        if ps_match:
                            pid = ps_match.group(1).strip()
                            sn = ps_match.group(2).strip()

                            extracted_data.append({
                                'Hostname': current_hostname,
                                'Capture Time': current_clock_dt, 
                                'Original Time String': raw_time_string,
                                'SN': sn,
                                'PID': pid,
                                'NAME': current_name,
                                'DESCR': current_descr,
                                'Source File': filename
                            })
                        
                        current_name = None 
                        current_descr = None

        except Exception as e:
            print(f"Error reading {filename}: {e}")

    # --- 3. Process Data ---
    if extracted_data:
        df = pd.DataFrame(extracted_data)
        
        # 1. Create a temporary column for Description Length
        df['Descr_Len'] = df['DESCR'].astype(str).map(len)

        # 2. Sort by Description Length (Longest first) THEN by Time (Newest first)
        df.sort_values(by=['Descr_Len', 'Capture Time'], ascending=[False, False], na_position='last', inplace=True)
        
        initial_count = len(df)
        
        # --- CRITICAL CHANGE HERE ---
        # Only look at Hostname and SN. Ignore Name/PID differences.
        # Because we sorted by length above, this will keep the "Chassis" row
        # and delete the "Supervisor" row if they share an SN.
        df.drop_duplicates(subset=['Hostname', 'SN'], keep='first', inplace=True)
        # ----------------------------
        
        final_count = len(df)
        duplicates_removed = initial_count - final_count

        # 4. Cleanup
        df = df[['Hostname', 'Original Time String', 'SN', 'PID', 'NAME', 'DESCR', 'Source File']]
        df.rename(columns={'Original Time String': 'Capture Time'}, inplace=True)

        output_file = os.path.join(folder_path, 'Inventory_Report_Final_Clean.xlsx')
        df.to_excel(output_file, index=False)
        
        print("-" * 30)
        print(f"Success! Scanned {files_processed} files.")
        print(f"Total entries found: {initial_count}")
        print(f"Duplicates removed:  {duplicates_removed}")
        print(f"Unique entries saved: {final_count}")
        print(f"File saved at: {output_file}")
    else:
        print("No matching data found.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        target_folder = sys.argv[1]
    else:
        target_folder = input("Enter the full path to your logs folder: ").strip()
        target_folder = target_folder.replace('"', '').replace("'", "")

    parse_inventory_files(target_folder)
