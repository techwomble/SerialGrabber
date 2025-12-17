import os
import re
import pandas as pd
import sys
import warnings
import zipfile
import tarfile
from dateutil import parser

# Suppress timezone warnings
warnings.filterwarnings("ignore", category=UserWarning)

# --- 1. Regex Patterns ---

# Priority 2: Matches "HOSTNAME#show inventory" (Direct command context)
PROMPT_HOSTNAME_PATTERN = re.compile(r'^(.*?)#\s*show inventory', re.MULTILINE)

# Priority 1: Matches "hostname NAME" (Configuration/Show Tech context)
CONFIG_HOSTNAME_PATTERN = re.compile(r'^hostname\s+([^\s]+)', re.MULTILINE)

# Matches time from "show clock"
CLOCK_PATTERN = re.compile(r'.*?#\s*show clock\s*\n\s*(.*)', re.MULTILINE)

# Fallback: Finds any line containing a standard time format (HH:MM:SS)
# We use this to scan the file if "show clock" is missing.
FALLBACK_TIME_PATTERN = re.compile(r'^.*?\d{2}:\d{2}:\d{2}.*?$', re.MULTILINE)

# Matches Inventory Items
NAME_DESCR_PATTERN = re.compile(r'NAME:\s*"(.*?)",\s*DESCR:\s*"(.*?)"')
PID_SN_PATTERN = re.compile(r'PID:\s*([^,]*)\s*,\s*VID:.*,\s*SN:\s*(.*)')


def get_college_from_path(file_path):
    """
    Parses the path to find the folder immediately after 'AllColleges'.
    """
    try:
        norm_path = os.path.normpath(file_path)
        parts = norm_path.split(os.sep)
        parts_lower = [p.lower() for p in parts]
        
        if 'allcolleges' in parts_lower:
            idx = parts_lower.index('allcolleges')
            if idx + 1 < len(parts):
                return parts[idx + 1]
    except:
        pass
    return "N/A"

def find_best_date(content):
    """
    Tries to find the 'show clock' date first.
    If missing, scans the whole file for the LAST valid date string found.
    Returns: (datetime_object, raw_string)
    """
    # 1. Try explicit 'show clock' (Best Source)
    clock_match = CLOCK_PATTERN.search(content)
    if clock_match:
        raw_str = clock_match.group(1).strip()
        try:
            dt = parser.parse(raw_str, fuzzy=True)
            return dt.replace(tzinfo=None), raw_str
        except:
            pass # Failed to parse, fall through to fallback

    # 2. Fallback: Find ALL lines with a timestamp (HH:MM:SS)
    # We want the LAST one in the file (most recent)
    potential_lines = FALLBACK_TIME_PATTERN.findall(content)
    
    if potential_lines:
        # Iterate backwards (from last line up)
        for line in reversed(potential_lines):
            try:
                # fuzzy=True ignores text around the date (e.g. "Log message at...")
                dt = parser.parse(line, fuzzy=True)
                # Ensure it has a year/valid structure (basic sanity check)
                if dt.year > 1990: 
                    return dt.replace(tzinfo=None), line.strip()
            except:
                continue # Skip lines that look like time but aren't parseable

    return None, None

def process_content(content, full_display_path, filename, college_name, extracted_data):
    try:
        # --- A. Hostname Logic ---
        prompt_match = PROMPT_HOSTNAME_PATTERN.search(content)
        config_match = CONFIG_HOSTNAME_PATTERN.search(content)

        if prompt_match:
            current_hostname = prompt_match.group(1).strip()
            hostname_priority = 2
        elif config_match:
            current_hostname = config_match.group(1).strip()
            hostname_priority = 1
        else:
            current_hostname = filename
            hostname_priority = 0

        # --- B. Date/Time Logic (New Helper Function) ---
        current_clock_dt, raw_time_string = find_best_date(content)

        # --- C. Extract Inventory Items ---
        lines = content.splitlines()
        current_name = None
        current_descr = None
        
        for line in lines:
            line = line.strip()

            nd_match = NAME_DESCR_PATTERN.search(line)
            if nd_match:
                current_name = nd_match.group(1)
                current_descr = nd_match.group(2)
                continue 

            if current_name and line.startswith("PID:"):
                ps_match = PID_SN_PATTERN.search(line)
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
                        'Source File': full_display_path,
                        'College': college_name,
                        '_Hostname_Priority': hostname_priority
                    })
                
                current_name = None 
                current_descr = None
                
    except Exception as e:
        print(f"  [ERROR] parsing content of {filename}: {e}", flush=True)


def parse_inventory_files(root_folder):
    extracted_data = []
    files_processed = 0
    archives_processed = 0

    abs_root_folder = os.path.abspath(root_folder)
    print(f"Scanning recursively in: {abs_root_folder}...\n")

    if not os.path.exists(abs_root_folder):
        print(f"Error: The folder '{abs_root_folder}' does not exist.")
        return

    # --- 2. Recursively Walk through folders ---
    for dirpath, dirnames, filenames in os.walk(abs_root_folder):
        for filename in filenames:
            full_file_path = os.path.join(dirpath, filename)
            
            if "Inventory_Report" in filename:
                continue

            college_val = get_college_from_path(full_file_path)

            # A. ZIP Files
            if filename.lower().endswith('.zip'):
                print(f"Archive found: {filename} ... unpacking", flush=True)
                try:
                    with zipfile.ZipFile(full_file_path, 'r') as z:
                        archives_processed += 1
                        for member in z.namelist():
                            if not member.endswith('/'): 
                                print(f"  -> Reading inside zip: {member}", flush=True)
                                with z.open(member) as f:
                                    content = f.read().decode('utf-8', errors='ignore')
                                    display_path = f"{full_file_path} > {member}"
                                    process_content(content, display_path, member, college_val, extracted_data)
                except Exception as e:
                    print(f"  [ERROR] Could not read zip {filename}: {e}", flush=True)

            # B. TAR/GZ Files
            elif filename.lower().endswith(('.tar', '.tar.gz', '.tgz')):
                print(f"Archive found: {filename} ... unpacking", flush=True)
                try:
                    with tarfile.open(full_file_path, 'r:*') as t:
                        archives_processed += 1
                        for member in t.getmembers():
                            if member.isfile():
                                print(f"  -> Reading inside tar: {member.name}", flush=True)
                                f = t.extractfile(member)
                                if f:
                                    content = f.read().decode('utf-8', errors='ignore')
                                    display_path = f"{full_file_path} > {member.name}"
                                    process_content(content, display_path, member.name, college_val, extracted_data)
                except Exception as e:
                    print(f"  [ERROR] Could not read tar {filename}: {e}", flush=True)

            # C. Regular Text Files
            elif not filename.lower().endswith(('.xlsx', '.xls', '.py', '.exe', '.bin', '.dll')):
                try:
                    print(f"Reading file: {filename}", flush=True)
                    files_processed += 1
                    with open(full_file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                        process_content(content, full_file_path, filename, college_val, extracted_data)
                except Exception as e:
                    print(f"  [ERROR] reading {filename}: {e}", flush=True)

    # --- 3. Process Data ---
    if extracted_data:
        print("\nProcessing data and generating Excel...", flush=True)
        df = pd.DataFrame(extracted_data)
        
        # 1. Create temp column for Description Length
        df['Descr_Len'] = df['DESCR'].astype(str).map(len)

        # 2. TECHNICAL SORT (For Deduplication)
        df.sort_values(
            by=['_Hostname_Priority', 'Descr_Len', 'Capture Time'], 
            ascending=[False, False, False], 
            na_position='last', 
            inplace=True
        )
        
        initial_count = len(df)
        
        # 3. Deduplication (Strictly by SN)
        df.drop_duplicates(subset=['SN'], keep='first', inplace=True)
        
        final_count = len(df)
        duplicates_removed = initial_count - final_count

        # 4. VISUAL SORT (Hostname -> Component Name)
        df.sort_values(by=['Hostname', 'NAME'], ascending=[True, True], inplace=True)

        # 5. Cleanup
        df = df[['Hostname', 'Original Time String', 'SN', 'PID', 'NAME', 'DESCR', 'Source File', 'College']]
        df.rename(columns={'Original Time String': 'Capture Time'}, inplace=True)

        output_file = os.path.join(abs_root_folder, 'Inventory_Report_Final.xlsx')
        df.to_excel(output_file, index=False)
        
        print("-" * 30)
        print(f"Success!")
        print(f"Processed {files_processed} regular files.")
        print(f"Processed {archives_processed} compressed archives.")
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
