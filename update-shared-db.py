import argparse
import logging
import os
import sys
import yaml

# Configure logging to print to stdout
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

# Create a stream handler to print to stdout
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)

# Create a formatter and set it for the handler
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Add the handler to the logger
log.addHandler(handler)

def load_yaml(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

def save_yaml(data, file_path):
    with open(file_path, 'w') as file:
        yaml.dump(data, file, default_flow_style=False)

def find_matching_tool_in_shared_db(tool, shared_db):
    if tool in shared_db['tools']:
        return tool
    elif (tool + "/.*") in shared_db['tools']:
        return (tool + "/.*")
    else:
        for key in shared_db['tools'].keys():
            if tool in key:
                log.debug(f"Closest matching tool for tool: {tool} in shared db is: {key}")
                return key
    log.debug(f"Could not find tool: {tool} in shared db")
    return None

def adjust_memory(shared_db, mem_wastage_data):
    for tool, data in mem_wastage_data.items():
        found_tool = find_matching_tool_in_shared_db(tool, shared_db)
        if found_tool:
            if 'mem' in shared_db['tools'][found_tool]:
                original_mem = shared_db['tools'][found_tool]['mem']
                wasted_mem = float(data['mem_wastage_min_gb'])
                adjusted_mem = max(0, original_mem - wasted_mem)
                shared_db['tools'][found_tool]['mem'] = round(adjusted_mem, 2)
            else:
                log.debug(f"No mem entry for tool: {found_tool} in shared db. Setting mem")
                wasted_mem = float(data['mem_wastage_min_gb'])
                adjusted_mem = float(data['max_tpv_mem_gb']) - wasted_mem
                shared_db['tools'][found_tool]['mem'] = round(adjusted_mem, 2)
        else:
            log.debug(f"No entry for tool: {tool} in shared db. Adding one")
            wasted_mem = float(data['mem_wastage_min_gb'])
            adjusted_mem = float(data['max_tpv_mem']) - wasted_mem
            tool = (tool + "/.*") if "toolshed" in tool else tool
            shared_db['tools'][tool] = {'mem': round(adjusted_mem, 2)}

    return shared_db

def main(tpv_shared_db_path, mem_wastage_path):
    # Load the shared database YAML file
    shared_db = load_yaml(tpv_shared_db_path)
    
    # Load the memory wastage output YAML file
    mem_wastage_data = load_yaml(mem_wastage_path)
    
    # Adjust memory in the shared database
    updated_shared_db = adjust_memory(shared_db, mem_wastage_data)
    
    # Save the updated shared database back to the file
    save_yaml(updated_shared_db, tpv_shared_db_path)
    print(f"Updated shared database saved to {tpv_shared_db_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Update the TPV shared database YAML file with adjusted memory values.')
    parser.add_argument('tpv_shared_db_path', type=str, help='Path to the TPV shared database YAML file')
    parser.add_argument('mem_wastage_path', type=str, help='Path to the memory wastage output YAML file')

    args = parser.parse_args()

    if not os.path.exists(args.tpv_shared_db_path):
        print(f"Error: The file {args.tpv_shared_db_path} does not exist.")
        exit(1)
    
    if not os.path.exists(args.mem_wastage_path):
        print(f"Error: The file {args.mem_wastage_path} does not exist.")
        exit(1)
    
    main(args.tpv_shared_db_path, args.mem_wastage_path)
