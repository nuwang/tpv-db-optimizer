import argparse
import logging
import math
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
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

# Add the handler to the logger
log.addHandler(handler)


def load_yaml(file_path):
    with open(file_path, "r") as file:
        return yaml.safe_load(file)


def save_yaml(data, file_path):
    with open(file_path, "w") as file:
        yaml.dump(data, file, default_flow_style=False)


def find_matching_tool_in_shared_db(tool, shared_db):
    if tool in shared_db["tools"]:
        return tool
    elif (tool + "/.*") in shared_db["tools"]:
        return tool + "/.*"
    else:
        for key in shared_db["tools"].keys():
            if tool in key:
                log.debug(
                    f"Closest matching tool for tool: {tool} in shared db is: {key}"
                )
                return key
    log.debug(f"Could not find tool: {tool} in shared db")
    return None


def get_proposed_mem(data, tool_entry, tool_name):
    if "mem" in tool_entry:
        tool_mem = tool_entry["mem"]
    else:
        log.debug(f"No mem entry for tool: {tool_name} in shared db. Setting mem")
        tool_mem = float(data["max_tpv_mem_gb"])
    wasted_mem = float(data["mem_wastage_min_gb"])
    adjusted_mem = max(0, tool_mem - wasted_mem)
    return round(adjusted_mem, 2)


def get_proposed_cores(data, tool_entry, tool_name):
    if "cores" in tool_entry:
        tool_cores = tool_entry["cores"]
    else:
        log.debug(f"No cores entry for tool: {tool_name} in shared db. Setting cores")
        if "max_tpv_cores" in data:
            tool_cores = float(data["avg_tpv_cores"])
        else:
            log.debug(f"No max_tpv_cores for: {tool_name} in shared db. Ignoring")
            return None
        # Compute fraction used (based on min wastage) and effective cores
    allocated = float(data["avg_allocated_cpu_seconds"])
    wastage = float(data["cpu_wastage_min_seconds"])
    used_fraction_min = max(0.0, min(1.0, 1.0 - (wastage / allocated)))
    effective_cores = tool_cores * used_fraction_min
    return math.ceil(effective_cores)


def adjust_resources(shared_db, resource_wastage_data):
    for tool, data in resource_wastage_data.items():
        found_tool = find_matching_tool_in_shared_db(tool, shared_db)
        if found_tool:
            tool_entry = shared_db["tools"][found_tool]
            tool_name = found_tool
        else:
            tool_name = (tool + "/.*") if "toolshed" in tool else tool
            tool_entry = {}
            shared_db["tools"][tool_name] = tool_entry

        # adjust resources
        tool_entry["mem"] = get_proposed_mem(data, tool_entry, tool_name)
        tool_entry["cores"] = get_proposed_cores(data, tool_entry, tool_name)

    return shared_db


def main(tpv_shared_db_path, resource_wastage_path):
    # Load the shared database YAML file
    shared_db = load_yaml(tpv_shared_db_path)

    # Load the resource wastage output YAML file
    resource_wastage_data = load_yaml(resource_wastage_path)

    # Adjust resources in the shared database
    updated_shared_db = adjust_resources(shared_db, resource_wastage_data)

    # Save the updated shared database back to the file
    save_yaml(updated_shared_db, tpv_shared_db_path)
    print(f"Updated shared database saved to {tpv_shared_db_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Update the TPV shared database YAML file with adjusted memory values."
    )
    parser.add_argument(
        "tpv_shared_db_path", type=str, help="Path to the TPV shared database YAML file"
    )
    parser.add_argument(
        "resource_wastage_path",
        type=str,
        help="Path to the resource wastage output YAML file",
    )

    args = parser.parse_args()

    if not os.path.exists(args.tpv_shared_db_path):
        print(f"Error: The file {args.tpv_shared_db_path} does not exist.")
        exit(1)

    if not os.path.exists(args.resource_wastage_path):
        print(f"Error: The file {args.resource_wastage_path} does not exist.")
        exit(1)

    main(args.tpv_shared_db_path, args.resource_wastage_path)
