import json


# Load JSON files
def load_json(file_path):
    with open(file_path, "r") as file:
        return json.load(file)


# Filter objects by specified keys
def filter_object(obj):
    keys_to_match = ["pageID", "element", "timestamp", "event"]
    return {key: obj[key] for key in keys_to_match}


# Check equivalence
def has_equivalent(obj, target_list):
    filtered_obj = filter_object(obj)
    for target in target_list:
        if filtered_obj == filter_object(target):
            return True
    return False


# Compare JSON files
def compare_jsons(json1, json2):
    missing_objects = []
    for obj in json1:
        if not has_equivalent(obj, json2):
            missing_objects.append(obj)
    return missing_objects


# Main function
if __name__ == "__main__":
    # Replace with your JSON file paths
    file1 = "node-2-4-b.json"
    file2 = "node-2-4-a.json"

    json1 = load_json(file1)
    json2 = load_json(file2)

    missing_objects = compare_jsons(json1, json2)

    if missing_objects:
        print("Objects in file1 not found in file2:")
        for obj in missing_objects:
            print(obj)
    else:
        print("All objects in file1 have equivalents in file2.")
