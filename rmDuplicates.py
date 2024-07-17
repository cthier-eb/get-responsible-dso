import json


def load_and_remove_duplicates(json_file):
    def remove_duplicates(data):
        if isinstance(data, dict):
            cleaned_data = {}
            for key, value in data.items():
                if key not in cleaned_data:
                    cleaned_data[key] = remove_duplicates(value)
                else:
                    print(f"Duplicate key found and removed: {key}")
            return cleaned_data
        elif isinstance(data, list):
            return [remove_duplicates(item) for item in data]
        else:
            return data

    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    cleaned_data = remove_duplicates(data)

    return cleaned_data


# Example usage:
cleaned_json = load_and_remove_duplicates("result copy.json")
with open("cleaned_result.json", 'w', encoding='utf-8') as f:
    json.dump(cleaned_json, f, indent=4, ensure_ascii=False)
