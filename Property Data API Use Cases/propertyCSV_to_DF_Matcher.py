import requests
import csv
import json

# Set your API parameters.
API_token = 'API_KEY_HERE'
format = 'JSON'
num_records = 1
download = False

request_headers = {
    'Authorization': f'Bearer {API_token}',
    'Content-Type': 'application/json',
}

# Read CSV and process each row
# Be sure the header is address,postalCode
csv_file = "C:\\Users\\Leonard\\Documents\\pythonScripts\\PropertyMatching\\parsed_addresses_FINAL_fixed_v5.csv"  # Update with the path to your CSV file
json_file = "C:\\Users\\Leonard\\Documents\\pythonScripts\\PropertyMatching\\DF_match_output_2025.json"  # Output JSON file

results = []
total_found = 0  # Counter for total records appended

with open(csv_file, newline='', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    rows = list(reader)  # Convert reader to list to count rows
    print(f'Total rows in CSV: {len(rows)}')

    for row in rows:
        address = row['address']
        postal_code = row['postalCode']
        print(str(row))
        
        query = f'address:"{address}" AND postalCode:{postal_code}*' # search based on the input csv_file 
        request_data = {
            'query': query,
            'format': format,
            'num_records': num_records,
            'download': download
        }
        
        # Make the API call
        r = requests.post('https://api.datafiniti.co/v4/properties/search', json=request_data, headers=request_headers)
        
        # Handle the response
        if r.status_code == 200:
            response_data = json.loads(r.content)
            if 'records' in response_data:
                results.extend(response_data['records'])  # Append only 'records' key
            if 'num_found' in response_data:
                total_found += response_data['num_found']  # Count total found records
            else:
                print("No records found for this address")
        else:
            results.append({'error': f'Request failed for address: {address}, postalCode: {postal_code}'})

# Write results to JSON file
with open(json_file, 'w', encoding='utf-8') as outfile:
    json.dump(results, outfile, indent=4)

print(f'Total records found: {total_found}')