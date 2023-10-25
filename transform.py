import io
import boto3
import chardet
import requests
import re 
import csv
from thefuzz import fuzz, process 

def handler(event, context):
    '''
Author: 
    Sai Manogyana Tokachichu
'''


    url = 'https://mason360.gmu.edu/events'
    url1 = 'https://mason360.gmu.edu/mobile_ws/v17/mobile_events_list'
    res = requests.get(url1)

    #print(res.status_code)

    if res.status_code == 200:
        res = res.json()
        '''for i in range(len(res)):
            print(res[i])'''
    

#Text Processsing 
    events = []
    pattern = r'<[^>]*>'
    id = 1
    event_locs = []
    for i in range(len(res)):
        if 'eventId' in res[i]['fields']:
            name = res[i]['p3'].strip()
            location = re.sub(pattern, '', res[i]['p6']).strip()

            dt = res[i]['p4'].split('</p>')
            for j in range(len(dt)):
                out = re.sub(pattern, '', dt[j]).replace('&ndash;', '-').strip().rstrip('-')
                dt[j] = out
            dt.pop()

            #time and date 
            events.append({'id': id, 'event_name': name, 'loc': location, 'datetime': dt, 'event_status': event_status})
            event_locs.append(location)
            id += 1
        else:
            event_status = res[i]['p1']
    
    for e in events:
        print(e)
    print(len(events))
    
#Attaching the shapeID to each event using fuzzy string matching algorithm. 

    # Define the CSV file name
    csv_file = "buildings.csv"

    # Initialize an empty dictionary to store the data
    buildings_list = {}

    # Open the CSV file in read mode
    def read_from_s3():
        # Replace these values with your S3 bucket and object key
        s3_bucket = 'event-finder-project'
        s3_object_key = 'buildings.csv'
        
        # Create an S3 client
        s3_client = boto3.client('s3')
        
        try:
            # Fetch the S3 object
            response = s3_client.get_object(Bucket=s3_bucket, Key=s3_object_key)
            print(response)
            # Detect the encoding of the data
            s3_object = response['Body'].read()
            print(s3_object)
            detected_encoding = chardet.detect(s3_object)

            # Use the detected encoding
            s3_object = s3_object.decode(detected_encoding['encoding'])
            # Read the content of the object
            #s3_object = response['Body'].read().decode('utf-8')
            
            # You can now process the content as needed, for example, using csv.DictReader
            csv_reader = csv.DictReader(io.StringIO(s3_object))
            buildings = {}
            for row in csv_reader:
                # Process each row in the CSV
                print(row)  # or do something else with the data
                key = row['Key']
                value = row['Value']
                buildings[key] = value

            return {
                'statusCode': 200,
                'body': 'File processed successfully',
                'data': buildings
            }
        except Exception as e:
            print(f"Error: Reading at buildings")
            return {
                'statusCode': 500,
                'body': 'Error processing the file'
            }
        
    result = read_from_s3()
    if result['statusCode'] == 200:
        buildings_list = result['data']
    else:
        return result

    # Print the resulting dictionary
    #print(buildings_list)
    shapeID = []
    for event_loc in event_locs:
        print(event_loc)
        res = process.extract(event_loc, buildings_list, limit=5, scorer=fuzz.partial_token_sort_ratio)
        print(res)
        shapeID.append(res[0][-1]) #Assuming first match is the best match. 

    #Append 'Shape ID' to each row and save it as events.csv
    idx = 0
    for event in events:
        event['shapeID'] = int(shapeID[idx])
        idx += 1

    print(events)

    #Saving transformed data to S3 bucket. 
    def save_csv_to_s3():
        s3_bucket = 'event-finder-project'
        s3_object_key = 'events.csv'  # Desired file name in S3
            
        # Create a CSV string from your data
        csv_data = io.StringIO()
        fieldnames=['id', 'event_name', 'loc', 'datetime', 'event_status', 'shapeID']
        column_data_types = [int, str, str, str, str, int]
        csv_writer = csv.DictWriter(csv_data, fieldnames=fieldnames)
        csv_writer.writeheader()

        for event in events:
            # Convert values to their appropriate data types
            typed_event = {field: data_type(event[field]) for field, data_type in zip(fieldnames, column_data_types)}
            csv_writer.writerow(typed_event)
        
        #csv_writer.writerows(events) -> original code 
        
        # Create an S3 client
        s3_client = boto3.client('s3')
        
        try:
            # Upload the CSV data to S3
            s3_client.put_object(Bucket=s3_bucket, Key=s3_object_key, Body=csv_data.getvalue())
            csv_data.close()
            
            return {
                'statusCode': 200,
                'body': 'File successfully saved to S3'
            }
        except Exception as e:
            print(f"Error: {str(e)}")
            return {
                'statusCode': 500,
                'body': 'Error saving the file to S3'
            }
    result1 = save_csv_to_s3()
    print(result1)
