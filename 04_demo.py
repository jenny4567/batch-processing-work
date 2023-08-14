import os
import csv
import boto3
from collections import Counter

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    source_bucket = os.environ['SOURCE_BUCKET'] 
    dest_bucket = os.environ['DEST_BUCKET']

    student_name = "jenny-wark"  # Change this to your own name!

    # Download the file from S3
    s3.download_file(source_bucket, 'IMDB-dataset-files/ImdbTitlePrincipals.csv', '/tmp/title_principals.csv')
    
    # Read the file using csv module
    occupations = []
    with open('/tmp/title_principals.csv', mode='r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            occupations.append(row['category'])

    # Calculate the top 10 most frequent occupations
    top_occupations = Counter(occupations).most_common(10)

    # Save the result as a CSV file
    with open('/tmp/top_occupations.csv', mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['occupation', 'count'])
        writer.writerows(top_occupations)

    # Upload the result to the destination S3 bucket in the specified folder
    s3_key = f"{student_name}/top_occupations.csv"
    s3.upload_file('/tmp/top_occupations.csv', dest_bucket, s3_key)

    return {
        'statusCode': 200,
        'body': 'Top occupations have been calculated and saved to the destination S3 bucket.'
    }