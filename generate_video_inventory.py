import csv
import os
from labelbox import Client
from dotenv import load_dotenv

load_dotenv()

def generate_video_inventory(api_key, output_csv_path=None):
    """
    Generate a CSV file containing all datasets and their videos.
    """
    client = Client(api_key=api_key)
    
    # Create default CSV path if none provided
    if output_csv_path is None:
        output_csv_path = f"labelbox_video_inventory.csv"
    
    print(f"Generating video inventory...")
    print(f"Output will be saved to: {output_csv_path}")
    
    inventory_data = []
    
    for dataset in client.get_datasets():
        print(f"\nProcessing dataset: {dataset.name}")
        
        # Get videos in this dataset
        for data_row in dataset.data_rows():
            if data_row.external_id:  # This is the video filename
                inventory_data.append({
                    'dataset_name': dataset.name,
                    'video_name': data_row.external_id,
                    'data_row_id': data_row.uid,
                    'dataset_id': dataset.uid
                })
                print(f"Found video: {data_row.external_id}")
    
    # Write to CSV
    if inventory_data:
        with open(output_csv_path, 'w', newline='') as csvfile:
            fieldnames = ['dataset_name', 'video_name', 'data_row_id', 'dataset_id']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            writer.writerows(inventory_data)
        
        print(f"\nSuccessfully created inventory with {len(inventory_data)} videos")
        print(f"Saved to: {output_csv_path}")
    else:
        print("\nNo videos found in any dataset")

if __name__ == "__main__":
    LABELBOX_API_KEY = os.getenv('LABELBOX_API_KEY')
    if not LABELBOX_API_KEY:
        raise ValueError("LABELBOX_API_KEY not found in environment variables")
    
    generate_video_inventory(LABELBOX_API_KEY) 