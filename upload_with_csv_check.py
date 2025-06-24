import csv
import os
import json
from labelbox import Client
from pathlib import Path
from dotenv import load_dotenv
import subprocess

load_dotenv()

def load_video_inventory(csv_path):
    inventory = {}
    if os.path.exists(csv_path):
        with open(csv_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                inventory[row['video_name']] = {
                    'dataset_name': row['dataset_name'],
                    'data_row_id': row['data_row_id'],
                    'dataset_id': row['dataset_id']
                }
    return inventory

def update_video_inventory(csv_path, new_videos):
    """Update the CSV inventory with newly uploaded videos."""
    existing_data = []
    if os.path.exists(csv_path):
        with open(csv_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            existing_data = list(reader)
    
    # Add new videos
    existing_data.extend(new_videos)
    
    # Write updated inventory
    with open(csv_path, 'w', newline='') as csvfile:
        fieldnames = ['dataset_name', 'video_name', 'data_row_id', 'dataset_id']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(existing_data)

def get_or_create_dataset(client, dataset_name):
    datasets = client.get_datasets()
    for dataset in datasets:
        if dataset.name == dataset_name:
            print(f"Found existing dataset: {dataset_name}")
            return dataset
    
    print(f"Creating new dataset: {dataset_name}")
    return client.create_dataset(name=dataset_name)

def upload_videos_with_csv_check(api_key, video_folder_path, inventory_csv_path):
    """
    Upload videos to Labelbox with CSV-based duplicate checking
    """
    client = Client(api_key=api_key)
    
    print("Loading video inventory...")
    inventory = load_video_inventory(inventory_csv_path)
    print(f"Found {len(inventory)} videos in inventory")
    
    # Get folder name for dataset
    folder_name = Path(video_folder_path).name
    print(f"Using folder name '{folder_name}' for dataset")
    
    dataset = get_or_create_dataset(client, folder_name)
    
    video_extensions = {'.mp4', '.mov', '.avi', '.mkv', '.wmv'}
    
    # Process each video in the folder
    video_folder = Path(video_folder_path)
    if not video_folder.exists():
        print(f"Error: Video folder path does not exist: {video_folder_path}")
        return
    
    print(f"\nScanning for videos in: {video_folder_path}")
    video_count = 0
    skipped_count = 0
    new_videos = []
    
    # Create a temporary output folder for converted videos
    converted_folder = Path('converted_videos')
    converted_folder.mkdir(exist_ok=True)
    
    for video_path in video_folder.rglob('*'):
        if video_path.suffix.lower() in video_extensions:
            video_count += 1
            video_name = video_path.name
            
            # Check if video exists in inventory
            if video_name in inventory:
                existing_info = inventory[video_name]
                print(f"Skipping {video_name} - already exists in dataset: {existing_info['dataset_name']}")
                skipped_count += 1
                continue

            # Convert video to Labelbox-compatible format
            converted_path = converted_folder / video_name
            command = [
                'ffmpeg',
                '-i', str(video_path),
                '-vcodec', 'libx264',
                '-acodec', 'aac',
                '-movflags', '+faststart',
                '-y',  # Overwrite output file if it exists
                str(converted_path)
            ]
            print(f"Converting {video_name} to Labelbox-compatible format...")
            result = subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
            if result.returncode != 0 or not converted_path.exists():
                print(f"Error converting {video_name}, skipping upload.")
                continue

            try:
                # Upload the converted video
                print(f"Uploading {video_name}...")
                data_row = dataset.create_data_row(
                    row_data=str(converted_path.absolute()),
                    external_id=video_name
                )
                print(f"Successfully uploaded {video_name}")
                
                # Add to new videos list for CSV update
                new_videos.append({
                    'dataset_name': folder_name,
                    'video_name': video_name,
                    'data_row_id': data_row.uid,
                    'dataset_id': dataset.uid
                })
            except Exception as e:
                print(f"Error uploading {video_name}: {str(e)}")
            finally:
                # Clean up the converted file
                if converted_path.exists():
                    try:
                        converted_path.unlink()
                    except Exception as cleanup_err:
                        print(f"Warning: Could not delete temporary file {converted_path}: {cleanup_err}")
    
    # Update CSV with new videos
    if new_videos:
        print("\nUpdating video inventory...")
        update_video_inventory(inventory_csv_path, new_videos)
        print(f"Added {len(new_videos)} new videos to inventory")
    
    print(f"\nSummary:")
    print(f"Total videos found: {video_count}")
    print(f"Videos skipped (already exist): {skipped_count}")
    print(f"New videos uploaded: {len(new_videos)}")

if __name__ == "__main__":

    LABELBOX_API_KEY = os.getenv('LABELBOX_API_KEY')
    VIDEO_FOLDER_PATH = os.getenv('VIDEO_FOLDER_PATH')
    INVENTORY_CSV = os.getenv('INVENTORY_CSV')
    
    if not all([LABELBOX_API_KEY, VIDEO_FOLDER_PATH, INVENTORY_CSV]):
        raise ValueError("Missing required environment variables. Please check your .env file.")
    
    upload_videos_with_csv_check(LABELBOX_API_KEY, VIDEO_FOLDER_PATH, INVENTORY_CSV) 