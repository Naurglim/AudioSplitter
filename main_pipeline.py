import os
import sys
from pydub import AudioSegment
from pathlib import Path
import math
from google.cloud.storage import Client, transfer_manager
from google.cloud import speech
from datetime import timedelta

from pipeline import Pipeline


SEGMENT_LENGTH = 15*60*1000  # 15 minutes in milliseconds
GCP_BUCKET = 'seminario_hamlet' # Name of the GCP bucket
GCP_PROJECT = 'transcriptor-449915'  # Name of the GCP project
PATH_AUDIO = 'audios/'  # Path where the audio files are located
PATH_TRANSCRIPTION = 'transcriptions/'  # Path where the transcriptions will be saved
os.makedirs(PATH_AUDIO, exist_ok=True)
os.makedirs(PATH_TRANSCRIPTION, exist_ok=True)
os.environ.setdefault("GCLOUD_PROJECT", GCP_PROJECT)

pipeline = Pipeline()

@pipeline.task()
def extract_audio(video_filename: str) -> str:
    audio_filename =  Path(video_filename).stem + '.flac'
    # Extract the audio from the video file
    AudioSegment.from_file(video_filename).export(audio_filename, format='flac')
    # Move the audio file to the audio directory   
    full_audio_filename = os.path.join(PATH_AUDIO, audio_filename)
    os.rename(audio_filename, full_audio_filename)
    print(f"Audio file saved as {audio_filename}")
    return full_audio_filename


@pipeline.task(depends_on=extract_audio)
def split_audio(file_path): 
    segment_length = SEGMENT_LENGTH

    filename = Path(file_path)
    audio_stem =  filename.stem
    original_path = filename.with_suffix('')
    os.makedirs(original_path, exist_ok=True)

    # Load the audio file
    audio = AudioSegment.from_file(file_path)
    
    # Get the total length of the audio file
    total_length = len(audio)
    
    # Calculate the number of segments needed
    num_segments = math.ceil(total_length / segment_length)

    # Loop through and create each segment
    for i in range(num_segments):
        start_time = i * segment_length
        end_time = min((i + 1) * segment_length, total_length)  # Ensure the last segment does not exceed total length
        segment = audio[start_time:end_time]

        # Generate the output file name
        part = i + 1
        output_file = f"{original_path}/{audio_stem}_part{part:02d}.flac"
        
        # Export the segment as an m4a file
        segment.export(output_file, format="flac") 
        print(f"Exported: {output_file}")
    return original_path


@pipeline.task(depends_on=split_audio)
def upload_to_bucket(source_directory):
    """Upload every file in a directory, including all files in subdirectories.

    Each blob name is derived from the filename, not including the `directory`
    parameter itself. For complete control of the blob name for each file (and
    other aspects of individual blob metadata), use
    transfer_manager.upload_many() instead.
    """

    bucket_name = GCP_BUCKET
    workers = 8

    storage_client = Client()
    bucket = storage_client.bucket(bucket_name)

    # Generate a list of paths (in string form) relative to the `directory`.
    # This can be done in a single list comprehension, but is expanded into
    # multiple lines here for clarity.

    # First, recursively get all files in `directory` as Path objects.
    directory_as_path_obj = Path(source_directory)
    paths = directory_as_path_obj.rglob("*")

    # Filter so the list only includes files, not directories themselves.
    file_paths = [path for path in paths if path.is_file()]

    # These paths are relative to the current working directory. Next, make them
    # relative to `directory`
    relative_paths = [path.relative_to(source_directory) for path in file_paths]

    # Finally, convert them all to strings.
    string_paths = [str(path) for path in relative_paths]

    print("Found {} files.".format(len(string_paths)))

    # Start the upload.
    results = transfer_manager.upload_many_from_filenames(
        bucket, string_paths, source_directory=source_directory, max_workers=workers
    )

    for name, result in zip(string_paths, results):
        # The results list is either `None` or an exception for each filename in
        # the input list, in order.

        if isinstance(result, Exception):
            print("Failed to upload {} due to exception: {}".format(name, result))
        else:
            print("Uploaded {} to {}.".format(name, bucket.name))
    return source_directory


@pipeline.task(depends_on=upload_to_bucket)
def transcribe_from_bucket():
    pass


if __name__ == '__main__':
    pipeline.run(video_filename=sys.argv[1])