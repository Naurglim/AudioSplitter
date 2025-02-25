import os
import sys
from pydub import AudioSegment
from pathlib import Path
import math

from pipeline import Pipeline


SEGMENT_LENGTH = 15*60*1000  # 15 minutes in milliseconds
PATH_AUDIO = 'audios/'  # Path where the audio files are located
PATH_TRANSCRIPTION = 'transcriptions/'  # Path where the transcriptions will be saved
os.makedirs(PATH_AUDIO, exist_ok=True)
os.makedirs(PATH_TRANSCRIPTION, exist_ok=True)


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


if __name__ == '__main__':
    pipeline.run(video_filename=sys.argv[1])