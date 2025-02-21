import sys
import os
from pydub import AudioSegment
import math
from pathlib import Path

def split_audio(file_path, segment_length=15*60*1000):  # 15 minutes in milliseconds
    
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
        output_file = f"{original_path}/{audio_stem}_part{i+1}.flac"
        
        # Export the segment as an m4a file
        segment.export(output_file, format="flac") 
        print(f"Exported: {output_file}")


source_path = os.path.abspath(sys.argv[1]) 
split_audio(source_path)