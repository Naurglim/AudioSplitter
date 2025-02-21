import os
import glob
from pydub import AudioSegment
from pathlib import Path

video_dir = 'videos/'  # Path where the videos are located
audio_dir = 'audios/'  # Path where the audio files will be saved
os.makedirs(audio_dir, exist_ok=True)
extension_list = ('*.mp4', '*.flv')

for extension in extension_list:
    for video in glob.glob(extension, root_dir=video_dir):
        audio_filename =  Path(video).stem + '.flac'
        video_filename = os.path.join(video_dir, video)
        # Extract the audio from the video file
        AudioSegment.from_file(video_filename).export(audio_filename, format='flac')
        # Move the audio file to the audio directory   
        os.rename(audio_filename, os.path.join(audio_dir, audio_filename))
        print(f"Audio file saved as {audio_filename}")