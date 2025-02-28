import os
import sys
from pydub import AudioSegment
from pathlib import Path
import math
from google.cloud.storage import Client, transfer_manager
from google.cloud import speech
from datetime import timedelta

from pipeline import Pipeline
from config import SEGMENT_LENGTH, GCP_BUCKET, GCP_PROJECT, PATH_AUDIO, PATH_TRANSCRIPTION, MAX_SEGMENTS

# In case the folders do not exist, create them:
os.makedirs(PATH_AUDIO, exist_ok=True)
os.makedirs(PATH_TRANSCRIPTION, exist_ok=True)
os.environ.setdefault("GCLOUD_PROJECT", GCP_PROJECT)

pipeline = Pipeline()

@pipeline.task()
def extract_audio(video_filename: str) -> str:
    """
    Extract the audio from the given video file and save it as a FLAC file.

    The output audio file will be saved in the specified audio directory.
    """
    output_audio_filename = Path(video_filename).stem + '.flac'
    full_audio_filename = os.path.join(PATH_AUDIO, output_audio_filename)
    # Extract the audio from the video file and export directly to the destination directory
    AudioSegment.from_file(video_filename).export(full_audio_filename, format='flac')
    print(f"Audio file saved as {output_audio_filename}")
    return full_audio_filename


@pipeline.task(depends_on=extract_audio)
def split_audio(full_audio_filename : str, segment_length : int = SEGMENT_LENGTH) -> str:
    """
    Split the audio file into multiple segments of a specified length.

    Each segment is saved as a separate file in the output directory.
    The segment length is specified in milliseconds.
    """
    filename = Path(full_audio_filename)
    audio_stem =  filename.stem
    output_directory = filename.with_suffix('')
    os.makedirs(output_directory, exist_ok=True)

    audio = AudioSegment.from_file(full_audio_filename)
    total_length = len(audio)
    num_segments = math.ceil(total_length / segment_length)

    # Loop through and create each segment
    for i in range(num_segments):
        start_time = i * segment_length
        end_time = min((i + 1) * segment_length, total_length)  # Ensure the last segment does not exceed total length
        segment = audio[start_time:end_time]

        # Export the segment to a flac file
        part = i + 1
        output_file = Path(output_directory) / f"{audio_stem}_part{part:02d}.flac"
        segment.export(output_file, format="flac") 
        print(f"Exported: {output_file}")
    return str(output_directory)


@pipeline.task(depends_on=split_audio)
def upload_to_bucket(source_directory : str, workers : int = 8, bucket_name : str = GCP_BUCKET) -> str:
    """Upload every file in a directory, including all files in subdirectories.

    Each blob name is derived from the filename, not including the `directory`
    parameter itself. For complete control of the blob name for each file (and
    other aspects of individual blob metadata), use
    transfer_manager.upload_many() instead.
    """

    storage_client = Client()
    bucket = storage_client.bucket(bucket_name)

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
def transcribe_from_bucket(source_directory : str, bucket_name : str = GCP_BUCKET) -> str:
    """
    Transcribe the audio files in the specified bucket and save the transcriptions in a separate directory.
    """
    # get the last folder of the path
    source_path = Path(source_directory)
    archivo = source_path.parts[-1]
    
    print(f"Transcribing audio file '{archivo}' from bucket: {bucket_name}")

    for part in range(1, MAX_SEGMENTS + 1):
        text_filename = f"{archivo}_part{part:02d}.txt"
        audio_filename = f"{archivo}_part{part:02d}.flac"
        if not file_exists(bucket_name, audio_filename):
            print(f"Couldn't find file {audio_filename}")
            break

        gcs_file = f"gs://{bucket_name}/{audio_filename}"
        transcription_path = Path(PATH_TRANSCRIPTION) / bucket_name
        transcription_path.mkdir(parents=True, exist_ok=True)
        transcription_file = f"transcriptions/{bucket_name}/{archivo}/{text_filename}"
        transcript = transcribe_gcs(gcs_file)
        to_file(transcription_file, transcript)


def file_exists(bucket_name: str, file_name: str) -> bool:
    storage_client = Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    return blob.exists()


def transcribe_gcs(gcs_uri: str) -> str:
    """Asynchronously transcribes the audio file from Cloud Storage
    Args:
        gcs_uri: The Google Cloud Storage path to an audio file.
            E.g., "gs://storage-bucket/file.flac".
    Returns:
        The generated transcript from the audio file provided.
    """
    client = speech.SpeechClient()
    audio = speech.RecognitionAudio(uri=gcs_uri)
    diarization_config = speech.SpeakerDiarizationConfig(
        enable_speaker_diarization=True,
        min_speaker_count=1,
        max_speaker_count=10,
    )
    config = speech.RecognitionConfig(
        encoding=speech.RecognitionConfig.AudioEncoding.FLAC,
        language_code="es-AR",
        enable_word_time_offsets=True,
        enable_automatic_punctuation=True,
        # diarization_config=diarization_config,
        audio_channel_count=2,
    )
    operation = client.long_running_recognize(config=config, audio=audio, timeout=3600)

    print("Waiting for operation to complete...")
    response = operation.result()

    transcript_builder = []
    # Each result is for a consecutive portion of the audio. Iterate through
    # them to get the transcripts for the entire audio file.
    result_part = 0
    for result in response.results:
        result_part += 1
        print("Processing result: batch {}".format(result_part))

        # The first alternative is the most likely one for this portion.
        best_option = result.alternatives[0]

        # transcript_builder.append(f"\nConfidence: {result.alternatives[0].confidence}")
        part_start_time = best_option.words[0].start_time.total_seconds()
        # speakers = set()

        for word_info in best_option.words:
            word = word_info.word
            start_time = word_info.start_time.total_seconds()
            if part_start_time > start_time:
                part_start_time = start_time
            # end_time = word_info.end_time
            # speakers.add(word_info.speaker_tag)

        # list_speakers = ", ".join([str(speaker) for speaker in speakers])
        str_time = str(timedelta(seconds=part_start_time))
        transcript_builder.append(f"\n\nStart: {str_time} - Confidence: {best_option.confidence}\n{best_option.transcript}")

    transcript = "".join(transcript_builder)

    return transcript


def to_file(file_name: str, content: str) -> None:
    file_name_path = Path(file_name)
    file_name_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_name, "w") as file:
        file.write(content)
   

if __name__ == '__main__':
    pipeline.run(video_filename=sys.argv[1])