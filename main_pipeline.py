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

    Args:
        video_filename (str): The path to the video file.

    Returns:
        str: The path to the extracted audio file.
    """
    output_audio_filename = Path(video_filename).stem + '.flac'
    full_audio_filename = os.path.join(PATH_AUDIO, output_audio_filename)
    AudioSegment.from_file(video_filename).export(full_audio_filename, format='flac')
    print(f"Audio file saved as {output_audio_filename}")
    return full_audio_filename


@pipeline.task(depends_on=extract_audio)
def split_audio(full_audio_filename: str, segment_length: int = SEGMENT_LENGTH) -> str:
    """
    Split the audio file into multiple segments of a specified length.

    Args:
        full_audio_filename (str): The path to the full audio file.
        segment_length (int): The length of each segment in milliseconds.

    Returns:
        str: The directory containing the audio segments.
    """
    filename = Path(full_audio_filename)
    audio_stem = filename.stem
    output_directory = filename.with_suffix('')
    os.makedirs(output_directory, exist_ok=True)

    audio = AudioSegment.from_file(full_audio_filename)
    total_length = len(audio)
    num_segments = math.ceil(total_length / segment_length)

    for i in range(num_segments):
        start_time = i * segment_length
        end_time = min((i + 1) * segment_length, total_length)
        segment = audio[start_time:end_time]

        part = i + 1
        output_file = Path(output_directory) / f"{audio_stem}_part{part:02d}.flac"
        segment.export(output_file, format="flac")
        print(f"Exported: {output_file}")
    return str(output_directory)


@pipeline.task(depends_on=split_audio)
def upload_to_bucket(source_directory: str, workers: int = 8, bucket_name: str = GCP_BUCKET) -> str:
    """
    Upload every file in a directory, including all files in subdirectories, to a Google Cloud Storage bucket.

    Args:
        source_directory (str): The path to the source directory.
        workers (int): The number of workers to use for the upload.
        bucket_name (str): The name of the Google Cloud Storage bucket.

    Returns:
        str: The source directory.
    """
    storage_client = Client()
    bucket = storage_client.bucket(bucket_name)

    directory_as_path_obj = Path(source_directory)
    paths = directory_as_path_obj.rglob("*")
    file_paths = [path for path in paths if path.is_file()]
    relative_paths = [path.relative_to(source_directory) for path in file_paths]
    string_paths = [str(path) for path in relative_paths]

    print("Found {} files.".format(len(string_paths)))

    results = transfer_manager.upload_many_from_filenames(
        bucket, string_paths, source_directory=source_directory, max_workers=workers
    )

    for name, result in zip(string_paths, results):
        if isinstance(result, Exception):
            print("Failed to upload {} due to exception: {}".format(name, result))
        else:
            print("Uploaded {} to {}.".format(name, bucket.name))
    return source_directory


@pipeline.task(depends_on=upload_to_bucket)
def transcribe_from_bucket(source_directory: str, bucket_name: str = GCP_BUCKET) -> str:
    """
    Transcribe the audio files in the specified bucket and save the transcriptions in a separate directory.

    Args:
        source_directory (str): The path to the source directory.
        bucket_name (str): The name of the Google Cloud Storage bucket.

    Returns:
        str: The source directory.
    """
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
    """
    Check if a file exists in a Google Cloud Storage bucket.

    Args:
        bucket_name (str): The name of the Google Cloud Storage bucket.
        file_name (str): The name of the file.

    Returns:
        bool: True if the file exists, False otherwise.
    """
    storage_client = Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    return blob.exists()


def transcribe_gcs(gcs_uri: str) -> str:
    """
    Asynchronously transcribes the audio file from Cloud Storage.

    Args:
        gcs_uri (str): The Google Cloud Storage path to an audio file.

    Returns:
        str: The generated transcript from the audio file provided.
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
        audio_channel_count=2,
    )
    operation = client.long_running_recognize(config=config, audio=audio, timeout=3600)

    print("Waiting for operation to complete...")
    response = operation.result()

    transcript_builder = []
    result_part = 0
    for result in response.results:
        result_part += 1
        print("Processing result: batch {}".format(result_part))

        best_option = result.alternatives[0]
        part_start_time = best_option.words[0].start_time.total_seconds()

        for word_info in best_option.words:
            start_time = word_info.start_time.total_seconds()
            if part_start_time > start_time:
                part_start_time = start_time

        str_time = str(timedelta(seconds=part_start_time))
        transcript_builder.append(f"\n\nStart: {str_time} - Confidence: {best_option.confidence}\n{best_option.transcript}")

    transcript = "".join(transcript_builder)
    return transcript


def to_file(file_name: str, content: str) -> None:
    """
    Write content to a file.

    Args:
        file_name (str): The name of the file.
        content (str): The content to write to the file.
    """
    file_name_path = Path(file_name)
    file_name_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_name, "w") as file:
        file.write(content)


if __name__ == '__main__':
    pipeline.run(video_filename=sys.argv[1])