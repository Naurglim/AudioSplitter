from google.cloud import speech
from datetime import timedelta


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
        min_speaker_count=2,
        max_speaker_count=2,
    )

    config = speech.RecognitionConfig(
        encoding=speech.RecognitionConfig.AudioEncoding.FLAC,
        sample_rate_hertz=44100, # 48000
        language_code="es-AR",
        enable_word_time_offsets=True,
        diarization_config=diarization_config,
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
        transcript_builder.append(f"\n{str_time}\n{best_option.transcript}")

    transcript = "".join(transcript_builder)

    return transcript


def to_file(file_name: str, content: str) -> None:
    with open(file_name, "w") as file:
        file.write(content)

    
if __name__ == "__main__":
    carpeta = "seminario_hamlet"
    archivo = "seminario_hamlet_clase_04"
    for part in range(1, 11):
        clase = f"{archivo}_part{part:02d}"
        gcs_file = f"gs://{carpeta}/{clase}.flac"
        transcription_file = f"transcriptions/{carpeta}/{clase}.txt"
        transcript = transcribe_gcs(gcs_file)
        to_file(transcription_file, transcript)