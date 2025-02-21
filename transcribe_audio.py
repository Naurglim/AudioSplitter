import os
import sys
import glob
import shutil
from openai import OpenAI

api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise ValueError("Please set the OPENAI_API_KEY environment variable")

client = OpenAI(api_key=api_key)

input_wildcard = sys.argv[1]
input_files = glob.glob(input_wildcard)
print(f"Transcribing from{input_files}")
output_files = []

for x in input_files:
    audio_file= open(x, "rb")
    print("Transcribing file " + x)
    transcription = client.audio.transcriptions.create(
        model="whisper-1",
        file=audio_file,
        response_format="text",
        language="en"
    )
    output_file = f"{x[:-4]}.txt"
    output_files.append(output_file)
    with open(output_file, "a", encoding="utf-8") as f:
        print(transcription, file=f)
        print(f"Transcription written to: {output_file}")

print(f"Outputs are {output_files}")
concat_file = os.path.dirname(output_files[0]) + "/output_file.txt"

with open(concat_file,'wb') as wfd:
    for f in output_files:
        with open(f,'rb') as fd:
            shutil.copyfileobj(fd, wfd)