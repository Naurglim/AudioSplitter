# AudioSplitter

AudioSplitter is a Python project designed to extract audio files from videos, split them into smaller segments and transcribe them.

## Features

- Extract audio from video files
- Split audio files into smaller segments
- Transcribe them automatically using speech to text
- Support for various audio formats
- Easy to use command-line interface

## Requirements

- Python 3.x
- `pydub` library
- `ffmpeg` or `libav`

## Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/yourusername/AudioSplitter.git
    ```
2. Navigate to the project directory:
    ```sh
    cd AudioSplitter
    ```
3. Install the required dependencies:
    ```sh
    pip install -r requirements.txt
    ```

## Usage TODO

To split an audio file, use the following command:
```sh
python splitter.py input_file output_directory segment_length
```
- `input_file`: Path to the input audio file
- `output_directory`: Directory where the output segments will be saved
- `segment_length`: Length of each segment in seconds

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Acknowledgements

- [pydub](https://github.com/jiaaro/pydub)
- [ffmpeg](https://ffmpeg.org/)
