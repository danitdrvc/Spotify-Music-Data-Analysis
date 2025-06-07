# Spotify Music Data Analysis

This project analyzes a large dataset of Spotify tracks to uncover trends, patterns, and insights related to genre specialization, collaboration, commercial success, and more. The analysis is performed using PySpark for scalable data processing.

## Project Structure

- `main.py` - Entry point for running different analyses.
- `data_analysis.py` - General data validation and statistical analysis.
- `genre_specialization.py` - Analyzes artist genre diversity and popularity consistency.
- `collaboration.py` - Examines collaboration patterns among artists.
- `breakthrough.py` - Identifies breakthrough tracks and artists.
- `comercial_success.py` - Investigates factors contributing to commercial success.
- `correlation.py` - Explores correlations between track features.
- `tempo_sweet_spot.py` - Analyzes tempo and its relation to popularity.
- `valence.py` - Studies the emotional valence of tracks.
- `dataset.csv` - The main dataset containing Spotify track information.
- `spotify_distribution_analysis.json` - Output of distribution analysis.

## Requirements

- Python 3.8+
- PySpark

## Setup

1. Clone the repository:
    ```sh
    git clone https://github.com/danitdrvc/Spotify-Music-Data-Analysis.git
    cd spotify-music-analysis
    ```

2. Install dependencies:
    ```sh
    pip install pyspark
    ```

3. Place your `dataset.csv` file in the project root directory.

## Usage

You can run different analyses by uncommenting the relevant function calls in `main.py` and executing:

```sh
python main.py
```

Each analysis script can also be imported and used independently.

## Dataset

The dataset should be a CSV file with columns such as:
- `artists`
- `popularity`
- `track_genre`
- ...and other Spotify track features.

## Output

Results are printed to the console and, for some analyses, saved as JSON files (e.g., `spotify_distribution_analysis.json`).
