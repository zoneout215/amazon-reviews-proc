import gzip
import json

import requests


def download_json_chunks(url: str, file_path_prefix: str, records_per_chunk: int):
    """
    Downloads a gzipped JSON file from a URL and saves it in chunks based on the number of JSON records.

    Args:
        url (str): The URL of the gzipped JSON file to download.
        file_path_prefix (str): The prefix for the chunk file names.
        records_per_chunk (int): The number of JSON records per chunk.
    """
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        chunk_index = 0
        record_count = 0
        chunk_data = []

        with gzip.GzipFile(fileobj=response.raw) as gz_file:
            for line in gz_file:
                record = json.loads(line)
                chunk_data.append(record)
                record_count += 1

                if record_count >= records_per_chunk:
                    chunk_file_path = f"{file_path_prefix}_chunk_{chunk_index}.json"
                    write_json_chunk_to_file(chunk_data, chunk_file_path)
                    chunk_index += 1
                    record_count = 0
                    chunk_data = []

            # Write any remaining records to a final chunk
            if chunk_data:
                chunk_file_path = f"{file_path_prefix}_chunk_{chunk_index}.json"
                write_json_chunk_to_file(chunk_data, chunk_file_path)

        print("\nDownload complete!")
        return chunk_index

    except requests.RequestException as e:
        print(f"Error downloading {url}: {e}")
        raise


def write_json_chunk_to_file(chunk_data: list, chunk_file_path: str):
    """
    Writes a list of JSON records to a file.

    Args:
        chunk_data (list): The list of JSON records to write.
        chunk_file_path (str): The path to the file where the chunk will be saved.
    """
    try:
        with open(chunk_file_path, "w") as f:
            json.dump(chunk_data, f)
    except OSError as e:
        print(f"Error writing chunk to {chunk_file_path}: {e}")
        raise


def main():
    url = "https://snap.stanford.edu/data/amazon/productGraph/item_dedup.json.gz"
    file_path_prefix = "test/item_dedup"
    records_per_chunk = 1000  # Adjust this value as needed

    download_json_chunks(url, file_path_prefix, records_per_chunk)


if __name__ == "__main__":
    main()
