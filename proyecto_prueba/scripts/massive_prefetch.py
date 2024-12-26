import os
import subprocess
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# Function to download a single SRA dataset using prefetch
def download_sra(sra_id, output_dir):
    try:
        subprocess.run(
            ["prefetch", sra_id, "-O", output_dir, "-C", "yes", "-X", "100G"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return f"Download successful for {sra_id}"
    except subprocess.CalledProcessError as e:
        return f"Download failed for {sra_id}: {e}"

# Main logic for concurrent downloading
def download_sra_concurrently(sra_list, output_dir, max_workers=20):
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_sra = {
            executor.submit(download_sra, sra_id, output_dir): sra_id
            for sra_id in sra_list
        }
        for future in as_completed(future_to_sra):
            sra_id = future_to_sra[future]
            try:
                results.append(future.result())
            except Exception as e:
                results.append(f"Error downloading {sra_id}: {e}")
    return results

# Main function
def main():
    parser = argparse.ArgumentParser(description="Download SRA datasets concurrently.")
    parser.add_argument(
        "sra_file",
        help="Path to the file containing SRA run IDs (one per line).",
    )
    parser.add_argument(
        "output_dir",
        help="Directory to save the downloaded data.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=20,
        help="Maximum number of concurrent downloads (default: 20).",
    )
    args = parser.parse_args()

    # Ensure the output directory exists
    os.makedirs(args.output_dir, exist_ok=True)

    # Read the SRA list from the file
    with open(args.sra_file, "r") as file:
        sra_list = [line.strip() for line in file if line.strip()]

    # Start the downloads
    print("Starting concurrent downloads...")
    download_results = download_sra_concurrently(sra_list, args.output_dir, args.max_workers)
    for result in download_results:
        print(result)

if __name__ == "__main__":
    main()
