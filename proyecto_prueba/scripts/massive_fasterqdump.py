import os
import subprocess
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# Function to process a single SRA file
def process_sra(sra_path, delete_sra):
    try:
        sra_dir, sra_file = os.path.split(sra_path)

        # Run fasterq-dump in the same directory as the .sra file
        subprocess.run(
            ["fasterq-dump", sra_path, "-O", sra_dir],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Delete the .sra file if requested
        if delete_sra:
            os.remove(sra_path)

        return f"Processed {sra_path} successfully."
    except subprocess.CalledProcessError as e:
        return f"Error processing {sra_path}: {e}"
    except Exception as e:
        return f"Error deleting {sra_path}: {e}"

# Main logic for concurrent processing
def process_sra_concurrently(sra_paths, max_workers, delete_sra):
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_sra = {
            executor.submit(process_sra, sra_path, delete_sra): sra_path for sra_path in sra_paths
        }
        for future in as_completed(future_to_sra):
            sra_path = future_to_sra[future]
            try:
                results.append(future.result())
            except Exception as e:
                results.append(f"Error processing {sra_path}: {e}")
    return results

# Main function
def main():
    parser = argparse.ArgumentParser(description="Process SRA files and save FASTQ in the same directory.")
    parser.add_argument(
        "root_dir",
        help="Root directory containing nested directories with SRA files.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=20,
        help="Maximum number of concurrent processes (default: 20).",
    )
    parser.add_argument(
        "-d",
        "--delete",
        action="store_true",
        help="Delete original .sra files after processing.",
    )
    args = parser.parse_args()

    # Find all .sra files in nested directories
    sra_paths = []
    for root, dirs, files in os.walk(args.root_dir):
        for file in files:
            if file.endswith(".sra"):
                sra_paths.append(os.path.join(root, file))

    if not sra_paths:
        print("No .sra files found.")
        return

    # Start processing
    print("Starting concurrent processing of SRA files...")
    results = process_sra_concurrently(sra_paths, args.max_workers, args.delete)
    for result in results:
        print(result)

if __name__ == "__main__":
    main()
