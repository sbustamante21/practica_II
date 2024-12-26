import os
import subprocess
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# Function to run FastQC on a single FASTQ file
def run_fastqc(fastq_path, output_dir):
    try:
        subprocess.run(
            ["fastqc", "-t", "2", "-o", output_dir, fastq_path],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return f"FastQC completed for {fastq_path}"
    except subprocess.CalledProcessError as e:
        return f"FastQC failed for {fastq_path}: {e}"

# Main logic for concurrent processing
def run_fastqc_concurrently(fastq_paths, output_dir, max_workers=10):
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_fastq = {
            executor.submit(run_fastqc, fastq_path, output_dir): fastq_path
            for fastq_path in fastq_paths
        }
        for future in as_completed(future_to_fastq):
            fastq_path = future_to_fastq[future]
            try:
                results.append(future.result())
            except Exception as e:
                results.append(f"Error processing {fastq_path}: {e}")
    return results

# Main function
def main():
    parser = argparse.ArgumentParser(description="Run FastQC on specified FASTQ files.")
    parser.add_argument(
        "root_dir",
        help="Root directory containing FASTQ files.",
    )
    parser.add_argument(
        "output_dir",
        help="Directory to save FastQC outputs.",
    )
    parser.add_argument(
        "file_suffix",
        help="File suffix to filter (e.g., _clean.fastq, .fastq).",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=10,
        help="Maximum number of concurrent FastQC processes (default: 10).",
    )
    args = parser.parse_args()

    # Ensure the output directory exists
    os.makedirs(args.output_dir, exist_ok=True)

    # Find all files with the specified suffix in the root directory
    fastq_paths = []
    for root, dirs, files in os.walk(args.root_dir):
        for file in files:
            if file.endswith(args.file_suffix):
                fastq_paths.append(os.path.join(root, file))

    if not fastq_paths:
        print(f"No files found with suffix '{args.file_suffix}'.")
        return

    # Start FastQC processing
    print(f"Starting FastQC analysis on files with suffix '{args.file_suffix}'...")
    results = run_fastqc_concurrently(fastq_paths, args.output_dir, args.max_workers)
    for result in results:
        print(result)

if __name__ == "__main__":
    main()
