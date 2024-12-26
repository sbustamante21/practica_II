import os
import subprocess
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# Function to process a single dataset with fastp
def process_fastq(dataset_dir, fastq_files, delete_original, threads):
    try:
        processed_files = []
        if len(fastq_files) == 2:
            # Paired-end processing
            fastq1, fastq2 = sorted(fastq_files)
            output1 = os.path.join(dataset_dir, os.path.basename(fastq1).replace(".fastq", "_clean.fastq"))
            output2 = os.path.join(dataset_dir, os.path.basename(fastq2).replace(".fastq", "_clean.fastq"))
            subprocess.run(
                [
                    "fastp",
                    "-i", fastq1,
                    "-I", fastq2,
                    "-o", output1,
                    "-O", output2,
                    "-h", os.path.join(dataset_dir, "fastp_report.html"),
                    "-j", os.path.join(dataset_dir, "fastp_report.json"),
                    "-w", str(threads),
                ],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            processed_files.extend([fastq1, fastq2])
            return f"Processed paired-end FASTQ files in {dataset_dir}"
        elif len(fastq_files) == 1:
            # Single-end processing
            fastq = fastq_files[0]
            output = os.path.join(dataset_dir, os.path.basename(fastq).replace(".fastq", "_clean.fastq"))
            subprocess.run(
                [
                    "fastp",
                    "-i", fastq,
                    "-o", output,
                    "-h", os.path.join(dataset_dir, "fastp_report.html"),
                    "-j", os.path.join(dataset_dir, "fastp_report.json"),
                    "-w", str(threads),
                ],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            processed_files.append(fastq)
            return f"Processed single-end FASTQ file in {dataset_dir}"
        else:
            return f"No valid FASTQ files found in {dataset_dir}"

    except subprocess.CalledProcessError as e:
        return f"Error processing FASTQ files in {dataset_dir}: {e}"
    finally:
        # Delete original files if requested
        if delete_original and processed_files:
            for file in processed_files:
                try:
                    os.remove(file)
                except Exception as e:
                    print(f"Error deleting {file}: {e}")

# Main logic for concurrent processing
def process_fastq_concurrently(dataset_dirs, max_workers=10, delete_original=False, threads=4):
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_dir = {
            executor.submit(process_fastq, dataset_dir, fastq_files, delete_original, threads): dataset_dir
            for dataset_dir, fastq_files in dataset_dirs.items()
        }
        for future in as_completed(future_to_dir):
            dataset_dir = future_to_dir[future]
            try:
                results.append(future.result())
            except Exception as e:
                results.append(f"Error processing {dataset_dir}: {e}")
    return results

# Main function
def main():
    parser = argparse.ArgumentParser(description="Run fastp on FASTQ datasets.")
    parser.add_argument(
        "root_dir",
        help="Root directory containing FASTQ datasets.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=10,
        help="Maximum number of concurrent fastp processes (default: 10).",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=4,
        help="Number of threads for each fastp process (default: 4).",
    )
    parser.add_argument(
        "-d",
        "--delete",
        action="store_true",
        help="Delete original FASTQ files after processing.",
    )
    args = parser.parse_args()

    # Collect dataset directories and their FASTQ files
    dataset_dirs = {}
    for root, dirs, files in os.walk(args.root_dir):
        fastq_files = [os.path.join(root, file) for file in files if file.endswith(".fastq")]
        if fastq_files:
            dataset_dirs[root] = fastq_files

    if not dataset_dirs:
        print("No FASTQ files found.")
        return

    # Start processing
    print("Starting fastp processing...")
    results = process_fastq_concurrently(dataset_dirs, args.max_workers, args.delete, args.threads)
    for result in results:
        print(result)

if __name__ == "__main__":
    main()
