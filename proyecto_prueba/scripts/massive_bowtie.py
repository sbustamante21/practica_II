import os
import subprocess
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# Function to check if Bowtie output is empty
def is_bowtie_output_empty(bowtie_stdout):
    for line in bowtie_stdout.splitlines():
        if line.startswith("@"):  # SAM headers start with '@'
            return False
        if line.strip():  # Non-header, non-empty line (alignment)
            return False
    return True

# Function to run Bowtie and subsequent BAM processing
def run_bowtie_and_process(dataset_dir, fastq_files, index_path, threads, delete_fastq):
    try:
        # Use the dataset name (assumes directory name as dataset name)
        dataset_name = os.path.basename(dataset_dir)
        sorted_bam = os.path.join(dataset_dir, f"{dataset_name}_sorted.bam")
        log_file = os.path.join(dataset_dir, f"{dataset_name}_bowtie.log")

        with open(log_file, "w") as log:
            # Perform alignment with Bowtie
            if len(fastq_files) == 2:
                # Paired-end alignment
                fastq1, fastq2 = sorted(fastq_files)  # Ensure correct pairing
                bowtie_command = [
                    "bowtie",
                    "-p", str(threads),
                    "-x", index_path,
                    "-1", fastq1,
                    "-2", fastq2,
                    "-a",  # Report all alignments
                    "--sam",  # Output SAM format
                ]
            elif len(fastq_files) == 1:
                # Single-end alignment (interleaved reads)
                fastq = fastq_files[0]
                bowtie_command = [
                    "bowtie",
                    "-p", str(threads),
                    "-x", index_path,
                    "--interleaved", fastq,
                    "-a",  # Report all alignments
                    "--sam",  # Output SAM format
                ]
            else:
                return f"No valid FASTQ files found for alignment in {dataset_dir}"

            # Capture Bowtie output
            bowtie_process = subprocess.run(
                bowtie_command,
                stdout=subprocess.PIPE,
                stderr=log,
                text=True,
            )

            # Check if Bowtie output is empty
            if is_bowtie_output_empty(bowtie_process.stdout):
                return f"No alignments found for {dataset_dir}. Skipping BAM generation."

            # Convert Bowtie output (SAM) to sorted BAM
            with open(sorted_bam, "wb") as bam_file:
                subprocess.run(
                    ["samtools", "sort", "-@", str(threads), "-o", sorted_bam],
                    input=bowtie_process.stdout,
                    text=True,
                    stderr=log,
                    check=True,
                )

            # Index the sorted BAM
            subprocess.run(
                ["samtools", "index", "-@", str(threads // 2), sorted_bam],
                stderr=log,
                check=True,
            )

            # Delete FASTQ files after alignment if delete_fastq is True
            if delete_fastq:
                for fastq in fastq_files:
                    os.remove(fastq)

        return f"Alignment completed for {dataset_dir}: {sorted_bam} and its index are available."
    except subprocess.CalledProcessError as e:
        return f"Error during processing in {dataset_dir}: {e}"
    except OSError as e:
        return f"Error deleting files in {dataset_dir}: {e}"

# Main logic for concurrent processing
def run_bowtie_concurrently(dataset_dirs, index_path, threads, delete_fastq, max_workers=10):
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_dir = {
            executor.submit(run_bowtie_and_process, dataset_dir, fastq_files, index_path, threads, delete_fastq): dataset_dir
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
    parser = argparse.ArgumentParser(description="Run Bowtie alignment and BAM processing on FASTQ datasets.")
    parser.add_argument(
        "root_dir",
        help="Root directory containing FASTQ datasets.",
    )
    parser.add_argument(
        "index_path",
        help="Path to Bowtie index (prefix of index files).",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=4,
        help="Number of threads per alignment and processing (default: 4).",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=10,
        help="Maximum number of concurrent Bowtie processes (default: 10).",
    )
    parser.add_argument(
        "-d", "--delete-fastq",
        action="store_true",
        help="Delete FASTQ files after alignment.",
    )
    args = parser.parse_args()

    # Collect dataset directories and their `_clean.fastq` files
    dataset_dirs = {}
    for root, dirs, files in os.walk(args.root_dir):
        fastq_files = [os.path.join(root, file) for file in files if file.endswith("_clean.fastq")]
        if fastq_files:
            dataset_dirs[root] = fastq_files

    if not dataset_dirs:
        print("No `_clean.fastq` files found for alignment.")
        return

    # Start alignment and processing
    print("Starting Bowtie alignment and BAM processing...")
    results = run_bowtie_concurrently(dataset_dirs, args.index_path, args.threads, args.delete_fastq, args.max_workers)
    for result in results:
        print(result)

if __name__ == "__main__":
    main()


