import os
import subprocess
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# Function to run HISAT2 and subsequent BAM processing
def run_hisat2_and_process(dataset_dir, fastq_files, index_path, threads, delete_fastq, known_splicesite_infile):
    try:
        # Use the dataset name (assumes directory name as dataset name)
        dataset_name = os.path.basename(dataset_dir)
        sorted_bam = os.path.join(dataset_dir, f"{dataset_name}_sorted.bam")
        log_file = os.path.join(dataset_dir, f"{dataset_name}_hisat2.log")

        # Prepare the HISAT2 command
        hisat2_command = [
            "hisat2",
            "-x", index_path,
            "-S", "-",  # Output directly to stdout
            "-a",
            "-p", str(threads),
        ]

        # Add the known splicesite infile if provided
        if known_splicesite_infile:
            hisat2_command.extend(["--known-splicesite-infile", known_splicesite_infile])

        if len(fastq_files) == 2:
            # Paired-end alignment
            fastq1, fastq2 = sorted(fastq_files)
            hisat2_command.extend(["-1", fastq1, "-2", fastq2])
        elif len(fastq_files) == 1:
            # Single-end alignment
            hisat2_command.extend(["-U", fastq_files[0]])
        else:
            return f"No valid FASTQ files found for alignment in {dataset_dir}"

        # Open log file for HISAT2 output
        with open(log_file, "w") as log:
            # Piping HISAT2 output to samtools sort
            hisat2_process = subprocess.Popen(
                hisat2_command,
                stdout=subprocess.PIPE,
                stderr=log,  # Save stderr to the log file
            )
            with open(sorted_bam, "wb") as bam_file:
                subprocess.run(
                    ["samtools", "sort", "-@", str(threads), "-o", sorted_bam],
                    stdin=hisat2_process.stdout,
                    stderr=log,  # Save samtools stderr to the log file
                    check=True,
                )
            hisat2_process.stdout.close()
            hisat2_process.wait()

        # Check for errors in HISAT2
        if hisat2_process.returncode != 0:
            return f"HISAT2 error in {dataset_dir}, see {log_file} for details."

        # Index the sorted BAM
        subprocess.run(
            ["samtools", "index", "-@", str(threads // 2), sorted_bam],
            check=True,
            stderr=open(log_file, "a"),  # Append samtools indexing stderr to the log file
        )

        # Delete FASTQ files after alignment if delete_fastq is True
        if delete_fastq:
            for fastq in fastq_files:
                os.remove(fastq)

        return f"Alignment completed for {dataset_dir}: {sorted_bam}, index, and log file are available."
    except subprocess.CalledProcessError as e:
        return f"Error during processing in {dataset_dir}: {e}"
    except OSError as e:
        return f"Error deleting files in {dataset_dir}: {e}"

# Main logic for concurrent processing
def run_hisat2_concurrently(dataset_dirs, index_path, threads, max_workers=10, delete_fastq=False, known_splicesite_infile=None):
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_dir = {
            executor.submit(run_hisat2_and_process, dataset_dir, fastq_files, index_path, threads, delete_fastq, known_splicesite_infile): dataset_dir
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
    parser = argparse.ArgumentParser(description="Run HISAT2 alignment and BAM processing on FASTQ datasets.")
    parser.add_argument(
        "root_dir",
        help="Root directory containing FASTQ datasets.",
    )
    parser.add_argument(
        "index_path",
        help="Path to HISAT2 index (prefix of index files).",
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
        help="Maximum number of concurrent HISAT2 processes (default: 10).",
    )
    parser.add_argument(
        "--delete-fastq",
        action="store_true",
        help="Delete FASTQ files after alignment.",
    )
    parser.add_argument(
        "--known-splicesite-infile",
        type=str,
        help="Path to a known splicesite infile for HISAT2.",
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
    print("Starting HISAT2 alignment and BAM processing...")
    results = run_hisat2_concurrently(
        dataset_dirs, args.index_path, args.threads, args.max_workers, args.delete_fastq, args.known_splicesite_infile
    )
    for result in results:
        print(result)

if __name__ == "__main__":
    main()

