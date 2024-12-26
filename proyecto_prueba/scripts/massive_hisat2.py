import os
import subprocess
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# Function to run HISAT2 and subsequent BAM processing
def run_hisat2_and_process(dataset_dir, fastq_files, index_path, threads):
    try:
        output_sam = os.path.join(dataset_dir, "aligned.sam")
        aligned_bam = os.path.join(dataset_dir, "aligned.bam")
        filtered_bam = os.path.join(dataset_dir, "filtered.bam")
        filtered_sorted_bam = os.path.join(dataset_dir, "filtered_sorted.bam")

        # Perform alignment with HISAT2
        if len(fastq_files) == 2:
            # Paired-end alignment
            fastq1, fastq2 = sorted(fastq_files)  # Ensure correct pairing
            subprocess.run(
                [
                    "hisat2",
                    "-x", index_path,
                    "-1", fastq1,
                    "-2", fastq2,
                    "-S", output_sam,
                    "-a",
                    "-p", str(threads),
                ],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        elif len(fastq_files) == 1:
            # Single-end alignment
            fastq = fastq_files[0]
            subprocess.run(
                [
                    "hisat2",
                    "-x", index_path,
                    "-U", fastq,
                    "-S", output_sam,
                    "-a",
                    "-p", str(threads),
                ],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        else:
            return f"No valid FASTQ files found for alignment in {dataset_dir}"

        # Convert SAM to BAM
        subprocess.run(
            [
                "samtools", "view",
                "-@", str(threads),
                "-b",
                "-o", aligned_bam,
                output_sam
            ],
            check=True,
        )

        # Delete the original SAM file
        os.remove(output_sam)

        # Step 1: Filter BAM by quality
        subprocess.run(
            [
                "samtools", "view",
                "-@", str(threads),
                "-q", "2",
                "-o", filtered_bam,
                aligned_bam
            ],
            check=True,
        )

        # Step 2: Sort BAM
        subprocess.run(
            [
                "samtools", "sort",
                "-@", str(threads),
                "-o", filtered_sorted_bam,
                filtered_bam
            ],
            check=True,
        )

        # Delete the intermediate filtered.bam to save space
        os.remove(filtered_bam)

        # Step 3: Index the sorted BAM
        subprocess.run(
            [
                "samtools", "index",
                "-@", str(threads // 2),  # Use half the threads for indexing
                filtered_sorted_bam
            ],
            check=True,
        )

        return f"Alignment completed for {dataset_dir}: unfiltered BAM and filtered_sorted BAM are available"
    except subprocess.CalledProcessError as e:
        return f"Error during processing in {dataset_dir}: {e}"
    except OSError as e:
        return f"Error deleting intermediate files in {dataset_dir}: {e}"

# Main logic for concurrent processing
def run_hisat2_concurrently(dataset_dirs, index_path, threads, max_workers=10):
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_dir = {
            executor.submit(run_hisat2_and_process, dataset_dir, fastq_files, index_path, threads): dataset_dir
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
    results = run_hisat2_concurrently(dataset_dirs, args.index_path, args.threads, args.max_workers)
    for result in results:
        print(result)

if __name__ == "__main__":
    main()
