import os
import sys
import json

import subprocess
from pprint import pprint
from typing import List
from collections import defaultdict

import pandas as pd

PREFECT_PATH = "/home/hamed/workspace/prefect"
SRC = f"{PREFECT_PATH}/src"


def generate_churn_data(output="reports/code_churn.csv"):
    # Run git log to get numstat data
    cmd = "git log --no-merges --numstat --pretty=format:'%H'".split()
    result = subprocess.run(cmd, capture_output=True, text=True)
    lines = result.stdout.split('\n')

    # Parse data into a DataFrame
    data = []
    current_hash = None
    for line in lines:
        if len(line) == 40:  # Commit hash
            current_hash = line
        elif '\t' in line:  # Numstat line (additions, deletions, file)
            additions, deletions, file = line.split('\t')

            # --- FILTER FILES ---
            # Exclude files outside the 'src' directory
            if not file.startswith('src/'):
                continue

            # Exclude test files (e.g., tests/, *_test.py, test_*.py)
            if 'tests/' in file or 'test_' in file or '_test.py' in file:
                continue

            # Exclude migration files (e.g., migrations/, *_migration.py)
            if 'migrations/' in file or '_migration.py' in file or '_migration' in file:
                continue

            # Add to data
            data.append({
                'file': file,
                'commits': 1,
                'lines_added': int(additions) if additions.isdigit() else 0,
                'lines_removed': int(deletions) if deletions.isdigit() else 0
            })

    # Aggregate by file
    df = pd.DataFrame(data)
    if not df.empty:
        churn_df = df.groupby('file').agg({
            'commits': 'sum',
            'lines_added': 'sum',
            'lines_removed': 'sum'
        }).reset_index()
    else:
        churn_df = pd.DataFrame(columns=['file', 'commits', 'lines_added', 'lines_removed'])
    churn_df.to_csv(output, index=False)


def list_python_files(directory):
    python_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".py"):
                full_path = os.path.join(root, file).lower()
                if (
                        "test" not in full_path
                        and "__pycache__" not in full_path
                        and "_migration" not in full_path
                ):
                    python_files.append(os.path.join(root, file))
    return python_files


def prettify_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        data: dict = json.load(f)

    old_keys: List[str] = list(data.keys())
    for key in old_keys:
        data[key.replace(f"{PREFECT_PATH}/", "")] = data.pop(key)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


def process_cpd_csv(file_path):
    """
    Process the CSV output from CPD and normalize it into rows.
    Expected CSV format: header line then each line with:
    token_count,occurrences,<start_line1>,<line_count1>,<path1>,<start_line2>,<line_count2>,<path2>,...
    """
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    if not lines:
        return []
    # Remove header line
    data_lines = lines[1:]
    normalized_rows = []
    clone_block_id = 1
    for line in data_lines:
        parts = line.strip().split(",")
        if len(parts) < 2:
            continue
        token_count = int(parts[0])
        occurrences = int(parts[1])
        # Each occurrence adds three fields: start_line, line_count, path
        for i in range(1, occurrences + 1):
            try:
                start_line = int(parts[i * 3 - 1])
                line_count = int(parts[i * 3])
                # Remove the PREFECT_PATH prefix from the path for clarity
                relative_path = parts[i * 3 + 1].replace(f"{PREFECT_PATH}\\", "")
                normalized_rows.append([token_count, clone_block_id, start_line, line_count, relative_path])
            except (IndexError, ValueError):
                continue
        clone_block_id += 1
    return normalized_rows


def print_most_common_clone(normalized_rows, title):
    """
    Finds and prints details (including code snippet) for the clone block that has the most occurrences.
    """
    blocks = defaultdict(list)
    for row in normalized_rows:
        # row: [token_count, clone_block_id, start_line, line_count, relative_path]
        blocks[row[1]].append(row)

    if not blocks:
        print(f"No clone blocks found for {title}.")
        return

    # Find the clone block with the highest number of occurrences.
    most_common_block_id, occurrences = max(blocks.items(), key=lambda item: len(item[1]))

    print(
        f"\nMost Common {title} Clone Block (Block ID: {most_common_block_id}, Occurrences: {len(occurrences)}, Token Count: {occurrences[0][0]}):")
    for occ in occurrences:
        token_count, clone_block_id, start_line, line_count, rel_path = occ
        abs_path = os.path.join(PREFECT_PATH, rel_path)
        print(f"\nFile: {abs_path} (Lines {start_line} to {start_line + line_count - 1}):")
        try:
            with open(abs_path, "r", encoding="utf-8") as f:
                file_lines = f.readlines()
            # Adjust for 0-based indexing.
            snippet = "".join(file_lines[start_line - 1: start_line - 1 + line_count])
            print("Snippet:")
            print(snippet)
        except Exception as e:
            print(f"Error reading file: {e}")


def generate_cpd_results(min_tokens=50,
                         output_type1='reports/cpd_type1_results.csv',
                         output_type2='reports/cpd_type2_results.csv'):
    # Ensure the reports directory exists.
    os.makedirs("reports", exist_ok=True)

    # Use the source directory directly.
    source_dir = SRC
    print("Analyzing source directory:", source_dir)

    # --- Type 1 clones: exact matches ---
    cmd_type1 = (f'pmd cpd --language python --minimum-tokens {min_tokens} '
                 f'--format csv_with_linecount_per_file -d "{source_dir}" > {output_type1}')
    print("Running Type 1 CPD command:")
    print(cmd_type1)
    ret1 = os.system(cmd_type1)
    if ret1 not in (0, 4):
        print("Error running CPD for Type 1 clones. Return code:", ret1)
    else:
        print(f"Type 1 clone results written to {output_type1}")

    # --- Type 2 clones: ignoring literals and identifiers ---
    cmd_type2 = (f'pmd cpd --language python --minimum-tokens {min_tokens} '
                 f'--ignore-literals --ignore-identifiers --format csv_with_linecount_per_file '
                 f'-d "{source_dir}" > {output_type2}')
    print("Running Type 2 CPD command:")
    print(cmd_type2)
    ret2 = os.system(cmd_type2)
    if ret2 not in (0, 4):
        print("Error running CPD for Type 2 clones. Return code:", ret2)
    else:
        print(f"Type 2 clone results written to {output_type2}")

    # --- Process and normalize the CSV outputs ---
    normalized_type1 = process_cpd_csv(output_type1)
    normalized_type2 = process_cpd_csv(output_type2)

    # Overwrite the CSVs with normalized data.
    header = "token_count,occurrence_id,start_line,line_count,path\n"
    with open(output_type1, "w", encoding="utf-8") as f:
        f.write(header)
        for row in normalized_type1:
            f.write(",".join(map(str, row)) + "\n")

    with open(output_type2, "w", encoding="utf-8") as f:
        f.write(header)
        for row in normalized_type2:
            f.write(",".join(map(str, row)) + "\n")

    print("Type 1 Clones Count:", len(normalized_type1))
    print("Type 2 Clones Count:", len(normalized_type2))

    # --- Print the most common clone block for each type ---
    print_most_common_clone(normalized_type1, "Type 1")
    print_most_common_clone(normalized_type2, "Type 2")

def generate_maintainability(output="reports/maintainability_index.json"):
    os.system(f"radon mi --ignore tests --json --output-file {output} {PREFECT_PATH}")
    prettify_json(output)
    import json
    import csv

    # Read the JSON file
    with open('reports/maintainability_index.json', 'r') as f:
        data = json.load(f)

    # Open a CSV file for writing
    with open(output.replace('.json','.csv'), 'w', newline='') as csvfile:
        fieldnames = ['file', 'mi', 'rank']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Write the header
        writer.writeheader()

        # Write the data
        for file, metrics in data.items():
            writer.writerow({'file': file, 'mi': metrics['mi'], 'rank': metrics['rank']})


def generate_mccabe(output="reports/mccabe.json"):
    os.system(f"radon cc --ignore tests --json --output-file {output} {PREFECT_PATH}")
    prettify_json(output)


def generate_halstead(output="reports/halstead.json"):
    os.system(f"radon hal --ignore tests --json --output-file {output} {PREFECT_PATH}")
    prettify_json(output)


if __name__ == "__main__":
    generate_cpd_results()
    generate_maintainability()
    generate_mccabe()
    generate_halstead()
    generate_churn_data()
