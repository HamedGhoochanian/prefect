import os
import json

import subprocess
from typing import List

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


def generate_cmd_results(min_tokens=50, output='reports/cpd_results.csv'):
    files = list_python_files(SRC)
    os.system(
        f"pmd cpd --language python --minimum-tokens {min_tokens} --format csv_with_linecount_per_file -d {' '.join(files)} > {output}")

    with open(output, "r") as f:
        lines = f.readlines()[1:]

    cleaned_lines = []

    for line in lines:
        parts = line.strip().split(",")

        occurrences = int(parts[1])
        for i in range(occurrences + 1):
            parts[1 + i * 3] = parts[1 + i * 3].replace(f"{PREFECT_PATH}/", "")

        cleaned_lines.append(",".join(parts))

    normalized_rows = []
    id = 1
    for line in cleaned_lines:
        parts = line.strip().split(",")
        token_count = int(parts[0])
        occurrences = int(parts[1])
        for i in range(1, occurrences + 1):
            path = parts[i * 3 + 1]
            line_count = int(parts[i * 3])
            start_line = int(parts[i * 3 - 1])
            normalized_rows.append([str(a) for a in [token_count, id, start_line, line_count, path]])
        id += 1
    result = "token_count,occurrence_id,start_line,line_count,path"
    for row in normalized_rows:
        result += "\n" + ",".join(row)
    with open(output, "w") as f:
        f.write(result)


def generate_maintainability(output="reports/maintainability_index.json"):
    os.system(f"radon mi --ignore tests --json --output-file {output} {PREFECT_PATH}")
    prettify_json(output)


def generate_mccabe(output="reports/mccabe.json"):
    os.system(f"radon cc --ignore tests --json --output-file {output} {PREFECT_PATH}")
    prettify_json(output)


def generate_halstead(output="reports/halstead.json"):
    os.system(f"radon hal --ignore tests --json --output-file {output} {PREFECT_PATH}")
    prettify_json(output)


if __name__ == "__main__":
    generate_cmd_results()
    generate_maintainability()
    generate_mccabe()
    generate_halstead()
    generate_churn_data()
