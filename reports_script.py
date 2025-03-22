import os
import json
import csv
import subprocess
from typing import List
import pandas as pd

PREFECT_PATH = "/Users/hamed/Documents/0uni/08-software-evolution/assignment/prefect"
SRC = f"{PREFECT_PATH}/src"


def generate_churn_data(output="reports/code_churn.csv"):
    # Run git log to get numstat data
    cmd = "git log --no-merges --numstat --pretty=format:'%H'".split()
    result = subprocess.run(cmd, capture_output=True, text=True)
    lines = result.stdout.split("\n")

    # Parse data into a DataFrame
    data = []
    current_hash = None
    for line in lines:
        if len(line) == 40:  # Commit hash
            current_hash = line
        elif "\t" in line:  # Numstat line (additions, deletions, file)
            additions, deletions, file = line.split("\t")

            # --- FILTER FILES ---
            # Exclude files outside the 'src' directory
            if not file.startswith("src/"):
                continue

            # Exclude test files (e.g., tests/, *_test.py, test_*.py)
            if "tests/" in file or "test_" in file or "_test.py" in file:
                continue

            # Exclude migration files (e.g., migrations/, *_migration.py)
            if "migrations/" in file or "_migration.py" in file or "_migration" in file:
                continue

            # Add to data
            data.append(
                {
                    "file": file,
                    "commits": 1,
                    "lines_added": int(additions) if additions.isdigit() else 0,
                    "lines_removed": int(deletions) if deletions.isdigit() else 0,
                }
            )

    # Aggregate by file
    df = pd.DataFrame(data)
    if not df.empty:
        churn_df = (
            df.groupby("file")
            .agg({"commits": "sum", "lines_added": "sum", "lines_removed": "sum"})
            .reset_index()
        )
    else:
        churn_df = pd.DataFrame(
            columns=["file", "commits", "lines_added", "lines_removed"]
        )
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


def generate_cmd_results(output="reports/cpd_results.csv", min_tokens=50):
    files = list_python_files(SRC)
    os.system(
        f"pmd cpd --language python --minimum-tokens {min_tokens} --format csv_with_linecount_per_file -d {' '.join(files)} > {output}"
    )

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
            normalized_rows.append(
                [str(a) for a in [token_count, id, start_line, line_count, path]]
            )
        id += 1
    result = "token_count,occurrence_id,start_line,line_count,path"
    for row in normalized_rows:
        result += "\n" + ",".join(row)
    with open(output, "w") as f:
        f.write(result)


def convert_maintainability_json_to_csv(json_file):
    with open(json_file, "r") as f:
        data = json.load(f)

    with open(json_file.replace(".json", ".csv"), "w", newline="") as csvfile:
        fieldnames = ["file", "mi", "rank"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()

        for file, metrics in data.items():
            writer.writerow(
                {"file": file, "mi": metrics["mi"], "rank": metrics["rank"]}
            )


def convert_mccabe_json_to_csv(json_file):
    with open(json_file, "r") as file:
        data = json.load(file)

    # Prepare data for CSV
    rows = []

    for file, items in data.items():
        for item in items:
            row = {
                "file": file,
                "type": item.get("type", ""),
                "rank": item.get("rank", ""),
                "complexity": item.get("complexity", ""),
                "col_offset": item.get("col_offset", ""),
                "lineno": item.get("lineno", ""),
                "endline": item.get("endline", ""),
                "name": item.get("name", ""),
            }
            rows.append(row)

    # Convert to DataFrame
    df = pd.DataFrame(rows)

    # Save to CSV
    csv_file_path = json_file.replace(".json", ".csv")
    df.to_csv(csv_file_path, index=False)


def convert_halstead_json_to_csv(json_file):
    with open(json_file, "r") as file:
        data = json.load(file)

    # Prepare data for CSV
    rows = []
    for file, metrics in data.items():
        metrics = metrics.get("total", {})
        row = {
            "file": file,
            "h1": metrics.get("h1", ""),
            "h2": metrics.get("h2", ""),
            "N1": metrics.get("N1", ""),
            "N2": metrics.get("N2", ""),
            "vocabulary": metrics.get("vocabulary", ""),
            "length": metrics.get("length", ""),
            "calculated_length": metrics.get("calculated_length", ""),
            "volume": metrics.get("volume", ""),
            "difficulty": metrics.get("difficulty", ""),
            "effort": metrics.get("effort", ""),
            "time": metrics.get("time", ""),
            "bugs": metrics.get("bugs", ""),
        }
        rows.append(row)
    df = pd.DataFrame(rows)
    csv_file_path = json_file.replace(".json", ".csv")
    df.to_csv(csv_file_path, index=False)


def generate_maintainability(output="reports/maintainability_index.json"):
    os.system(f"radon mi --ignore tests --json --output-file {output} {SRC}")
    prettify_json(output)
    convert_maintainability_json_to_csv(output)


def generate_mccabe(output="reports/mccabe.json"):
    os.system(f"radon cc --ignore tests --json --output-file {output} {SRC}")
    prettify_json(output)
    convert_mccabe_json_to_csv(output)


def generate_halstead(output="reports/halstead.json"):
    os.system(f"radon hal --ignore tests --json --output-file {output} {SRC}")
    prettify_json(output)
    convert_halstead_json_to_csv(output)


def generate_lizard(output="reports/lizard.csv"):
    os.system(f"echo 'NLOC,CCN,token,param,length,location' > {output}")
    os.system(f"lizard --csv {SRC}>> {output}")
    with open(output, "r") as f:
        data = f.readlines()
    data = [line.replace(f"{PREFECT_PATH}/", "") for line in data]
    with open(output, "w") as f:
        f.writelines(data)


if __name__ == "__main__":
    generate_lizard("reports/lizard.csv")
    print("lizard generated")
    generate_cmd_results("reports/cpd.csv", min_tokens=50)
    print("cmd results generated")
    generate_maintainability("reports/maintainability_index.json")
    print("maintainability generated")
    generate_mccabe("reports/mccabe.json")
    print("mccabe generated")
    generate_halstead("reports/halstead.json")
    print("halstead generated")
    generate_churn_data("reports/code_churn.csv")
    print("churn data generated")
