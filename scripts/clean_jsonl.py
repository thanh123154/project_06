#!/usr/bin/env python3
"""
Clean JSONL files by removing empty lines and fixing JSON format issues.
"""
import json
import argparse
import sys
from pathlib import Path


def clean_jsonl(input_file: str, output_file: str, validate_only: bool = False) -> None:
    """Clean JSONL file by removing empty lines and fixing common JSON issues."""
    input_path = Path(input_file)
    if not input_path.exists():
        print(
            f"Error: Input file {input_file} does not exist", file=sys.stderr)
        sys.exit(1)

    valid_lines = 0
    invalid_lines = 0
    empty_lines = 0

    with open(input_path, 'r', encoding='utf-8') as infile:
        if not validate_only:
            outfile = open(output_file, 'w', encoding='utf-8')

        for line_num, line in enumerate(infile, 1):
            line = line.strip()

            # Skip empty lines
            if not line:
                empty_lines += 1
                continue

            # Validate JSON
            try:
                json.loads(line)
                valid_lines += 1
                if not validate_only:
                    outfile.write(line + '\n')
            except json.JSONDecodeError as e:
                invalid_lines += 1
                print(f"Line {line_num}: Invalid JSON - {e}")
                print(
                    f"Content: {line[:100]}{'...' if len(line) > 100 else ''}")

                # Try to fix common issues
                if not validate_only:
                    try:
                        # Try to fix common JSON issues
                        # Replace single quotes
                        fixed_line = line.replace("'", '"')
                        json.loads(fixed_line)
                        outfile.write(fixed_line + '\n')
                        valid_lines += 1
                        print(f"Line {line_num}: Fixed and included")
                    except json.JSONDecodeError:
                        print(f"Line {line_num}: Could not fix, skipping")

        if not validate_only:
            outfile.close()

    print(f"\nSummary:")
    print(f"Valid lines: {valid_lines}")
    print(f"Invalid lines: {invalid_lines}")
    print(f"Empty lines: {empty_lines}")
    print(
        f"Total lines processed: {valid_lines + invalid_lines + empty_lines}")

    if validate_only:
        if invalid_lines > 0:
            print(
                f"\nFile has {invalid_lines} invalid JSON lines. Run without --validate-only to clean.")
            sys.exit(1)
        else:
            print("File is clean!")


def main():
    parser = argparse.ArgumentParser(description="Clean JSONL files")
    parser.add_argument("input_file", help="Input JSONL file")
    parser.add_argument(
        "-o", "--output", help="Output file (default: input_file_clean.jsonl)")
    parser.add_argument("--validate-only", action="store_true",
                        help="Only validate, don't create output file")

    args = parser.parse_args()

    if args.validate_only:
        output_file = None
    else:
        if args.output:
            output_file = args.output
        else:
            input_path = Path(args.input_file)
            output_file = str(input_path.parent /
                              f"{input_path.stem}_clean{input_path.suffix}")

    clean_jsonl(args.input_file, output_file, args.validate_only)


if __name__ == "__main__":
    main()
