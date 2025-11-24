#!/usr/bin/env python3
"""
CSV Splitter - Split CSV files by size or line count

Usage:
    python split.py -i <input.csv> -s 3m     # Split by size (1k, 1m, 1g)
    python split.py -i <input.csv> -l 1000   # Split by line count
"""

import argparse
import os
import sys
import re


def parse_size(size_str: str) -> int:
    """Parse size string like '1k', '3m', '1g' to bytes."""
    size_str = size_str.strip().lower()
    match = re.match(r'^(\d+(?:\.\d+)?)\s*([kmgb]?)b?$', size_str)
    if not match:
        raise ValueError(f"Invalid size format: {size_str}. Use format like 1k, 3m, 1g, 1kb, 3mb")

    num = float(match.group(1))
    unit = match.group(2)

    multipliers = {
        '': 1,
        'b': 1,
        'k': 1024,
        'm': 1024 * 1024,
        'g': 1024 * 1024 * 1024,
    }

    return int(num * multipliers.get(unit, 1))


def get_output_filename(input_file: str, index: int) -> str:
    """Generate output filename like singapore_1.csv from singapore.csv"""
    base, ext = os.path.splitext(input_file)
    return f"{base}_{index}{ext}"


def split_by_size(input_file: str, max_size: int) -> list[str]:
    """Split CSV file by size, preserving header in each chunk."""
    output_files = []

    with open(input_file, 'r', encoding='utf-8') as f:
        header = f.readline()
        header_size = len(header.encode('utf-8'))

        file_index = 1
        current_size = header_size
        current_lines = [header]

        for line in f:
            line_size = len(line.encode('utf-8'))

            # If adding this line exceeds max_size, write current chunk
            if current_size + line_size > max_size and len(current_lines) > 1:
                output_file = get_output_filename(input_file, file_index)
                with open(output_file, 'w', encoding='utf-8') as out:
                    out.writelines(current_lines)
                output_files.append(output_file)
                print(f"  Created: {output_file} ({current_size:,} bytes, {len(current_lines)-1:,} rows)")

                file_index += 1
                current_size = header_size
                current_lines = [header]

            current_lines.append(line)
            current_size += line_size

        # Write remaining lines
        if len(current_lines) > 1:
            output_file = get_output_filename(input_file, file_index)
            with open(output_file, 'w', encoding='utf-8') as out:
                out.writelines(current_lines)
            output_files.append(output_file)
            print(f"  Created: {output_file} ({current_size:,} bytes, {len(current_lines)-1:,} rows)")

    return output_files


def split_by_count(input_file: str, num_files: int) -> list[str]:
    """Split CSV file into exactly N files."""
    output_files = []

    # Count total lines first
    with open(input_file, 'r', encoding='utf-8') as f:
        header = f.readline()
        total_lines = sum(1 for _ in f)

    if total_lines == 0:
        print("  Warning: No data rows in file")
        return []

    # Calculate lines per file (distribute evenly)
    base_lines = total_lines // num_files
    extra_lines = total_lines % num_files  # First N files get 1 extra line

    with open(input_file, 'r', encoding='utf-8') as f:
        header = f.readline()

        for file_index in range(1, num_files + 1):
            # First 'extra_lines' files get one more line
            lines_for_this_file = base_lines + (1 if file_index <= extra_lines else 0)

            if lines_for_this_file == 0:
                continue

            current_lines = [header]
            for _ in range(lines_for_this_file):
                line = f.readline()
                if line:
                    current_lines.append(line)

            if len(current_lines) > 1:
                output_file = get_output_filename(input_file, file_index)
                with open(output_file, 'w', encoding='utf-8') as out:
                    out.writelines(current_lines)
                output_files.append(output_file)
                print(f"  Created: {output_file} ({len(current_lines)-1:,} rows)")

    return output_files


def split_by_lines(input_file: str, max_lines: int) -> list[str]:
    """Split CSV file by line count, preserving header in each chunk."""
    output_files = []

    with open(input_file, 'r', encoding='utf-8') as f:
        header = f.readline()

        file_index = 1
        current_lines = [header]
        line_count = 0

        for line in f:
            current_lines.append(line)
            line_count += 1

            if line_count >= max_lines:
                output_file = get_output_filename(input_file, file_index)
                with open(output_file, 'w', encoding='utf-8') as out:
                    out.writelines(current_lines)
                output_files.append(output_file)
                print(f"  Created: {output_file} ({line_count:,} rows)")

                file_index += 1
                current_lines = [header]
                line_count = 0

        # Write remaining lines
        if len(current_lines) > 1:
            output_file = get_output_filename(input_file, file_index)
            with open(output_file, 'w', encoding='utf-8') as out:
                out.writelines(current_lines)
            output_files.append(output_file)
            print(f"  Created: {output_file} ({line_count:,} rows)")

    return output_files


def main():
    parser = argparse.ArgumentParser(
        description='Split CSV files by size or line count',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python split.py -i data.csv -s 3m      # Split into ~3MB chunks
  python split.py -i data.csv -s 500k    # Split into ~500KB chunks
  python split.py -i data.csv -l 1000    # Split into 1000 lines per file
  python split.py -i data.csv -l 5000    # Split into 5000 lines per file
  python split.py -i data.csv -n 5       # Split into exactly 5 files
  python split.py -i data.csv -n 10      # Split into exactly 10 files
        '''
    )

    parser.add_argument('-i', '--input', required=True, help='Input CSV file')
    parser.add_argument('-s', '--size', help='Split by size (e.g., 1k, 3m, 1g)')
    parser.add_argument('-l', '--lines', type=int, help='Split by line count')
    parser.add_argument('-n', '--num', type=int, help='Split into N files')

    args = parser.parse_args()

    # Validate input file
    if not os.path.exists(args.input):
        print(f"Error: Input file '{args.input}' not found")
        sys.exit(1)

    if not args.input.lower().endswith('.csv'):
        print("Warning: Input file does not have .csv extension")

    # Validate arguments
    options_count = sum([bool(args.size), bool(args.lines), bool(args.num)])
    if options_count == 0:
        print("Error: Must specify one of -s (size), -l (lines), or -n (num files)")
        parser.print_help()
        sys.exit(1)

    if options_count > 1:
        print("Error: Cannot specify multiple split methods. Choose one: -s, -l, or -n")
        sys.exit(1)

    # Get input file info
    input_size = os.path.getsize(args.input)
    with open(args.input, 'r', encoding='utf-8') as f:
        total_lines = sum(1 for _ in f) - 1  # Exclude header

    print(f"\nInput: {args.input}")
    print(f"Size: {input_size:,} bytes ({input_size/1024/1024:.2f} MB)")
    print(f"Rows: {total_lines:,} (excluding header)\n")

    # Split
    if args.size:
        max_size = parse_size(args.size)
        print(f"Splitting by size: {max_size:,} bytes ({max_size/1024/1024:.2f} MB) per file\n")
        output_files = split_by_size(args.input, max_size)
    elif args.lines:
        print(f"Splitting by lines: {args.lines:,} lines per file\n")
        output_files = split_by_lines(args.input, args.lines)
    else:
        print(f"Splitting into {args.num} files\n")
        output_files = split_by_count(args.input, args.num)

    print(f"\nDone! Created {len(output_files)} files.")


if __name__ == '__main__':
    main()
