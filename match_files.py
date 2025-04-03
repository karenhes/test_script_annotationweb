def read_fts_as_txt(file):
    with open(file, 'r') as f:
        lines = f.readlines()
    timestamps = [int(line.strip()) for line in lines]
    return timestamps


def read_txt_timestamps(file):
    with open(file, 'r') as f:
        lines = f.readlines()
    data = [line.strip().split(';') for line in lines[1:]]  # Skip header line and split data
    timestamps = [int(line[0]) for line in data]
    return timestamps, data


def match_timestamps(fts_file, txt_file, range_limit):
    fts_timestamps = read_fts_as_txt(fts_file)
    txt_timestamps, txt_data = read_txt_timestamps(txt_file)

    # Find exact matches
    exact_matches = set(fts_timestamps).intersection(txt_timestamps)

    # Remove exact matches from the lists
    remaining_fts_timestamps = [ts for ts in fts_timestamps if ts not in exact_matches]
    remaining_txt_timestamps = [ts for ts in txt_timestamps if ts not in exact_matches]

    # Find close matches within the range limit
    close_matches = []
    for fts_ts in remaining_fts_timestamps:
        close_match = next((txt_ts for txt_ts in remaining_txt_timestamps if abs(txt_ts - fts_ts) <= range_limit), None)
        if close_match:
            close_matches.append((fts_ts, close_match))
            remaining_txt_timestamps.remove(close_match)

    return exact_matches, close_matches, txt_data


def extract_info_and_write_to_file(txt_file, exact_matches, close_matches, output_file):
    _, txt_data = read_txt_timestamps(txt_file)

    matched_data = [line for line in txt_data if
                    int(line[0]) in exact_matches or any(int(line[0]) == match[1] for match in close_matches)]

    with open(output_file, 'w') as f:
        f.write(
            "{Timestamp; Branch number; Position in branch; Branch length; Branch generation; branchCode; Offset [mm]}\n")
        for line in matched_data:
            f.write(';'.join(line) + '\n')


# Example usage
fts_file = '/Users/karen/Documents/AnnotationWeb_Lung/Data/Patient001/2025-03-25_09-36_VideoRecording_28.cx3/US_Acq/BronchoscopyVideo_1_20250325T093638/BronchoscopyVideo_1_20250325T093638_openCV.fts'
txt_file = '/Users/karen/Documents/AnnotationWeb_Lung/Data/Patient001/2025-03-25_09-36_VideoRecording_28.cx3/TrackingInformation/01_20250325T093638_TrackingInformation.txt'

range_limit = 30  # Define the range limit within which to consider timestamps as matching
output_file = 'matched_data.txt'

exact_matches, close_matches, _ = match_timestamps(fts_file, txt_file, range_limit)
extract_info_and_write_to_file(txt_file, exact_matches, close_matches, output_file)

print(f"Exact matching timestamps: {exact_matches}")
print(f"Number of exact matching timestamps: {len(exact_matches)}")
print(f"Close matching timestamps within range: {close_matches}")
print(f"Number of close matching timestamps: {len(close_matches)}")
print(f"Matched data has been written to {output_file}")

"""
def read_fts_as_txt(file):
    with open(file, 'r') as f:
        lines = f.readlines()
    timestamps = [int(line.strip()) for line in lines]
    return timestamps


def read_txt_timestamps(file):
    with open(file, 'r') as f:
        lines = f.readlines()
    timestamps = [int(line.split(';')[0].strip()) for line in lines[1:]]  # Skip header line and extract timestamps
    return timestamps


def match_timestamps(fts_file, txt_file, range_limit):
    fts_timestamps = read_fts_as_txt(fts_file)
    txt_timestamps = read_txt_timestamps(txt_file)

    # Find exact matches
    exact_matches = set(fts_timestamps).intersection(txt_timestamps)

    # Remove exact matches from the lists
    remaining_fts_timestamps = [ts for ts in fts_timestamps if ts not in exact_matches]
    remaining_txt_timestamps = [ts for ts in txt_timestamps if ts not in exact_matches]

    # Find close matches within the range limit
    close_matches = []
    for fts_ts in remaining_fts_timestamps:
        close_match = next((txt_ts for txt_ts in remaining_txt_timestamps if abs(txt_ts - fts_ts) <= range_limit), None)
        if close_match:
            close_matches.append((fts_ts, close_match))
            remaining_txt_timestamps.remove(close_match)

    return exact_matches, close_matches


# Example usage
fts_file = '/Users/karen/Documents/AnnotationWeb_Lung/Data/Patient001/2025-03-25_09-36_VideoRecording_28.cx3/US_Acq/BronchoscopyVideo_1_20250325T093638/BronchoscopyVideo_1_20250325T093638_openCV.fts'
txt_file = '/Users/karen/Documents/AnnotationWeb_Lung/Data/Patient001/2025-03-25_09-36_VideoRecording_28.cx3/TrackingInformation/01_20250325T093638_TrackingInformation.txt'
range_limit = 15  # Define the range limit within which to consider timestamps as matching
exact_matches, close_matches = match_timestamps(fts_file, txt_file, range_limit)
print(f"Exact matching timestamps: {exact_matches}")
print(f"Number of exact matching timestamps: {len(exact_matches)}")
print(f"Close matching timestamps within range: {close_matches}")
print(f"Number of close matching timestamps: {len(close_matches)}")
"""

