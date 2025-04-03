import os


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

    # Find closest matches within the range limit
    close_matches = []
    for fts_ts in remaining_fts_timestamps:
        closest_match = min((txt_ts for txt_ts in remaining_txt_timestamps if abs(txt_ts - fts_ts) <= range_limit),
                            key=lambda x: abs(x - fts_ts), default=None)
        if closest_match is not None:
            close_matches.append((fts_ts, closest_match))
            remaining_txt_timestamps.remove(closest_match)

    return exact_matches, close_matches, txt_data


def write_matches_to_file(fts_file, txt_file, output_file, range_limit, mhd_prefix):
    exact_matches, close_matches, txt_data = match_timestamps(fts_file, txt_file, range_limit)
    timestamps = read_fts_as_txt(fts_file)

    with open(output_file, 'w') as f:
        f.write(
            "Filename; Timestamp from FTS; Matching Timestamp from TXT; Branch number; Position in branch; Branch length; Branch generation; branchCode; Offset [mm]\n")

        # Write exact matches and close matches
        for i, timestamp in enumerate(timestamps):
            mhd_file = f"{mhd_prefix}_{i}.mhd"
            if timestamp in exact_matches:
                matching_data = next((line for line in txt_data if int(line[0]) == timestamp), ['-'] * 7)
                f.write(f"{mhd_file}; {timestamp}; {timestamp}; {'; '.join(matching_data[1:])}\n")
            else:
                close_match = next((match[1] for match in close_matches if match[0] == timestamp), None)
                if close_match:
                    matching_data = next((line for line in txt_data if int(line[0]) == close_match), ['-'] * 7)
                    f.write(f"{mhd_file}; {timestamp}; {close_match}; {'; '.join(matching_data[1:])}\n")
                else:
                    f.write(f"{mhd_file}; {timestamp}; no match; -; -; -; -; -; -\n")



# Example usage
fts_file = '/Users/karen/Documents/AnnotationWeb_Lung/Data/Patient001/2025-03-25_09-36_VideoRecording_28.cx3/US_Acq/BronchoscopyVideo_1_20250325T093638/BronchoscopyVideo_1_20250325T093638_openCV.fts'
txt_file = '/Users/karen/Documents/AnnotationWeb_Lung/Data/Patient001/2025-03-25_09-36_VideoRecording_28.cx3/TrackingInformation/01_20250325T093638_TrackingInformation.txt'
output_file = 'combined_data.txt'
mhd_prefix = 'BronchoscopyVideo_1_20250325T093638_openCV'  # This can be changed to any prefix as needed
range_limit = 60  # Define the range limit within which to consider timestamps as matching

write_matches_to_file(fts_file, txt_file, output_file, range_limit, mhd_prefix)

print(f"Combined data has been written to {output_file}")
