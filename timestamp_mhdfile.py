import os


def read_fts_as_txt(file):
    with open(file, 'r') as f:
        lines = f.readlines()
    timestamps = [int(line.strip()) for line in lines]
    return timestamps


def write_timestamps_and_mhd_files(fts_file, output_file, mhd_prefix):
    timestamps = read_fts_as_txt(fts_file)

    with open(output_file, 'w') as f:
        f.write("Timestamp; MHD File\n")
        for i, timestamp in enumerate(timestamps):
            mhd_file = f"{mhd_prefix}_{i}.mhd"
            f.write(f"{timestamp}; {mhd_file}\n")


# Example usage
fts_file = '/Users/karen/Documents/AnnotationWeb_Lung/Data/Patient001/2025-03-25_09-36_VideoRecording_28.cx3/US_Acq/BronchoscopyVideo_1_20250325T093638/BronchoscopyVideo_1_20250325T093638_openCV.fts'
output_file = 'timestamps_and_mhd_files.txt'
mhd_prefix = 'BronchoscopyVideo_1_20250325T093638_openCV'
write_timestamps_and_mhd_files(fts_file, output_file, mhd_prefix)

print(f"Timestamps and corresponding MHD filenames have been written to {output_file}")
