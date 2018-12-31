'''
splitjoin.py
sonicskye@2018

The functions are used to split and join files

based on:
    https://stonesoupprogramming.com/2017/09/16/python-split-and-join-file/
with modification by adding natural sort


'''

import os
import re


# https://stackoverflow.com/questions/11150239/python-natural-sorting
def natural_sort(l):
    convert = lambda text: int(text) if text.isdigit() else text.lower()
    alphanum_key = lambda key: [convert(c) for c in re.split('([0-9]+)', key)]
    return sorted(l, key = alphanum_key)


def split(source, dest_folder, write_size):
    # Make a destination folder if it doesn't exist yet
    if not os.path.exists(dest_folder):
        os.mkdir(dest_folder)
    else:
        # Otherwise clean out all files in the destination folder
        for file in os.listdir(dest_folder):
            os.remove(os.path.join(dest_folder, file))

    partnum = 0

    # Open the source file in binary mode
    input_file = open(source, 'rb')

    while True:
        # Read a portion of the input file
        chunk = input_file.read(write_size)

        # End the loop if we have hit EOF
        if not chunk:
            break

        # Increment partnum
        partnum += 1

        # Create a new file name
        filename = os.path.join(dest_folder, ('part-' + str(partnum)))

        # Create a destination file
        dest_file = open(filename, 'wb')

        # Write to this portion of the destination file
        dest_file.write(chunk)

        # Explicitly close
        dest_file.close()

    # Explicitly close
    input_file.close()

    # Return the number of files created by the split
    return partnum


def join(source_dir, dest_file, read_size):
    # Create a new destination file
    output_file = open(dest_file, 'wb')

    # Get a list of the file parts
    parts = os.listdir(source_dir)

    # Sort them by name (remember that the order num is part of the file name)
    # should use natural sort
    #parts.sort()
    parts = natural_sort(parts)

    # Go through each portion one by one
    for file in parts:

        # Assemble the full path to the file
        path = os.path.join(source_dir, file)

        # Open the part
        input_file = open(path, 'rb')

        while True:
            # Read all bytes of the part
            bytes = input_file.read(read_size)

            # Break out of loop if we are at end of file
            if not bytes:
                break

            # Write the bytes to the output file
            output_file.write(bytes)

        # Close the input file
        input_file.close()

    # Close the output file
    output_file.close()


# example
'''
imageFilePath = os.path.join(os.path.dirname(__file__), 'cryptocurrency.jpg')
destinationFolderPath = os.path.join(os.path.dirname(__file__), 'tmp')
imageFilePath2 = os.path.join(os.path.dirname(__file__), 'cryptocurrency2.jpg')
split(imageFilePath, destinationFolderPath, 2350)
join(destinationFolderPath, imageFilePath2, 4700)
'''
