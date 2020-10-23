from pyunpack import Archive
import os


def decompress(file_path, extract_path):
    """Decompresses files using pyunpack.
    Parameters:
    file_path (str): Path of compressed file to decompress.
    extract_path (str): Basename of path to decompress too. Compressed files will be
    decompressed into /extract_path/file_path (e.g. if the file_path is file_path.tar
    and extract_path is /local/path, file_path.tar will be decompressed into
    /local/path/file_path/).
    :return:
    """
    try:
        file_name = os.path.basename(file_path)[:os.path.basename(file_path).index('.')]
        extract_path = os.path.join(extract_path, file_name)
        try:
            os.mkdir(extract_path)
        except:
            pass

        Archive(file_path).extractall(extract_path)
    except Exception as e:
        print(f"Extraction failed: {e}")


def is_compressed(file_path):
    """Checks whether a file path is compressed inn pyunpack supported forms
    Parameter:
    file_path (str): File path to check for compression.
    Return:
    (bool): Whether file_path is compressed.
    """
    extension = os.path.splitext(file_path)[1]
    compressed_extensions = ['.7z', '.ace', '.alz', '.a', 'arc', '.arj', '.bz2'
                             , '.cab', '.Z', '.cpio', '.deb', '.dms', '.gz',
                             '.lrz', '.lha', '.lzh', '.lz', '.lzma', '.lzo'
                             , '.rpm', '.rar', '.rz', '.tar', '.xz', '.zip']

    if extension in compressed_extensions:
        return True
    else:
        return False


def recursive_compression_helper(file_path, compressed_files):
    """Helper function to decompress recursively compressed files.
    file_path (str): File path to folder with compressed files.
    compressed_files (list): List of compressed files that have already
    been decompressed.
    """
    file_list = []

    for path, subdirs, files in os.walk(file_path):
        for name in files:
            file_list.append(os.path.join(path, name))

    for file in file_list:
        if is_compressed(file) and file not in compressed_files:
            decompress(file, file_path)
            compressed_files.append(file)

            recursive_compression_helper(file_path, compressed_files)


def recursive_compression(file_path, extract_path):
    """Decompresses a file and any compressed files within it.
    file_path (str): Path of file to decompress.
    extract_path (str): Basename of path to decompress too. Compressed files will be
    decompressed into /extract_path/file_path (e.g. if the file_path is file_path.tar
    and extract_path is /local/path, file_path.tar will be decompressed into
    /local/path/file_path/).
    """
    decompress(file_path, extract_path)
    compressed_path = os.path.join(extract_path,
                                   os.path.basename(file_path)[:os.path.basename(file_path).index('.')])

    recursive_compression_helper(compressed_path, [])

# 1. The extract_path must be an existing directory.
# 2. The actual files are inside a folder inside of extract_path
recursive_compression('error_log-1578067261.gz', 'tomato')
