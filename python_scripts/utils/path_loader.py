import os


def generate_file_path(file_name: str, file_extension: str, directory: str = 'python_scripts/logs') -> str:
    """
    Generates the complete path for a file based on its name and extension, 
    and an optional directory path.

    Parameters:
    file_name (str): The name of the file.
    file_extension (str): The file's extension, without a dot (e.g., 'txt').
    directory (str): Optional; the directory path. Defaults to current working directory.

    Returns:
    str: The absolute file path.
    """
    # Ensure the file extension starts with a dot
    if not file_extension.startswith('.'):
        file_extension = '.' + file_extension

    # Use the specified directory or default to the current directory
    if directory is None:
        directory = os.getcwd()

    # Construct the full file path
    full_path = os.path.join(directory, f"{file_name}{file_extension}")

    # Normalize the path for consistent use across operating systems
    return os.path.abspath(full_path)

