from file_helpers import gather_files
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--path", type=str, help="path to parent dir of files")
args = parser.parse_args()

def all_downloaded(path):
    files = gather_files(path)
    res = 'true' if len(files)==50000 else 'false'
    return res


if __name__ == '__main__':
    print(all_downloaded(args.path))