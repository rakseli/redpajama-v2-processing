from file_helpers import gather_files
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--path", type=str, help="path to parent dir of files")
parser.add_argument("--n_urls", type=int, help="number of urls")


def all_downloaded(path,n_urls):
    files = gather_files(path)
    res = 'true' if len(files)==n_urls else 'false'
    return res

if __name__ == '__main__':
    args = parser.parse_args()
    print(all_downloaded(args.path,args.n_urls))