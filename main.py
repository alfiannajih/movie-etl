import sys
import os
import pathlib

sys.path.append(str(pathlib.Path(os.path.dirname(os.path.realpath(__file__)), "src")))

from src.movie_etl.flows.etl_flow import movies_flow

def main():
    movies_flow.serve("Movie ETL")

if __name__ == "__main__":
    main()