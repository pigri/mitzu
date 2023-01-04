#!/bin/sh
set -e

cd examples/notebook
for notebook in $(ls *.ipynb); do
    poetry run jupyter nbconvert --to python --output "$notebook.py" $notebook
    poetry run python3 "$notebook.py"
done