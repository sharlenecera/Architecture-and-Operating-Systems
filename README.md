# Sharlene's File Indexer

## Description
File Indexer CLI Service for Architecture and Operating Systems Assignment 2 of 2.

## Installation

### Prerequisites
- Python 3.10 or higher

### Steps
1. Create and activate a virtual environment.
    ```sh
    python3 -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

3. Install dependencies:
    ```sh
    pip install -r requirements.txt
    ```

4. Use the tool's help option to see available commands.
    ```sh
    python3 -m file_indexer --help
    ```

## Running Code

### C++
To compile and run on a MacOS terminal, do:
```sh
cd <director_of_cpp_file>
clang++ -std=c++17 -O2 -mmacosx-version-min=10.15 f_indexer.cpp -o f_indexer
./f_indexer --help
```
