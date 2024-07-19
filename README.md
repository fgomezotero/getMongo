# getMongo

Python script to replace Nifi getMongo processor.

## Description

We need an alternative way to replace the use of getMongo Nifi processor. This python script aims to meet this requirement.

## Getting Started

### Dependencies

* We use the format of declare requirements files proposed by pip-compile-muti.

### Badgets

![Python](https://img.shields.io/badge/Python-%233776AB?logo=python&logoColor=black)
![MongoDB](https://img.shields.io/badge/MongoDB-47A248?logo=MongoDB&logoColor=black)

### Installing

* Clone the repository to a local directory.

  ```bash
  git clone https://github.com/fgomezotero/getMongo.git
  cd getMongo
  ```

* Create and activate a python environments

  ```bash
  python -m venv <environment_name>
  source <environment_name>/bin/activate
  ```
### Executing program

* How to run the program

    ```bash
    python getMongo.py --help or ./getMongo.py --help
    ```
> :warning: **Warning**:  Check the correct format of the query file in json format

## Authors

Franklin Gómez - fgomezotero@gmail.com

## License

This project is licensed under the MIT License