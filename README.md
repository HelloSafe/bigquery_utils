# Helpful BigQuery functions 

The [Python Client for Google BigQuery](https://cloud.google.com/python/docs/reference/bigquery/latest) is a set of methods that can be used to automate data manipulation on data tables stored on BigQuery. Some methods can be used across multiple projects. Hence, we create the bigquery_utils python project which contains helpful methods to pull data from BigQuery and apply basic manipulations. 

# Install

1. Clone git repository
    
    ```bash
    git clone https://github.com/HelloSafe/bigquery_utils.git
    cd bigquery_utils
    ```
    
2. Install poetry project
    
    ```bash
    brew install pipx # only if pipx is not installed, use pipx ensurepath to check
    pipx install poetry # only if poetry is not installed, use poetry --version to check
    ```
    
3. Install project
    
    ```bash
    poetry install
    ```
    

# Usage

If you want to use functions from `bigquery_utils` in another project, you first need to instantiate a new poetry project. Then, you can add `bigquery_utils` to your newly created project.

```bash
poetry new myproject
cd myproject
poetry add [root]/bigquery_utils/dist/bigquery_utils-0.1.0.tar.gz
```

**Note**: `[root]` depends on where you installed the `bigquery_utils` project.