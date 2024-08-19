# Storage Review Snowflake


<!-- WARNING: THIS FILE WAS AUTOGENERATED! DO NOT EDIT! -->

## How to use

# Snowflake Storage Analysis Application

This repository contains a modular Streamlit application that provides
comprehensive analysis and forecasting for Snowflake storage usage. The
application is divided into several components that handle different
aspects of the analysis, such as data querying, visualization,
forecasting, and recommendations. The modular approach makes the code
more maintainable, readable, and reusable.

## Directory Structure

## Main Components

### `storage/session.py`: Snowflake Session Management

This module contains the logic for creating and managing a Snowflake
session using the Snowpark API. It handles both OAuth token-based
authentication and traditional username/password-based authentication.
The session creation is abstracted to allow reuse across different parts
of the application.

### `storage/queries.py`: Query Execution

This module centralizes all the SQL queries used in the application. It
contains functions to run these queries and return the results as pandas
DataFrames. The queries themselves are organized into functions, which
accept parameters to dynamically generate the correct SQL for different
analyses.

### `storage/visualization.py`: Data Visualization

This module handles all the plotting and visualization tasks within the
Streamlit app. It uses Plotly to create interactive charts, such as line
charts for storage trends, pie charts for storage breakdowns, and bar
charts for unused table costs. The visualizations are designed to be
clear and informative, providing insights at a glance.

### `storage/forecast.py`: Storage Forecasting

This module contains the logic for generating storage forecasts using
Snowflake’s machine learning capabilities. The forecast generation
involves creating training data, building a forecast model, and
generating predictions for future storage usage. The forecast results
are then returned for visualization and further analysis.

### `storage/recommendations.py`: Recommendations Generation

This module generates recommendations based on the data analysis and
forecast results. It assesses factors like projected storage growth,
unused tables, and overall storage usage, and provides actionable
insights to help optimize storage and reduce costs. The recommendations
are then displayed in the Streamlit app.

### `streamlit_app.py`: Main Streamlit Application

This is the main entry point of the application. It ties together the
components from the `storage/` directory to create a cohesive user
interface. The application is structured to first fetch and display
storage data, followed by unused tables analysis, storage forecasting,
and recommendations. The session state is managed to ensure that data is
only fetched or computed when necessary, reducing redundant operations
and improving performance.

## Application Workflow

1.  **Data Fetching and Initialization:**  
    The application initializes session state variables to store the
    data fetched from Snowflake. It checks if the data is already in the
    session state; if not, it runs the corresponding SQL queries to
    fetch the data.

2.  **Data Visualization:**  
    After fetching the data, the application uses the `visualization.py`
    module to display various charts that show historical storage usage,
    daily trends, and a breakdown of storage components (active, stage,
    fail-safe).

3.  **Unused Tables Analysis:**  
    Users can specify criteria (e.g., days since last access, storage
    cost per TB) to identify unused tables. The application queries
    Snowflake for tables that meet these criteria and displays the
    results, including potential cost savings from removing or archiving
    these tables.

4.  **Storage Forecasting:**  
    Users can generate a forecast for future storage usage by specifying
    the number of training days and prediction days. The forecast
    includes upper and lower bounds, which are visualized alongside the
    predicted usage. The application also provides a cost estimation
    based on the forecasted storage.

5.  **Recommendations:**  
    Based on the analysis and forecasts, the application generates
    recommendations to help users optimize their Snowflake storage.
    These include strategies for managing growth, cleaning up unused
    tables, and general best practices for storage management.

## Local Development

### Prerequisites

Before running the application, ensure you have the following
dependencies installed:

- **Streamlit:** For creating the interactive web interface.
- **Plotly:** For generating interactive visualizations.
- **Snowflake-Snowpark-Python:** For interacting with Snowflake using
  the Snowpark API.

You can install these dependencies using the provided `environment.yml`
file or directly via pip:

``` bash
pip install streamlit plotly snowflake-snowpark-python
```

> **Note:** This application uses Snowflake’s Cortex forecast function
> to predict future storage usage. Ensure your Snowflake account has the
> necessary permissions to use this function. If you prefer, you can
> replace this with another forecasting method.

### Running the Application Locally

To run the application locally, navigate to the project directory and
use the following command:

``` bash
streamlit run streamlit_app.py
```

This will start a local Streamlit server and automatically open the
application in your web browser.

### Testing the Application

You can modify the application locally and test your changes
immediately. Streamlit’s built-in hot-reloading feature automatically
updates the app as you save your changes, making the development process
seamless and efficient.

## Deploying the App to Streamlit in Snowflake

### Prerequisites

To deploy the application to Streamlit on Snowflake, you need to install
the Snowflake CLI (`snow`). This CLI allows you to interact with your
Snowflake instance and manage various resources, including Streamlit
applications.

Install the Snowflake CLI by following the [installation
instructions](https://docs.snowflake.com/en/developer-guide/snowflake-cli-v2/installation/installation#how-to-install-sf-cli-using-pip-pypi).

### Deployment Steps

1.  **Create a Connection:**  
    Ensure you have a Snowflake connection configured using the
    Snowflake CLI. This connection will be used to authenticate and
    deploy your Streamlit application.

2.  **Deploy the Application:**  
    Once you have set up the connection, you can deploy the application
    using the following command:

    ``` bash
    snow streamlit deploy --open --replace
    ```

    This command deploys your application to Snowflake’s Streamlit
    environment, replacing any existing deployment with the same name.
    The `--open` flag automatically opens the application in your
    browser after deployment.

## Deploy with GitHub Actions

### Continuous Deployment with GitHub Actions

You can automate the deployment of your Streamlit application to
Snowflake using GitHub Actions. This ensures that every time you push
changes to your repository, the application is automatically redeployed.

### Prerequisites

1.  **GitHub Secrets Setup:**
    - You need to configure secrets in your GitHub repository for
      authentication with Snowflake. These secrets include your
      Snowflake account information, username, password, and any other
      required credentials.
    - Refer to the [GitHub Secrets
      documentation](https://docs.github.com/en/actions/security-for-github-actions/security-guides/using-secrets-in-github-actions)
      for guidance on setting up and managing secrets.
2.  **GitHub Actions Workflow:**
    - A workflow file (`.github/workflows/deploy.yml`) should be created
      to define the steps for deploying the application. This file
      specifies the actions to be taken on each push, such as running
      tests and deploying the app.

### Deployment Process

1.  **Fork or Clone the Repository:**  
    Start by forking or cloning this repository to your GitHub account.

2.  **Push the Repository:**  
    After setting up the repository and secrets, push your code to
    GitHub. Ensure GitHub Actions is enabled in your repository
    settings.

3.  **Automated Deployment:**  
    With GitHub Actions configured, every push to the repository
    triggers an automated workflow that tests and deploys your
    application to Snowflake’s Streamlit environment.

> If you run into any issue please put an issue in the repo and we will
> get to it asap.
