# Data Migration Accelerator - Databricks Job Executor

A Streamlit application for executing and monitoring Databricks migration jobs.

## Features

- Execute a configured Databricks migration job
- Monitor job runs and progress in real-time
- View job logs and diagnostics (supports multi-task jobs)
- Cancel running jobs if needed

## Setup

### Prerequisites

- Python 3.8+
- Streamlit
- Databricks workspace access
- Databricks personal access token

### Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set environment variables:
```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-personal-access-token"
export DATABRICKS_JOB_ID="123456"  # Optional: specific job ID to run
```

Or create a `.env` file:
```
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-personal-access-token
DATABRICKS_JOB_ID=123456
```

### Running the Application Locally

From the `databricks_job_executor` directory:

```bash
streamlit run streamlit_app/app.py
```

Or from the parent directory:

```bash
streamlit run databricks_job_executor/streamlit_app/app.py
```

### Deploying to Databricks

This application can be deployed to Databricks using Databricks Asset Bundles.

1.  **Ensure Databricks CLI is installed and configured.**
    ```bash
    pip install databricks-cli
    databricks configure --token
    ```
    Follow the prompts to enter your Databricks host and token.

2.  **Navigate to the `databricks_job_executor` directory.**
    ```bash
    cd databricks_job_executor
    ```

3.  **Deploy the bundle:**
    ```bash
    databricks bundle deploy -t dev
    ```
    This will deploy the Streamlit application as a Databricks App.

4.  **Access the Streamlit App:**
    Once deployed, navigate to your Databricks workspace, find the deployed app (e.g., `dbx-job-executor-app`), and launch it. The Streamlit app will be accessible directly, providing an interactive application experience.

    **Note on `databricks.yml` command:** The `databricks.yml` now uses `bash -c "python -m streamlit run ..."` to ensure the Streamlit application path is correctly resolved within the Databricks App environment.

    You can also pass `DATABRICKS_JOB_ID` as a widget parameter when launching the job run if you want to override the default job ID configured in the `.env` file.

## Configuration

The application requires the following environment variables:

- **DATABRICKS_HOST** (required): Your Databricks workspace URL (e.g., `https://your-workspace.cloud.databricks.com`)
- **DATABRICKS_TOKEN** (required): Your Databricks personal access token
- **DATABRICKS_JOB_ID** (required): The specific job ID to run

These credentials are read from environment variables at startup. The connection status is displayed in the sidebar.

## Usage

1. Ensure your environment variables are set correctly (including `DATABRICKS_JOB_ID`)
2. Launch the application
3. The sidebar will show your connection status and configured job
4. Click "Run Job" to execute the configured migration job
5. Monitor job runs and view logs as needed

**Note:** The application is designed to run a single configured job specified by `DATABRICKS_JOB_ID`. For multi-task jobs, logs are retrieved per task automatically.

## Security Note

Never commit your `DATABRICKS_TOKEN` to version control. Always use environment variables or secure credential management systems.

### Setting Environment Variables and Secrets on Databricks

When deploying and running the Streamlit app on Databricks, you can configure the necessary environment variables and secrets using the following methods:

1.  **Job Environment Variables (via `databricks.yml`)**:
    The `DATABRICKS_BUNDLE_ENV` variable is automatically set based on your bundle target (e.g., `dev`, `prod`) within the `databricks.yml` file. Other environment variables can also be set directly in the `new_cluster.spark_env_vars` section of your job definition in `databricks.yml`.

    Example from `databricks.yml`:
    ```yaml
    tasks:
      - task_key: run_streamlit_app
        new_cluster:
          # ...
          spark_env_vars:
            STREAMLIT_SERVER_PORT: "8080"
            DATABRICKS_BUNDLE_ENV: ${bundle.environment}
            # Add other environment variables here if needed, e.g.:
            # MY_CUSTOM_VAR: "value"
    ```

2.  **Databricks Widgets (for `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_JOB_ID`)**:
    When you launch a Databricks App, you can pass parameters as widgets. The Streamlit app is configured to read `databricks_host`, `databricks_token`, and `databricks_job_id` from these widgets if they are present.

    To set widgets when launching the app:
    *   Go to your Databricks workspace.
    *   Navigate to "Apps" (or the equivalent section where deployed apps are listed).
    *   Select your deployed app (e.g., `databricks-job-executor-streamlit`).
    *   Click "Launch" or "Run App".
    *   In the launch dialog, you may find options to set parameters. If not directly available, you might need to configure them in the `databricks.yml` or rely on secrets.
        *   `databricks_host`: `https://your-workspace.cloud.databricks.com`
        *   `databricks_token`: `dapixxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx` (your personal access token)
        *   `databricks_job_id`: `123456` (the ID of the job you want to execute)

3.  **Databricks Secrets (for `DATABRICKS_TOKEN`)**:
    For enhanced security, it is recommended to store your `DATABRICKS_TOKEN` in Databricks Secrets. The application will attempt to retrieve the token from a secret scope if it's not provided via environment variables or widgets.

    To set up Databricks Secrets:
    *   **Create a Secret Scope**:
        ```bash
        databricks secrets create-scope --scope databricks-token-scope
        ```
        (You might need to configure ACLs for this scope to allow users/groups to read it.)
    *   **Put the Secret**:
        ```bash
        databricks secrets put --scope databricks-token-scope --key databricks-token-key
        ```
        When prompted, paste your Databricks personal access token.

    The application will then automatically attempt to retrieve the token using `dbutils.secrets.get("databricks-token-scope", "databricks-token-key")` when running in the Databricks environment.

