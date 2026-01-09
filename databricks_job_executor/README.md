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
- Databricks service principal with OAuth M2M credentials (client ID and client secret)

### Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set environment variables:
```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_CLIENT_ID="your-client-id"
export DATABRICKS_CLIENT_SECRET="your-client-secret"
export DATABRICKS_JOB_ID="123456"  # Optional: specific job ID to run
```

Or create a `.env` file:
```
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_CLIENT_ID=your-client-id
DATABRICKS_CLIENT_SECRET=your-client-secret
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

- **DATABRICKS_HOST** (required for local): Your Databricks workspace URL (e.g., `https://your-workspace.cloud.databricks.com`)
- **DATABRICKS_CLIENT_ID** (required for local): Your service principal client ID
- **DATABRICKS_CLIENT_SECRET** (required for local): Your service principal client secret
- **DATABRICKS_JOB_ID** (required): The specific job ID to run

**Authentication Methods:**
- **Local Development**: Uses OAuth M2M (service principal) with `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET`
- **Databricks Runtime**: Automatically uses built-in authentication (no credentials needed)

These credentials are read from environment variables at startup. The connection status is displayed in the sidebar.

## Usage

1. Ensure your environment variables are set correctly (including `DATABRICKS_JOB_ID`)
2. Launch the application
3. The sidebar will show your connection status and configured job
4. Click "Run Job" to execute the configured migration job
5. Monitor job runs and view logs as needed

**Note:** The application is designed to run a single configured job specified by `DATABRICKS_JOB_ID`. For multi-task jobs, logs are retrieved per task automatically.

## Security Note

Never commit your `DATABRICKS_CLIENT_SECRET` to version control. Always use environment variables or secure credential management systems (e.g., Databricks Secrets).

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

2.  **Databricks App Configuration**:
    When deploying to Databricks as an app, authentication is handled automatically using the Databricks runtime's built-in authentication. No explicit credentials (client ID/secret) are needed when running on Databricks.

    For local development configuration, you can optionally use Databricks Widgets to pass `databricks_host`, `databricks_client_id`, `databricks_client_secret`, and `databricks_job_id` if needed.

3.  **Databricks Secrets (for Local Development)**:
    For enhanced security during local development, you can store your OAuth credentials in Databricks Secrets and retrieve them programmatically.

    To set up Databricks Secrets:
    *   **Create a Secret Scope**:
        ```bash
        databricks secrets create-scope --scope oauth-credentials
        ```
        (You might need to configure ACLs for this scope to allow users/groups to read it.)
    *   **Put the Secrets**:
        ```bash
        databricks secrets put --scope oauth-credentials --key client-id
        databricks secrets put --scope oauth-credentials --key client-secret
        ```
        When prompted, enter your service principal credentials.

    **Note**: When running on Databricks as an app, the runtime automatically handles authentication, so explicit credential storage is not required.

