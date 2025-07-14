# Bank Data Processing Pipeline on Azure

This project contains a fully automated, containerized data pipeline to process bank data. It uses Apache Spark for processing, Docker for containerization, Terraform for infrastructure provisioning on Azure, and Azure DevOps for CI/CD.

## Solution Overview

1.  **Infrastructure**: Managed by Terraform (`infra/main.tf`), which creates an Azure Resource Group, a Storage Account (for data), and a Container Registry (for the Docker image).
2.  **Data Pipeline**: A PySpark application (`src/`) that reads CSV data from Azure Blob Storage, performs cleaning and aggregation, and writes the results back to Blob Storage in Parquet and CSV formats.
3.  **CI/CD**: An Azure DevOps pipeline (`azure-pipelines.yml`) automatically tests the code, builds a Docker image, and pushes it to the Azure Container Registry on every commit to the `main` branch.

## How to Deploy and Run

### Prerequisites

1.  [Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli)
2.  [Terraform](https://learn.hashicorp.com/tutorials/terraform/install-cli)
3.  [Docker Desktop](https://www.docker.com/products/docker-desktop)
4.  An Azure account and an Azure DevOps organization.

### Step 1: Set Up Azure DevOps

1.  Create a new project in Azure DevOps.
2.  Push this repository to your Azure Repos or link it from GitHub.
3.  Create a **Service Connection** to your Azure subscription. Go to `Project Settings` > `Service connections`, create a new connection, and select `Azure Resource Manager` using the `Service principal (automatic)` authentication. Note the name of this connection.
4.  Update the `azure-pipelines.yml` file with your service connection name in the `variables` section.

### Step 2: Deploy Infrastructure

Use Terraform to create the cloud resources.

```bash
# Navigate to the infra directory
cd infra

# Initialize Terraform
terraform init

# Apply the configuration
terraform apply