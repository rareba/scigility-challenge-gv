terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~>3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

# Variable for consistent naming
variable "base_name" {
  description = "Base name for all resources to ensure uniqueness and consistency."
  type        = string
  default     = "scigilitybank"
}

variable "location" {
  description = "Azure region where resources will be deployed."
  type        = string
  default     = "West Europe"
}

# 1. Create a Resource Group
resource "azurerm_resource_group" "rg" {
  name     = "rg-${var.base_name}-challenge"
  location = var.location
}

# 2. Create an Azure Container Registry (ACR)
resource "azurerm_container_registry" "acr" {
  name                = "${var.base_name}acr"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "Standard"
  admin_enabled       = true # Simplifies access for the demo; use service principals for prod
}

# 3. Create an Azure Storage Account
resource "azurerm_storage_account" "sa" {
  name                     = "${var.base_name}-storage"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

# 4. Create a Blob Container within the Storage Account
resource "azurerm_storage_container" "container" {
  name                  = "data"
  storage_account_name  = azurerm_storage_account.sa.name
  container_access_type = "private"
}

# Outputs to easily retrieve names and keys
output "storage_account_name" {
  value = azurerm_storage_account.sa.name
}

output "storage_account_key" {
  value     = azurerm_storage_account.sa.primary_access_key
  sensitive = true
}

output "container_registry_name" {
  value = azurerm_container_registry.acr.name
}

output "container_registry_login_server" {
  value = azurerm_container_registry.acr.login_server
}