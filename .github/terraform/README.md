# GitHub Repository Terraform Management

This directory contains Terraform configuration to manage the `mlb_statsapi_data_platform` GitHub repository as Infrastructure as Code.

## Overview

The repository was initially created using GitHub CLI (`gh`), and is now managed via Terraform using the import functionality.

## Setup

### 1. Configure Environment

```bash
cd .github/terraform

# Copy template and fill in your GitHub token
cp .env.template .env
nano .env  # Add your GITHUB_TOKEN

# Source environment variables
source .env
```

### 2. Initialize Terraform

```bash
# Initialize Terraform (first time only)
terraform init

# Import existing repository (already done)
# terraform import github_repository.repo mlb_statsapi_data_platform
```

### 3. Manage Repository

```bash
# Preview changes
terraform plan

# Apply changes
terraform apply

# View current state
terraform show
```

## What's Managed

- Repository settings (description, topics, features)
- Branch protection rules (main branch)
- Issue labels (bug, enhancement, testing, etc.)
- Default branch configuration

## What's NOT Managed

- Repository secrets (manage via GitHub UI or `gh secret` command)
- GitHub Actions workflows (managed in `.github/workflows/`)
- Collaborators (can be added later if needed)

## Making Changes

1. Edit the Terraform files (main.tf, variables.tf)
2. Run `terraform plan` to preview
3. Run `terraform apply` to apply
4. Commit the changes to git

## Backend Configuration

Currently using local backend. For team collaboration, consider:
- S3 backend (like pymlb_statsapi)
- Terraform Cloud
- GitLab CI with S3

## Notes

- Repository URL uses SSH host alias: `github.com-poweredgesports_gmail`
- Prevent destroy is enabled on the repository resource
- Main branch has protection requiring PR approval
