# Contributing to the Project

First off, thank you for considering contributing to this project! Your help is greatly appreciated.

Following these guidelines helps to communicate that you respect the time of the developers managing and developing this open-source project. In return, they should reciprocate that respect in addressing your issue, assessing changes, and helping you finalize your pull requests.

## Table of Contents

- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Environment Setup](#environment-setup)
- [How to Contribute](#how-to-contribute)
  - [Reporting Bugs and Requesting Features](#reporting-bugs-and-requesting-features)
  - [Your First Code Contribution](#your-first-code-contribution)
- [Development Workflow](#development-workflow)
  - [Branching](#branching)
  - [Making Changes](#making-changes)
  - [Submitting a Pull Request](#submitting-a-pull-request)
- [Style Guides](#style-guides)
  - [Python Styleguide](#python-styleguide)
  - [Commit Message Guidelines](#commit-message-guidelines)

## Getting Started

### Prerequisites

- **Docker**: The development environment is containerized using VS Code Dev Containers. Please install [Docker Desktop](https://www.docker.com/products/docker-desktop/).
- **VS Code**: With the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) installed.
- **Running Services**: Ensure you have instances of Kafka, MongoDB, and PostgreSQL running and accessible to the development container.

### Environment Setup

1.  **Clone the repository**:
    ```sh
    git clone https://github.com/Bootcamp-IA-P5/proyecto9-grupo4.git
    cd proyecto9-grupo4
    ```
2.  **Create a `.env` file**: Copy the `.env.example` to a new file named `.env` and fill in the connection details for your databases (PostgreSQL) and Kafka.
    ```sh
    cp .env.example .env
    ```
3.  **Open in Dev Container**: Open the project folder in VS Code. You should see a prompt to "Reopen in Container". Click it. This will build the container based on the `.devcontainer/devcontainer.json` file and automatically install all dependencies from `requirements.txt`.

## How to Contribute

### Reporting Bugs and Requesting Features

This project uses GitHub Issues to track bugs and feature requests. Please check the existing issues to see if your problem or idea has already been discussed.

When creating a new issue, please use the provided templates (like the `documentation_request.yml` for documentation tasks) to ensure you provide all the necessary information.

### Your First Code Contribution

Unsure where to begin? You can start by looking through `good first issue` and `help wanted` issues.

## Development Workflow

### Branching

Create a new branch for your feature or bugfix from the `main` branch. A good branch name is descriptive and includes the issue number if applicable.

**Format**: `feature/issue-123-new-kafka-parser` or `fix/bug-45-sql-connection`

```sh
git checkout -b feature/my-new-feature
```

### Making Changes

- Write clean, readable code with clear comments and docstrings in English.
- Ensure your changes work as expected. You can use the provided scripts to test the data flow:
  - `scripts/sql_clean_db.py`: To reset your PostgreSQL database.
  - `scripts/sql_load_db.py`: To load test data into PostgreSQL.
  - `scripts/read_from_kafka.py`: To consume messages and write to MongoDB.

### Submitting a Pull Request

1.  Once your changes are ready, commit them with a descriptive message.
2.  Push your branch to the repository and open a Pull Request against the `main` branch.
3.  In your PR description, link to the issue it resolves (e.g., "Closes #123").
4.  Ensure your PR is reviewed and approved before it gets merged.

## Style Guides

### Python Styleguide

All Python code should adhere to PEP 8. We recommend using a linter like `flake8` or `pylint` to check your code.

### Commit Message Guidelines

Use clear and concise commit messages. The first line should be a short summary (50 characters or less), followed by a blank line and a more detailed explanation if necessary.