# GENAI_BUILDING_DYNAMIC_TASKS

A sample project demonstrating how to build dynamic GenAI workflows using Apache Airflow, Weaviate and Streamlit. This repository contains an Airflow compose setup, example DAGs, and a Streamlit UI to explore model results and embeddings.

Key goals:
- Run Apache Airflow (v3.0.0) locally using a compose stack.
- Use Weaviate as a vector database for GenAI workflows.
- Provide a Streamlit frontend for model results and exploration.
- Prefer Podman (free) for container commands instead of Docker.

Prerequisites
- Podman (free edition) installed and configured (podman and podman-compose / podman compose).
- Python 3.11
- Optional: VPN if your environment requires it to reach APIs (e.g., whitelisted OpenAI endpoints).
- An OpenAI API key (if using OpenAI vectorizers or the OpenAI provider).

Quick start (commands you used)
1. Create a project directory
   mkdir my-project && cd my-project

2. Create a docker-compose.yaml
   (place the provided docker-compose.yaml from this repo into the project root)

3. Create virtual env using
   uv venv --python 3.11
   If uv does not exist then install using native python/homebrew etc.

4. Switch to use this new env
   source .venv/bin/activate

5. Install apache airflow
   uv pip install apache-airflow==3.0.0

6. Create Dockerfile and docker-compose.yaml accordingly
   (Dockerfile in this repo already copies requirements.txt and runs pip install)

7. Create .env file as needed
   See sample_env in this repository for variables to set.

8. Install dependencies and libs in containers (use Podman)
   podman compose up -d
   OR
   podman compose up --build -d
   OR
   podman compose down && podman compose up --build -d

   To start streamlit, while connected to VPN
   podman compose up streamlit 2>&1 | head -50

Access
- Airflow UI: http://localhost:8080/
- Streamlit app: http://localhost:8501

Python dependency installs (local development)
- Install streamlit requirements:
  uv pip install -r include/streamlit/requirements.txt

- Or install components individually:
  uv pip install langchain==0.1.16
  uv pip install apache-airflow-providers-weaviate==1.3.4

Requirements and Docker image
- The repository Dockerfile copies the repository root requirements.txt and runs pip install -r requirements.txt.
- If you need the additional packages to be available in the Airflow image, add them to the root requirements.txt (suggested contents provided below) and rebuild the image via Podman: podman compose up --build -d

Files of interest
- docker-compose.yaml — compose stack for Airflow, Weaviate and Streamlit.
- Dockerfile — builds an image from apache/airflow:3.0.0 and installs requirements.txt.
- requirements.txt — Python packages installed into the Airflow image.
- include/streamlit/requirements.txt — Streamlit dependencies used by the Streamlit service.
- sample_env — example environment variables (.env) to configure Airflow and Weaviate connectors.
- dags/ — place your Airflow DAGs here.

Notes about Podman vs Docker
- This repository and the instructions use Podman (podman compose / podman-compose) for container orchestration locally. Replace any `docker` or `docker-compose` invocation with the `podman` equivalent:
  - podman compose up -d
  - podman compose up --build -d
  - podman compose down

Third-party tutorials and attribution / license notes
- Astronomer Academy tutorial:
  - "Introduction to GenAI with Apache Airflow" — https://academy.astronomer.io/introduction-to-genai-with-apache-airflow
  - Note: This repository references and follows patterns shown in Astronomer training videos for educational purposes. The tutorial content (videos, slides, etc.) is copyright Astronomer and subject to Astronomer Academy terms. No Astronomer content is redistributed in this repo; consult the Astronomer site for reuse permissions.

- Udemy Airflow tutorial:
  - Udemy course materials are proprietary to the course author and Udemy. This repo references general knowledge obtained from Udemy-style training for educational purposes only. Do not redistribute Udemy content; consult Udemy terms: https://www.udemy.com/terms/

License for this repository
- This repository is licensed under the Apache License, Version 2.0. See the LICENSE file in this repo for full terms.

Security / secrets
- Do NOT hardcode API keys or other secrets in repository files. Use the .env file or Airflow Connections/variables and store secrets in a secure secret manager.

Adding or changing dependencies
- To add packages required by Airflow tasks/providers (for example `langchain` or `apache-airflow-providers-weaviate`), add them to the root requirements.txt and rebuild the image.
- The Streamlit service already installs include/streamlit/requirements.txt at container startup per docker-compose.yaml.

Where to go from here
- Add your DAGs to the `dags/` directory.
- Configure WEAVIATE and OPENAI keys in .env (see sample_env).
- Use Airflow Connections and Variables to store any other runtime configuration.

Contact / Reporting issues
- Open issues in this repository describing reproductions steps and environment details.