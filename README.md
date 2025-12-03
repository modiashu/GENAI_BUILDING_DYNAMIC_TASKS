# GENAI_BUILDING_DYNAMIC_TASKS

A sample project demonstrating how to build dynamic GenAI workflows using Apache Airflow, Weaviate and Streamlit. This repository contains an Airflow compose setup, example DAGs, and a Streamlit UI for exploring model results.

Important: this project demonstrates Retrieval-Augmented Generation (RAG) — the pattern of retrieving relevant documents or embeddings from a vector database (Weaviate) and then using an OpenAI LLM to generate answers conditioned on that retrieved context. The repository shows how to orchestrate RAG pipelines with Airflow, persist vectors in Weaviate, and present results via Streamlit.

Key goals:
- Run Apache Airflow (v3.0.0) locally using a compose stack.
- Use Weaviate as a vector database for GenAI workflows.
- Provide a Streamlit frontend for model results and exploration.
- Demonstrate Retrieval-Augmented Generation (RAG) with OpenAI LLMs as the generation step.
- Prefer Podman (free) for container commands instead of Docker.

What this repository demonstrates (high level)
- How to orchestrate dynamic GenAI tasks in Airflow that implement a RAG pipeline:
  1. Ingest source documents (PDFs, text files, etc.) or structured data.
  2. Vectorize text chunks using a vectorizer (configurable; can use OpenAI embedding endpoints or other vectorizers).
  3. Store and index embeddings in Weaviate (vector DB) for semantic retrieval.
  4. Perform a similarity search (retrieval) against Weaviate at runtime to gather relevant context.
  5. Call an LLM (the example setup uses OpenAI) and pass retrieved context as part of the prompt to generate the final answer. This is the "generation" step in RAG.
  6. Persist generation results and metadata for downstream consumption and visualization in Streamlit.
- This pattern separates retrieval (Weaviate) from generation (LLM) so that the LLM can produce grounded responses using documents from your corpus.

Prerequisites
- Podman (free edition) installed and configured (podman and podman-compose / podman compose).
- Python 3.11
- Optional: VPN if your environment requires it to reach APIs (e.g., whitelisted OpenAI endpoints).
- An OpenAI API key (if using OpenAI vectorizers or the OpenAI provider). This repository uses the OpenAI provider in examples to show how to incorporate a hosted LLM for the generation step of a RAG pipeline.

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
   (Dockerfile in this repo already copies requirements.txt and runs pip install -r requirements.txt)

7. Create .env file as needed
   See sample_env in this repository for variables to set. Important environment variables include OPENAI_API_KEY and WEAVIATE connection settings (host, api key if used). Use Airflow Connections and Variables to keep secrets out of source files.

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
- dags/ — place your Airflow DAGs here (this is where you implement the orchestration steps described above).
- README.md — this file (updated to call out RAG + OpenAI LLM usage).

Notes about Podman vs Docker
- This repository and the instructions use Podman (podman compose / podman-compose) for container orchestration locally. Replace any `docker` or `docker-compose` invocation with the `podman` equiv:
  - podman compose up -d
  - podman compose up --build -d
  - podman compose down

Third-party tutorials and attribution / license notes
- Astronomer Academy tutorial:
  - "Introduction to GenAI with Apache Airflow" — https://academy.astronomer.io/introduction-to-genai-with-apache-airflow
  - Note: This repository references and follows patterns shown in Astronomer training videos for educational purposes. The tutorial content (videos, slides, etc.) is copyright Astronomer and subject to their terms.

- Udemy Airflow tutorial:
  - Udemy course materials are proprietary to the course author and Udemy. This repo references general knowledge obtained from Udemy-style training for educational purposes only. Do not redistribute proprietary course content.

License for this repository
- This repository is licensed under the Apache License, Version 2.0. See the LICENSE file in this repo for full terms.

Security / secrets
- Do NOT hardcode API keys or other secrets in repository files. Use the .env file or Airflow Connections/variables and store secrets in a secure secret manager.
- For RAG workflows, ensure that any private documents you ingest are handled per your data protection and retention policies. When using hosted models (OpenAI), check your organization's policy about sending sensitive data to third-party APIs.

Adding or changing dependencies
- To add packages required by Airflow tasks/providers (for example `langchain` or `apache-airflow-providers-weaviate`), add them to the root requirements.txt and rebuild the image.
- The Streamlit service already installs include/streamlit/requirements.txt at container startup per docker-compose.yaml.

Suggested DAG flow for a RAG pipeline (concrete steps to implement in your Airflow DAGs)
- Task 1 — Source ingestion: extract raw documents, split into chunks with metadata.
- Task 2 — Embedding: compute embeddings for each chunk (e.g., OpenAI embeddings or another encoder).
- Task 3 — Upsert into Weaviate: insert vectors and metadata to enable semantic search.
- Task 4 — Query + Retrieval: given a user query, perform a similarity search in Weaviate to get the top-k relevant chunks.
- Task 5 — Prompt construction: assemble retrieved chunks into a prompt or context window; apply any prompt templates and system instructions.
- Task 6 — LLM generation: call the OpenAI LLM (or other model) to generate an answer conditioned on the retrieved context (this is the RAG generation step).
- Task 7 — Persist & serve: store the generated response, logs, and provenance; surface results in Streamlit or another UI.

Where to go from here
- Add your DAGs to the `dags/` directory implementing the steps above.
- Configure WEAVIATE and OPENAI keys in .env (see sample_env).
- Use Airflow Connections and Variables to store any other runtime configuration.
- If you want to swap out the generation model, isolate the model-calling task so you can replace OpenAI with another provider without changing retrieval or storage logic.

Contact / Reporting issues
- Open issues in this repository describing reproductions steps and environment details.
