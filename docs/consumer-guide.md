# Consuming dbxmetagen from Another Project

Three approaches for using dbxmetagen in a separate Databricks project,
ordered from most production-ready to quickest for experimentation.

---

## Option 1: UC Volume Wheel + Recipe YAMLs (recommended for production)

This approach publishes the wheel and notebooks to shared locations, then
uses portable job YAMLs that reference them via variables.

### Step 1: Publish artifacts

From the dbxmetagen repo:

```bash
./publish.sh \
  --profile MY_PROFILE \
  --volume /Volumes/shared_catalog/shared_schema/dbxmetagen \
  --workspace /Workspace/Shared/dbxmetagen
```

This builds the wheel, uploads it to a UC Volume, copies notebooks to the
workspace, and uploads ontology configurations.

### Step 2: Add recipe YAMLs to your project

Copy the recipe(s) you need from `recipes/` into your project's `resources/`:

```bash
cp recipes/metagen.job.yml          ../my-project/resources/
cp recipes/analytics_pipeline.job.yml ../my-project/resources/
cp recipes/build_vector_index.job.yml ../my-project/resources/
```

### Step 3: Define variables in your databricks.yml

```yaml
variables:
  dbxmetagen_wheel:
    default: "/Volumes/shared_catalog/shared_schema/dbxmetagen/dbxmetagen-0.5.3-py3-none-any.whl"
  dbxmetagen_notebooks:
    default: "/Workspace/Shared/dbxmetagen"
  catalog_name:
    default: "my_catalog"
  schema_name:
    default: "my_schema"
  node_type:
    default: "i3.xlarge"
```

### Step 4: Deploy and run

```bash
databricks bundle deploy -t dev
databricks bundle run dbxmetagen_metadata_job -t dev
databricks bundle run dbxmetagen_analytics_pipeline -t dev
```

### Updating

When dbxmetagen is updated, re-run `publish.sh` with the same paths. The
new wheel version will be uploaded and jobs will pick it up on next run.

---

## Option 2: pip install from GitHub (quick for dev/experimentation)

Install the package directly in a notebook:

```python
%pip install git+https://github.com/<org>/dbxmetagen.git@main
```

Or pin to a version tag:

```python
%pip install git+https://github.com/<org>/dbxmetagen.git@v0.5.3
```

Then import and use directly:

```python
from dbxmetagen.main import main
from dbxmetagen.knowledge_base import KnowledgeBaseBuilder
from dbxmetagen.fk_prediction import FKPredictor, FKPredictionConfig
```

**Note**: This gives you the Python library but not the orchestration
notebooks. You'll need to write your own notebook logic or copy the
relevant notebooks from the repo.

### Using in a requirements.txt or pyproject.toml

```
# requirements.txt
dbxmetagen @ git+https://github.com/<org>/dbxmetagen.git@main
```

```toml
# pyproject.toml
[project]
dependencies = [
    "dbxmetagen @ git+https://github.com/<org>/dbxmetagen.git@v0.5.3",
]
```

---

## Option 3: Clone repo + deploy (full deployment including app)

Clone the repo and run the standard deployment:

```bash
git clone https://github.com/<org>/dbxmetagen.git
cd dbxmetagen
cp example.env dev.env
# Edit dev.env with your catalog, schema, warehouse, host
./deploy.sh --target dev --profile MY_PROFILE
```

Use `--no-app` to deploy only the jobs without the web UI:

```bash
./deploy.sh --target dev --no-app
```

This creates all jobs, builds the wheel, and deploys everything as a
Databricks Asset Bundle.

---

## What Each Recipe Provides

| Recipe | What it does |
|--------|-------------|
| `metagen.job.yml` | Generates column/table comments and PI classification |
| `analytics_pipeline.job.yml` | Full pipeline: knowledge bases, graph, embeddings, similarity, ontology, FK prediction, clustering, data quality, vector index |
| `build_vector_index.job.yml` | Builds or refreshes the Vector Search index from the knowledge graph |

## Prerequisites

All approaches require:

- A Databricks workspace with Unity Catalog enabled
- A SQL warehouse (for AI_QUERY in FK prediction and ontology)
- A Vector Search endpoint (for the vector index recipe)
- Access to Foundation Model APIs (for metadata generation)

The target catalog and schema will be populated with dbxmetagen's output
tables (`metadata_generation_log`, `column_knowledge_base`, `graph_nodes`,
`graph_edges`, `fk_predictions`, `ontology_entities`, etc.).
