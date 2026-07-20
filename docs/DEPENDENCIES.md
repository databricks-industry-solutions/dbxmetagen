# Dependencies

All packages use permissive licenses (Apache 2.0, MIT, BSD, PSF).

## Python (direct dependencies from pyproject.toml)

| Package | Version | License | Source |
|---------|---------|---------|--------|
| mlflow | 3.11.1 | Apache 2.0 | https://github.com/mlflow/mlflow |
| openai | 1.56.1 | Apache 2.0 | https://github.com/openai/openai-python |
| cloudpickle | 3.1.0 | BSD 3-Clause | https://github.com/cloudpipe/cloudpickle |
| pydantic | 2.10.3 | MIT | https://github.com/pydantic/pydantic |
| ydata-profiling | >=4.12.1,<5 | MIT | https://github.com/ydataai/ydata-profiling |
| databricks-langchain | 0.4.0 | Apache 2.0 | https://github.com/databricks/databricks-ai-bridge |
| databricks-sdk | 0.68.0 | Apache 2.0 | https://github.com/databricks/databricks-sdk-py |
| databricks-vectorsearch | 0.66 | Apache 2.0 | https://github.com/databricks/databricks-vectorsearch |
| openpyxl | 3.1.5 | MIT | https://foss.heptapod.net/openpyxl/openpyxl |
| deprecated | 1.2.13 | MIT | https://github.com/tantale/deprecated |
| pyyaml | 6.0.1 | MIT | https://pypi.org/project/PyYAML/ |
| requests | 2.32.5 | Apache 2.0 | https://github.com/psf/requests |
| nest-asyncio | 1.6.0 | BSD 2-Clause | https://github.com/erdewit/nest_asyncio |

## Optional extras (from pyproject.toml `[project.optional-dependencies]`)

| Extra | Packages | Notes |
|-------|----------|-------|
| `pi` / `pi-lg` | spacy 3.8.7 (MIT), presidio-analyzer 2.2.358 (MIT) | Deterministic PI detection libraries. These install spaCy/Presidio but **not** the spaCy model. |
| `ontology` | rdflib>=6.3.0 (BSD), pyoxigraph>=0.3.0 (Apache 2.0) | Ontology / knowledge-graph support. |

The spaCy language model (`en_core_web_md`) required for PI mode is **not** an extra -- it is a
separate wheel installed from a public GitHub URL pinned in `requirements-pi.txt`
(`pip install -r requirements-pi.txt`). For higher accuracy use `en_core_web_lg` and set
`spacy_model_names` accordingly.

## Python (key transitive dependencies used by the app/agent)

| Package | Version | License | Source |
|---------|---------|---------|--------|
| fastapi | 0.135.1 | MIT | https://github.com/tiangolo/fastapi |
| langgraph | 1.1.3 | MIT | https://github.com/langchain-ai/langgraph |
| langchain | 1.2.13 | MIT | https://github.com/langchain-ai/langchain |
| langchain-community | 0.4.1 | MIT | https://github.com/langchain-ai/langchain |
| grpcio | 1.78.0 | Apache 2.0 | https://github.com/grpc/grpc |
| uvicorn | 0.42.0 | BSD 3-Clause | https://github.com/encode/uvicorn |
| tiktoken | 0.12.0 | MIT | https://github.com/openai/tiktoken |
| scikit-learn | 1.8.0 | BSD 3-Clause | https://github.com/scikit-learn/scikit-learn |
| numpy | 2.1.3 | BSD 3-Clause | https://github.com/numpy/numpy |
| pandas | 2.3.3 | BSD 3-Clause | https://github.com/pandas-dev/pandas |
| scipy | 1.15.3 | BSD 3-Clause | https://github.com/scipy/scipy |
| matplotlib | 3.10.0 | PSF | https://github.com/matplotlib/matplotlib |

## JavaScript (frontend)

| Package | Version | License | Source |
|---------|---------|---------|--------|
| react | ^19.0.0 | MIT | https://github.com/facebook/react |
| react-dom | ^19.0.0 | MIT | https://github.com/facebook/react |
| react-force-graph-2d | ^1.26.0 | MIT | https://github.com/vasturiano/react-force-graph |
| recharts | ^2.15.0 | MIT | https://github.com/recharts/recharts |
| tailwindcss | ^3.4.0 | MIT | https://github.com/tailwindlabs/tailwindcss |
| vite | ^6.0.0 | MIT | https://github.com/vitejs/vite |
