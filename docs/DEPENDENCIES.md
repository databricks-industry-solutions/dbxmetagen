# Dependencies

Almost all packages use permissive licenses (Apache 2.0, MIT, BSD, PSF). The one
exception is `psycopg2-binary` (the Postgres/Lakebase driver used by the app), which is
**LGPL v3 with exceptions** -- a weak-copyleft license. It is used unmodified as a
dynamically-linked dependency (not statically linked or modified), which the LGPL permits
for redistribution; it is an app-only dependency and is not part of the core `dbxmetagen`
library wheel.

Versions below are the pinned resolutions from the committed `requirements.txt`
(the source of truth for the notebook/app installs); `uv.lock` is gitignored.

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

## Python (app / agent dependencies — from `apps/dbxmetagen-app/app/requirements.txt`)

The web app declares these directly (versions are the committed `requirements.txt` pins):

| Package | Version | License | Source |
|---------|---------|---------|--------|
| fastapi | 0.135.1 | MIT | https://github.com/fastapi/fastapi |
| uvicorn | 0.42.0 | BSD 3-Clause | https://github.com/encode/uvicorn |
| langgraph | 1.1.3 | MIT | https://github.com/langchain-ai/langgraph |
| langchain-core | 1.2.20 | MIT | https://github.com/langchain-ai/langchain |
| cachetools | 6.2.6 | MIT | https://github.com/tkem/cachetools |
| sqlalchemy | 2.0.48 | MIT | https://github.com/sqlalchemy/sqlalchemy |
| sqlparse | 0.5.5 | BSD 3-Clause | https://github.com/andialbrecht/sqlparse |
| mcp | >=1.9,<2 | MIT | https://github.com/modelcontextprotocol/python-sdk |
| psycopg2-binary | 2.9.x | **LGPL v3 w/ exceptions** | https://github.com/psycopg/psycopg2 |

## Python (key transitive dependencies pulled in by the above)

| Package | Version | License | Source |
|---------|---------|---------|--------|
| langchain | 1.2.13 | MIT | https://github.com/langchain-ai/langchain |
| langchain-community | 0.4.1 | MIT | https://github.com/langchain-ai/langchain |
| grpcio | 1.78.0 | Apache 2.0 | https://github.com/grpc/grpc |
| tiktoken | 0.12.0 | MIT | https://github.com/openai/tiktoken |
| scikit-learn | 1.8.0 | BSD 3-Clause | https://github.com/scikit-learn/scikit-learn |
| numpy | 2.1.3 | BSD 3-Clause | https://github.com/numpy/numpy |
| pandas | 2.3.3 | BSD 3-Clause | https://github.com/pandas-dev/pandas |
| scipy | 1.15.3 | BSD 3-Clause | https://github.com/scipy/scipy |
| matplotlib | 3.10.0 | PSF (matplotlib license, BSD-style) | https://github.com/matplotlib/matplotlib |

## JavaScript (frontend — from `apps/dbxmetagen-app/app/src/package.json`)

All frontend packages are MIT-licensed.

| Package | Version | License | Source |
|---------|---------|---------|--------|
| react | ^19.0.0 | MIT | https://github.com/facebook/react |
| react-dom | ^19.0.0 | MIT | https://github.com/facebook/react |
| @xyflow/react | ^12.3.0 | MIT | https://github.com/xyflow/xyflow |
| react-force-graph-2d | ^1.26.0 | MIT | https://github.com/vasturiano/react-force-graph |
| recharts | ^2.15.0 | MIT | https://github.com/recharts/recharts |
| react-joyride | ^3.1.0 | MIT | https://github.com/gilbarbara/react-joyride |
| dagre | ^0.8.5 | MIT | https://github.com/dagrejs/dagre |
| tailwindcss | ^3.4.0 | MIT | https://github.com/tailwindlabs/tailwindcss |
| vite | ^6.0.0 | MIT | https://github.com/vitejs/vite |
| @vitejs/plugin-react | ^4.3.0 | MIT | https://github.com/vitejs/vite-plugin-react |
| postcss | ^8.5.0 | MIT | https://github.com/postcss/postcss |
| autoprefixer | ^10.4.20 | MIT | https://github.com/postcss/autoprefixer |
