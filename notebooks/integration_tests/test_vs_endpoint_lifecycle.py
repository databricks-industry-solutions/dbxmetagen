# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Test: VS Endpoint Create/Delete Lifecycle
# MAGIC
# MAGIC Validates that `ensure_endpoint` correctly creates a Vector Search endpoint
# MAGIC using the `EndpointType` enum (not a raw string), then cleans up.

# COMMAND ----------

import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import EndpointType

w = WorkspaceClient()
TEST_EP_NAME = f"dbxmetagen-test-lifecycle-{int(time.time())}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create endpoint via SDK with EndpointType enum

# COMMAND ----------

w.vector_search_endpoints.create_endpoint(
    name=TEST_EP_NAME,
    endpoint_type=EndpointType.STANDARD,
)
print(f"Created endpoint '{TEST_EP_NAME}'")

ep = w.vector_search_endpoints.wait_get_endpoint_vector_search_endpoint_online(TEST_EP_NAME)
assert ep.endpoint_status.state.value == "ONLINE", (
    f"Expected ONLINE, got {ep.endpoint_status.state}"
)
print(f"Endpoint is ONLINE (num_indexes={ep.num_indexes})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Verify ensure_endpoint is idempotent (get succeeds, no re-create)

# COMMAND ----------

import sys
sys.path.append("../../src")

from dbxmetagen.vector_index import VectorIndexConfig, VectorIndexBuilder

config = VectorIndexConfig(
    catalog_name="__unused__",
    schema_name="__unused__",
    endpoint_name=TEST_EP_NAME,
)
builder = VectorIndexBuilder(config=config, spark=spark)
returned_name = builder.ensure_endpoint()
assert returned_name == TEST_EP_NAME, f"Expected '{TEST_EP_NAME}', got '{returned_name}'"
print("ensure_endpoint returned existing endpoint correctly")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Cleanup -- delete the test endpoint

# COMMAND ----------

w.vector_search_endpoints.delete_endpoint(TEST_EP_NAME)
print(f"Deleted endpoint '{TEST_EP_NAME}'")

# Confirm deletion
try:
    w.vector_search_endpoints.get_endpoint(TEST_EP_NAME)
    raise AssertionError(f"Endpoint '{TEST_EP_NAME}' still exists after delete")
except Exception as e:
    if "not found" in str(e).lower() or "does_not_exist" in str(e).lower() or "RESOURCE_DOES_NOT_EXIST" in str(e):
        print("Confirmed endpoint deleted")
    else:
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify ensure_endpoint creates from scratch

# COMMAND ----------

TEST_EP_NAME_2 = f"{TEST_EP_NAME}-2"
config2 = VectorIndexConfig(
    catalog_name="__unused__",
    schema_name="__unused__",
    endpoint_name=TEST_EP_NAME_2,
)
builder2 = VectorIndexBuilder(config=config2, spark=spark)
returned = builder2.ensure_endpoint()
assert returned == TEST_EP_NAME_2, f"Expected '{TEST_EP_NAME_2}', got '{returned}'"
print(f"ensure_endpoint created new endpoint '{TEST_EP_NAME_2}' successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

w.vector_search_endpoints.delete_endpoint(TEST_EP_NAME_2)
print(f"Deleted endpoint '{TEST_EP_NAME_2}'")
print("ALL PASSED")
