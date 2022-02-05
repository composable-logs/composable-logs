# %%
P = {"task.variable_a": "value-used-during-interactive-development"}
# %% tags=["parameters"]
# ---- During automated runs parameters will be injected in this cell ---
# %%
# -----------------------------------------------------------------------

# %%
# Example comment

# Currently, pynb_dag_runner package is imported in editable mode in dev-notebook,
# but this is not accessible when running unit tests in ci.
import sys

sys.path.append("/home/host_user/workspace/pynb_dag_runner")
from pynb_dag_runner.tasks.task_opentelemetry_logging import PydarLogger

# %%

logger = PydarLogger(P)

# %%

logger.log_artefact("from_notebook.txt", "foobar123")

# print(1 + 12 + 123)
# %%
print(f"""variable_a={P["task.variable_a"]}""")
# %%
