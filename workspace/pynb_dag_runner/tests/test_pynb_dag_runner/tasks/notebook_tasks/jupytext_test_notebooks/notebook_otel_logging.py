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
# test logging of artefact with utf-8 content

logger.log_artefact("README.md", "foobar123")
logger.log_artefact("class_a/binary.bin", bytes(range(256)))

# %%
# test logging of general json-serializable values

logger.log_value("value_str_a", "a")
logger.log_value("value_float_1_23", 1.23)
logger.log_value("value_list_1_2_null", [1, 2, None])
logger.log_value("value_dict", {"a": 123, "b": "foo"})
logger.log_value("value_list_nested", [1, [2, None, []]])

# %%
# test logging using typed loggers

logger.log_boolean("boolean_true", True)
logger.log_int("int_1", 1)
logger.log_float("float_1p23", 1.23)
logger.log_string("string_abc", "abc")

# %%

# Trying to log None with typed-loggers should fail
#
# TODO: Check this.
# TODO: It seems an exception is logged even if we catch the exception
# TODO: what should be behavior?
#
# def assert_fails(f):
#     try:
#         f()
#     except:
#         return
#     raise Exception("function call did not raise Exception")
#
#
# assert_fails(lambda: logger.log_value("value_fail", None))  # type: ignore
# assert_fails(lambda: logger.log_boolean("bool_fail", None))  # type: ignore
# assert_fails(lambda: logger.log_int("int_fail", None))  # type: ignore
# assert_fails(lambda: logger.log_float("float_fail", None))  # type: ignore
# assert_fails(lambda: logger.log_string("string_fail", None))  # type: ignore
# %%
