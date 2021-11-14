# %%
P = {"task_parameter.variable_a": "value-used-during-interactive-development"}
# %% tags=["parameters"]
# ---- During automated runs parameters will be injected in this cell ---
# %%
# -----------------------------------------------------------------------
# %%
# Example comment
print(1 + 12 + 123)
# %%
print(f"""variable_a={P["task_parameter.variable_a"]}""")
# %%
