# %%
P = {"task.variable_a": "value-used-during-interactive-development"}
# %% tags=["parameters"]
# ---- During automated runs parameters will be injected in this cell ---
# %%
# -----------------------------------------------------------------------
# %%
# Example comment
print(1 + 12 + 123 + 1234 + 12345)
# %%
print(f"""variable_a={P["task.variable_a"]}""")
# %%
