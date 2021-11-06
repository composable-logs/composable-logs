# %%
from typing import Dict, Any

P: Dict[str, Any] = {}
# %% tags=["parameters"]
# ---- During automated runs parameters will be injected in this cell ---
# %%
# -----------------------------------------------------------------------
print(1 + 12 + 123)
# %%
print(P)
# %%
# An example comment

if P["retry.nr"] in [0, 1, 2]:
    raise Exception("Thrown from notebook!")
else:
    print(f"success run at retry {P['retry.nr']}!")
# %%
# %%
