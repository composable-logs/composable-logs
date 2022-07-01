The `/sphinx-docs`-folder contains:
- documentation
- scripts (using Sphinx) for building the documentation web site.

## Build locally
In `<repo-root>/sphinx-docs` directory:

```bash
pip3 install -U sphinx==5.0.2

# generate static html pages into sphinx-docs/build/html directory
make html

# start webserver (in another terminal)
cd build/html
python3 -m http.server --bind 0.0.0.0 4200

# open 127.0.0.1:4200 in local browser
```

## License note
The **initial setup** of the documentation (in this `sphinx-docs`
directory) was created using the Sphinx 5.0.2 documentation quickstart tool,
see [PR108](https://github.com/pynb-dag-runner/pynb-dag-runner/pull/108).

It created:
- the initial versions of `conf.py`, `index.rst` and `Makefile` files;
- the directory structure under `sphinx-docs` (of empty directories).

This setup is used with the understanding that these files are licensed
under the Sphinx project's two clause BSD license, see:

- https://github.com/sphinx-doc/sphinx
- https://github.com/sphinx-doc/sphinx/blob/5.x/sphinx/cmd/quickstart.py
- https://github.com/sphinx-doc/sphinx/tree/5.x/sphinx/templates/quickstart
- https://www.sphinx-doc.org/en/master/man/sphinx-quickstart.html

This note only address the inital files created by the Sphinx quickstart tool.
All subsequent modifications and contributions (unless otherwise noted) are
licensed under the main project license, see [LICENSE.md](../LICENSE.md)
in the repo root.
