## Sphinx documentation for `pynb-dag-runner`

The `/sphinx-docs` folder contains documentation and tooling for building
the documentation web site.

## Build locally

```bash
make help
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

This note only address the inital Sphinx setup of the `sphinx-docs`-folder. All
subsequent modifications and contributions (unless otherwise noted) are
licensed under the main project license, see [LICENSE.md](../LICENSE.md)
in the repo root.
