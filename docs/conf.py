"""Sphinx configuration for dandi-compute-code documentation."""

import importlib.metadata

# -- Project information -----------------------------------------------------

project = "dandi-compute-code"
author = "Cody Baker"
copyright = f"2024, {author}"

try:
    release = importlib.metadata.version("dandi-compute-code")
except importlib.metadata.PackageNotFoundError:
    release = "unknown"

version = ".".join(release.split(".")[:2])

# -- General configuration ---------------------------------------------------

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx_autodoc_typehints",
]

autosummary_generate = True
autodoc_typehints = "description"
autodoc_member_order = "bysource"
napoleon_google_docstring = True
napoleon_numpy_docstring = True

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# -- Options for HTML output -------------------------------------------------

html_theme = "pydata_sphinx_theme"
html_title = "dandi-compute-code"

html_theme_options = {
    "github_url": "https://github.com/dandi-compute/code",
    "use_edit_page_button": False,
    "show_toc_level": 2,
    "navigation_with_keys": False,
    "header_links_before_dropdown": 0,
}

html_static_path = ["_static"]
