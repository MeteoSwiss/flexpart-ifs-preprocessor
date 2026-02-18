import os

extensions = ["sphinx.ext.napoleon", "autoapi.extension", "sphinx.ext.viewcode"]

project = "flexpart-ifs-preprocessor"
copyright = "2026, MeteoSwiss"
author = "Victoria Cherkas"

version = os.getenv("VERSION", default="")
build_id = os.getenv("BUILD_ID", default="")
release = f"{version}-{build_id}"

exclude_patterns = ["_build"]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# html template settings
html_title = project
html_theme = "pydata_sphinx_theme"

html_last_updated_fmt = "%d.%m.%Y"

html_theme_options = {
    "show_nav_level": 2,
    "navigation_depth": 4,
    "show_toc_level": 1,
    "secondary_sidebar_items": ["page-toc"],
    "logo": {
        "text": project,
        "image_light": "_static/app-icon_meteoswiss_rounded_rgb.png",
        "image_dark": "_static/app-icon_meteoswiss_rounded_rgb.png",
    },
    "footer_start": ["version", "last-updated", "copyright"],
    "footer_end": ["theme-version", "sphinx-version"]
}

# Disable left side navigation of specific pages, since they are empty
# (BUG in theme: https://github.com/pydata/pydata-sphinx-theme/issues/1662)
html_sidebars = {"usage": [], "changelog": [], "migration_guide": [], "readme": []}

napoleon_use_param = False  # improve parameters description
add_module_names = False  # avoid the display of redundant module names

autoapi_dirs = ["../flexpart_ifs_preprocessor"]
autoapi_options = ["members", "undoc-members", "show-inheritance", "show-module-summary", "imported-members"]
