# Databricks python

I created this repository to use in my demo for the 2022 ilionx Data Days. With it I try to demonstrate the
pitfalls of using Databricks in production environments and share some of my ideas about best-practices.

There are some notebook examples that can be imported into Databricks either via the workspace or databricks-cli.

The `databripy` package is meant as an example on how to organise your code into a package. It is not useful for
anything other than the demo and it only includes very basic operations. There are also some unit tests, but these
are far from complete. They only serve as an example.

The repository is organized as follows

```
databripy
├── /.github/workflows -- Pipeline to automatically run unit tests for every push
│
├── /adb_notebooks   -- Databricks notebooks as Python source files
│   ├── /almost_ok   -- Demo project that is almost ok for production
│   ├── /bad_example -- Demo of pitfalls
│   ├── /tools       -- Tools used to create data for the demo
│   └── /main.py     -- Main notebook/entry-point for the package-backed project 
│
├── /config          -- Configuration files for this project
│
├── /databripy       -- The Python package
│
├── /doc             -- Sphinx documentation
│
└── /tests           -- Unit tests
```

# Changelog

### v0.1.0
 - Repository initialization