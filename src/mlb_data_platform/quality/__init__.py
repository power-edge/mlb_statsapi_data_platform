"""Data quality validation using PyDeequ.

PyDeequ is AWS's data quality library for PySpark, built on top of Apache Deequ.
It provides unit tests for data, allowing you to define constraints and
automatically verify them on large datasets.

Why PyDeequ over Great Expectations:
- Native PySpark integration (better performance)
- Designed for big data (scales to billions of rows)
- Apache 2.0 license (permissive)
- AWS-maintained (reliable)
- Works with our Kappa architecture
"""
