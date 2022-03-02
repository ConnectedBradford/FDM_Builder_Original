from setuptools import find_packages, setup

setup(
    name="FDMBuilder",
    packages=find_packages(),
    version="0.1.0",
    install_requires=["google-cloud-bigquery", "pandas", "numpy", 
                      "python-dateutil", "pandas-gbq"],
    description="Tools to build FDM Datasets for CYP",
    author="Sam Relins",
    licence="MIT"
)
