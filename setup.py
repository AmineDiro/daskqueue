from distutils.core import setup
from os import path
import os

# read the contents of your README file
this_directory = path.abspath(path.dirname(__file__))

description = "daskqueue distributed queue package"

long_description = description

version = "0.1.0"

setup(
    name="daskqueue",
    packages=["daskqueue"],
    version=version,
    license="MIT",
    description=description,
    long_description=long_description,
    long_description_content_type="text/x-rst",
    author="Amine Dirhoussi",
    keywords=["Distributed Task Queue"],
    install_requires=["numpy", "dask", "distributed"],
    python_requires=">3.6",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Build Tools",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)
