from setuptools import find_packages, setup

from os import path
import os

# read the contents of your README file
this_directory = path.abspath(path.dirname(__file__))

description = "daskqueue distributed queue package"

# try:
#     import pypandoc

#     long_description = pypandoc.convert_file("README.md", "rst")
# except (IOError, ImportError):
long_description = open("README.md").read()

version = "0.1.2"

setup(
    name="daskqueue",
    packages=find_packages(exclude=["*tests*"]),
    version=version,
    license="MIT",
    description=description,
    project_urls={
        "Source": "https://github.com/AmineDiro/daskqueue/",
    },
    author="Amine Dirhoussi",
    maintainer_email="aminedirhoussi1@gmail.com",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords=["Distributed Task Queue"],
    install_requires=["numpy", "dask>=2022.7.1", "distributed>=2022.7.1"],
    python_requires=">3.6",
    include_package_data=True,
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
