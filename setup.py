"""Python setup.py for pod package"""
import io
import os
from setuptools import Extension, find_packages, setup
from Cython.Build import cythonize
from Cython.Compiler import Options


def read(*paths, **kwargs):
    """Read the contents of a text file safely.
    >>> read("pod", "VERSION")
    '0.1.0'
    >>> read("README.md")
    ...
    """

    content = ""
    with io.open(
        os.path.join(os.path.dirname(__file__), *paths),
        encoding=kwargs.get("encoding", "utf8"),
    ) as open_file:
        content = open_file.read().strip()
    return content


def read_requirements(path):
    return [
        line.strip()
        for line in read(path).split("\n")
        if not line.startswith(('"', "#", "-", "git+"))
    ]


# Modules to be compiled and include_dirs when necessary
extensions = [
    Extension(
        "pod.memo",
        ["pod/memo.py"],
    ),
    # Extension(
    #     "pod.storage",
    #     ["pod/storage.py"],
    # ),
    # Extension(
    #     "pod.pickling",
    #     ["pod/pickling.py"],
    # ),
    # Extension(
    #     "pod._pod",
    #     ["pod/_pod.py"],
    # ),
]


setup(
    name="pod",
    version=read("pod", "VERSION"),
    description="project_description",
    url="https://github.com/illinoisdata/pod/",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    author="mIXs222;SumayT9",
    packages=find_packages(exclude=["tests", ".github"]),
    install_requires=read_requirements("requirements.txt"),
    ext_modules=cythonize(extensions, compiler_directives={"language_level": 3, "profile": False}),
)
