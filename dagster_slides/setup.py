from setuptools import find_packages, setup

setup(
    name="slides",
    packages=find_packages(exclude=["slides_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
