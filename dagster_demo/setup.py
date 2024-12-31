from setuptools import find_packages, setup

setup(
    name="demo",
    packages=find_packages(exclude=["demo_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
