from setuptools import setup, find_packages

setup(
    name="glue_bundle",
    version="1.0",
    packages=find_packages(),
    install_requires=[
        "pydantic==2.9.2",
        "annotated-types==0.7.0",
        "typing-extensions==4.15.0",
    ],
)
