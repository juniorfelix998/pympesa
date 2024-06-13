from setuptools import find_packages, setup

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name="py_mpesa_daraja_api",
    version="0.0.1",
    description="Mpesa library that can be used to make api calls to mpesa daraja API",
    packages=find_packages(),
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/juniorfelix998/pympesa",
    author="Felix Okoth",
    author_email="juniorfelix998@gmail.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.10",
        "Operating System :: OS Independent",
    ],
    install_requires=["pydantic", "requests"],
    extras_require={
        "dev": ["twine>=5.1.0"],
    },
    python_requires=">=3.10",
)
