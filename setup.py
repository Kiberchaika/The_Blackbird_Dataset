from setuptools import setup, find_packages

setup(
    name="blackbird-dataset",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "webdavclient3",
        "tqdm",
        "click",
        "pydantic"
    ],
    entry_points={
        'console_scripts': [
            'blackbird=blackbird.cli:main',
        ],
    },
    author="K4",
    description="A tool for managing music datasets with multiple components",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/blackbird-dataset",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.8",
) 