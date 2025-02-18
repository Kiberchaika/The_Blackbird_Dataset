from setuptools import setup, find_packages

setup(
    name="blackbird-dataset",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "click>=8.0.0",
        "tqdm>=4.0.0",
        "webdavclient3>=3.14.0"
    ],
    entry_points={
        'console_scripts': [
            'blackbird=blackbird.cli:cli',
        ],
    },
    author="Your Name",
    author_email="your.email@example.com",
    description="A tool for managing music datasets with multiple components",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/blackbird-dataset",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    python_requires=">=3.7",
    extras_require={
        'dev': [
            'pytest>=6.0.0',
            'pytest-cov>=2.0.0',
            'black>=20.8b1',
            'mypy>=0.800',
        ],
    },
) 