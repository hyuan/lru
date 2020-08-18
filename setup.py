import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="python-lru",
    version="0.3.0",
    author="Nathan Shearer",
    author_email="shearern@gmail.com",
    description="LRU Cache Implementation",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/shearern/lru",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)