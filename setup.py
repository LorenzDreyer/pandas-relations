from setuptools import setup, find_packages

setup(
    name='pandas-relations',
    version='0.0.1',
    author='Lorenz Dreyer',
    description='A package to turn pandas dataframes into relational dataframes',
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url='https://github.com/LorenzDreyer/pandas-relations',
    packages=find_packages(),
    install_requires=[i.strip() for i in open("requirements.txt").readlines()],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Development Status :: 2 - Pre-Alpha",
    ],
    keywords='pandas relational, pandas, relational, dataframe, relational dataframe',
)
