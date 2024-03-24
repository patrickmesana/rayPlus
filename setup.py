from setuptools import setup, find_packages

setup(
    name='rayPlus',
    version='0.1.0.dev',
    packages=find_packages(),
    install_requires=[
        # List your project's dependencies here
        # e.g., 'requests >= 2.23.0',
    ],
    # Additional metadata about your package
    author='Patrick Mesana',
    author_email='patrick.mesana@hec.ca',
    description='Useful functions for parallel and distributed computing with Ray',
    # More fields can be added as needed
)