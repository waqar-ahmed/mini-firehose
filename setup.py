######################################
#   Build your package:
######################################
# python setup.py sdist bdist_wheel

######################################
#   Install dependencies
######################################
# pip install .         # Install dependencies mentioned in install_requires
# pip install .[test]   # Install dependencies mentioned in extras_require[test]

from setuptools import setup, find_packages

# Read requirements
install_requires = open('requirements.txt').read().splitlines()
test_requires = open('requirements-test.txt').read().splitlines()

setup(
    name="mini-firehose",
    version="0.1.0",
    author="Waqar Ahmed",
    author_email="wqrahd@gmail.com",
    description="A lightweight data buffering and delivery system",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url="https://github.com/waqar-ahmed/mini-firehose",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.10',
    install_requires=install_requires,
    extras_require={
        'test': test_requires
    },
    entry_points={
         "console_scripts": [
        "mini-firehose=mini_firehose.cli:main",
    ]}
)
