import pathlib
from setuptools import setup

DIR = pathlib.Path(__file__).parent

README = (DIR / "README.rst").read_text()

setup(
    name='tfinance',
    version='0.1.4',
    description='Tehran Stock Exchange OSINT Tool for Python',
    long_description=README,
    long_description_content_type='text/x-rst',
    packages=['tfinance', 'tfinance.models', 'tfinance.meta', 'tfinance.abc'],
    install_requires=["pandas", "requests", "bs4", "sqlalchemy", "tqdm", "lxml"],
    url='https://github.com/sadeg/tfinance',
    license='GPL3',
    author='Sadiq Rahmati',
    author_email='sadeg.r@protonmail.com',
)
