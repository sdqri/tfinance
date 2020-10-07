import pathlib
from setuptools import setup

DIR = pathlib.Path(__file__).parent

README = (DIR / "README.md").read_text()

setup(
    name='tfinance',
    version='0.1.0',
    description='A Simple Tehran Stock Exchange Scraping & OSINT tool',
    long_description=README,
    long_description_content_type='text/markdown',
    packages=['tfinance'],
    install_requires=["pandas", "requests", "bs4", "sqlalchemy", "tqdm"],
    url='https://github.com/sadeg/tfinance',
    license='GPL3',
    author='Sadiq Rahmati',
    author_email='sadeg.r@protonmail.com',
)
