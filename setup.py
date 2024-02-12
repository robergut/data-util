# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()

setup(
    name='cmp',
    version='0.1.0',
    py_modules=['cmp'],
    description='Compare data between two tables from different database source',
    long_description=readme,
    url='https://github.com/robergut/data.cmp',
    packages=find_packages(exclude=('tests', 'docs')),
    entry_points='''
        [console_scripts]
        cmp=cmp:main',
    ''',
)