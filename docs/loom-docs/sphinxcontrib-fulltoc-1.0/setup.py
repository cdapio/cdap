# -*- coding: utf-8 -*-

# Bootstrap installation of Distribute
import distribute_setup
distribute_setup.use_setuptools()

from setuptools import setup, find_packages

try:
    long_desc = open('README', 'r').read()
except IOError:
    long_desc = ''

requires = ['Sphinx>=0.6',
            'docutils>=0.6',
            ]

NAME = 'sphinxcontrib-fulltoc'
VERSION = '1.0'

setup(
    name=NAME,
    version=VERSION,
    url='http://sphinxcontrib-fulltox.readthedocs.org',
    license='Apache 2',
    author='Doug Hellmann',
    author_email='doug.hellmann@gmail.com',
    description='Include a full table of contents in your Sphinx HTML sidebar',
    long_description=long_desc,
    zip_safe=False,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Documentation',
        'Topic :: Utilities',
    ],
    platforms='any',
    packages=find_packages(),
    include_package_data=True,
    install_requires=requires,
    namespace_packages=['sphinxcontrib'],
    py_modules=['distribute_setup'],
)
