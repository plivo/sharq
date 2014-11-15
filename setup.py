# -*- coding: utf-8 -*-
# Copyright (c) 2014 Plivo Team. See LICENSE.txt for details.
from setuptools import setup

setup(
    name='SharQ',
    version='0.3.0',
    url='https://github.com/plivo/sharq',
    author='Plivo Team',
    author_email='hello@plivo.com',
    packages=['sharq'],
    package_data={
        'sharq': ['scripts/lua/*.lua']
    },
    license=open('LICENSE.txt').read(),
    description='An API queueing system built at Plivo.',
    long_description=open('README.md').read(),
    install_requires=[
        'redis==2.10.1',
        'msgpack-python==0.4.2'
    ],
    classifiers = [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
