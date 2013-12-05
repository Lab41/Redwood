#!/usr/bin/env python

from setuptools import setup
from setuptools.command.install import install
import os

class MyInstall(install):

    def run(self):
        install.run(self)
        print "Installing dependencies"

setup(
    name='RedwoodUtility',
    version='0.1.0',
    author='Lab41',
    author_email='paulm@lab41.org',
    description='A project that implements statistical methods for identifying anomalous files.',
    url='http://lab41.github.io/Redwood',
    packages=['redwood', 'redwood.filters', 'redwood.shell','redwood.io','redwood.helpers', 'redwood.connection', 'redwood.foundation'],
    scripts=['bin/redwood'],
    license='LICENSE.txt',
    long_description=open('README.md').read(),
    keywords='redwood stats statistics anomalies'.split(),
    cmdclass={'install': MyInstall},
    classifiers=[
        'Programming Language :: Python',
        'Operating System :: POSIX :: Linux',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Environment :: Other Environment'
    ],
    data_files=[
        ('', ['LICENSE.txt'])
    ],
    install_requires=[
        'numpy',
        'scipy',
    ]
)
