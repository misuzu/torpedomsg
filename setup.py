import os
import re
from setuptools import setup


def read(*paths):
    """Build a file path from *paths* and return the contents."""
    with open(os.path.join(*paths), 'r') as f:
        return f.read()


def find_version(*file_paths):
    """
    Build a path from *file_paths* and search for a ``__version__``
    string inside.
    """
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


version = find_version('torpedomsg/__init__.py')

setup(
    name='torpedomsg',
    version=version,
    description='Flexible Pub-Sub on top of Tornado',
    long_description=read('README.rst'),
    author='misuzu',
    url='https://github.com/misuzu/torpedomsg',
    download_url = 'https://github.com/misuzu/torpedomsg/tarball/{}'.format(version),
    license='MIT',
    packages=['torpedomsg'],
    install_requires=[
        'cbor<2',
        'tornado<5'
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ]
)
