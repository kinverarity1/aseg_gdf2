import os
from setuptools import setup

about = {}

try:
    import pypandoc
except:
    print('pypandoc not found, using Markdown instead')
    with open('README.md') as f:
        about['readme'] = f.read()
    with open('CHANGELOG.md') as f:
        about['changelog'] = f.read()
else:
    about['readme'] = pypandoc.convert('README.md', 'rst')
    about['readme'] = about['readme'].replace('\r', '')
    about['changelog'] = pypandoc.convert('CHANGELOG.md', 'rst')
    about['changelog'] = about['changelog'].replace('\r', '')
    
with open(os.path.join(os.path.dirname(__file__), 'aseg_gdf2', '__version__.py'), 'r') as f:
    exec(f.read(), about)

with open(os.path.join(os.path.dirname(__file__), 'requirements.txt'), 'r') as f:
    requirements = f.read().splitlines()

WHITESPACE_SEP_KEYWORDS = 'geophysics'

CLASSIFIERS = [
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.2",
    "Programming Language :: Python :: 3.3",
    "Programming Language :: Python :: 3.4",
    "Programming Language :: Python :: 3.5",
    "Programming Language :: Python :: 3.6",
    "Topic :: Scientific/Engineering",
    ]


setup(
    name=about['__title__'],
    version=about['__version__'],
    description=about['__description__'],
    long_description=about['readme'] + '\n\n' + about['changelog'],
    url=about['__url__'],
    author=about['__author__'],
    author_email=about['__author_email__'],
    license=about['__license__'],
    classifiers=CLASSIFIERS,
    keywords=WHITESPACE_SEP_KEYWORDS,
    packages=["aseg_gdf2", ],
    install_requires=requirements,
    entry_points={'console_scripts': [
        ]})
