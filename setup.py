from setuptools import setup, find_packages
import os
import codecs
import re


def read(here: str, *parts) -> str:
    """
    Build an absolute path from *parts* and and return the contents of the
    resulting registration.  Assume UTF-8 encoding.
    """
    with codecs.open(os.path.join(here, *parts), 'rb', 'utf-8') as f:
        return f.read()


def find_meta(meta_file: str, meta: str) -> str:
    """
    Extract __*meta*__ from meta_file.
    """
    meta_match = re.search(
        r"^__{meta}__ = ['\"]([^'\"]*)['\"]".format(meta=meta),
        meta_file, re.M
    )
    if meta_match:
        return meta_match.group(1)
    raise RuntimeError('Unable to find __{meta}__ string.'.format(meta=meta))


def get_install_requirements(req_file: str) -> list:
    """Get install requirements from the requirements.txt file
    Lines starting with # are skipped
    """
    reqs = []
    for line in req_file.split():
        if not line.startswith('#'):
            reqs.append(line)
    return reqs


HERE = os.path.abspath(os.path.dirname(__file__))
META_PATH = os.path.join('databripy', '__init__.py')
REQ_PATH = os.path.join('requirements.txt')
META_FILE = read(HERE, META_PATH)
REQ_FILE = read(HERE, REQ_PATH)

setup(
    name=find_meta(META_FILE, 'title'),
    version=find_meta(META_FILE, 'version'),
    description=find_meta(META_FILE, 'description'),
    long_description=read(HERE, 'README.md'),
    license=find_meta(META_FILE, 'license'),
    author=find_meta(META_FILE, 'author'),
    author_email=find_meta(META_FILE, 'email'),
    maintainer=find_meta(META_FILE, 'author'),
    maintainer_email=find_meta(META_FILE, 'email'),
    url=find_meta(META_FILE, 'uri'),
    python_requires='~=3.8',
    install_requires=get_install_requirements(REQ_FILE),
    packages=find_packages(exclude=["test", "test.*"]),
)
