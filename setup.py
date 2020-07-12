from setuptools import setup, find_packages
import cevast

with open('README.md', 'r') as fh:
    long_description = fh.read()

setup(
    name='cevast',
    version=cevast.__version__,
    author='Radim Podola',
    author_email='rpodola@gmail.com',
    description=cevast.__doc__,
    long_description=long_description,
    long_description_content_type='text/markdown',
    license = "MIT",
    url='https://github.com/crocs-muni/cert-validation-stats',
    packages=find_packages(),
    classifiers=[
         'Programming Language :: Python :: 3',
         'License :: OSI Approved :: MIT License',
         'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
    install_requires=[
        'Click',
    ],
    entry_points='''
        [console_scripts]
        cevast=cevast.cli:cli
        certdb=cevast.certdb.cli:certdb_group
        datasetrepository=cevast.dataset.cli:dataset_repository_group
        datasetmanager=cevast.dataset.cli:manager_group
    ''',
)
