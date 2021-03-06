#  Copyright (c) 2021 KTH Royal Institute of Technology
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import importlib.util

import setuptools

# TODO: readme?
# with open('./README.md', 'r') as fp:
#     long_description = fp.read()

with open('./requirements.txt', 'r') as fp:
    reqs = fp.readlines()

# parse the version file
spec = importlib.util.spec_from_file_location('version',
                                              './exprec/_version.py')
version = importlib.util.module_from_spec(spec)
spec.loader.exec_module(version)

setuptools.setup(
    name='exprec',
    version=version.__version__,
    author='KTH Royal Institute of Technology',
    author_email='molguin@kth.se',
    description='Experiment recording library built on top of PyTables and '
                'HDF5.',  # FIXME
    long_description='',  # TODO
    long_description_content_type='text/markdown',
    url='https://github.com/KTH-EXPECA/ExperimentRecorder',
    packages=setuptools.find_packages(),
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Topic :: System :: Networking',
        'Topic :: System :: Benchmark',
        'Topic :: System :: Emulators'
    ],
    install_requires=reqs,
    extras_require={},
    entry_points={},  # TODO, for server in the future.
    python_requires='>=3.8',
    license='Apache v2'
)
