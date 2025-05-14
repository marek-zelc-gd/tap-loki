#!/usr/bin/env python

from setuptools import setup

setup(name='tap-loki',
      version='0.0.2',
      description='Singer.io tap for extracting data from the Loki API',
      author='marek-zelc-gd',
      url='https://github.com/marek-zelc-gd',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_loki'],
      install_requires=[
          'singer-python==5.12.2',
          'promalyze @ git+https://github.com/meshcloud/promalyze.git#egg=promalyze-0.0.3',
          'pytz'
      ],
      entry_points='''
          [console_scripts]
          tap-loki=tap_loki:main
      ''',
      packages=['tap_loki'],
      package_data={
          'tap_loki/schemas': [
              'agents.json'
          ],
      },
      include_package_data=True,
      )
