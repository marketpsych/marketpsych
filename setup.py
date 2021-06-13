from distutils.core import setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
  name='marketpsych',
  packages=['marketpsych'],
  version='0.0.2',
  author='MarketPsych Data',
  description='Python libraries for working with MarketPsych\'s feeds',
  long_description=long_description,
  long_description_content_type="text/markdown",
  license='MIT',
  url = 'https://github.com/marketpsych/marketpsych',
  keywords = ['sentiment'],
  install_requires=[
          'dataclasses',
          'datetime',
          'ipywidgets',
          'matplotlib',
          'pandas',
          'paramiko'
      ],
  classifiers=[ 
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: MIT License',
    'Programming Language :: Python :: 3'
  ],
  python_requires='>=3.6'
)
