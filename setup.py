from distutils.core import setup
setup(
  name = 'marketpsych',
  packages = ['marketpsych'],
  version = '0.0.1.2',
  license='MIT',
  description = 'MarketPsych libraries',
  url = 'https://github.com/marketpsych/marketpsych',
  keywords = ['sentiment'],
  install_requires=[
          'dataclasses',
          'datetime',
          'ipywidgets',
          'paramiko'
      ],
  classifiers=[ 
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: MIT License',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
  ]
)
