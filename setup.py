from setuptools import setup, find_packages

setup(
    name='google-sheets-spark-connector',
    version='1.0.0',
    url='https://github.com/liam-clifford/google-sheets-spark-connector',
    author='Liam Clifford',
    author_email='liamclifford4@gmail.com',
    description='A Python library for connecting to Google Sheets API and performing various operations using Spark Dataframes',
    packages=find_packages(),
    install_requires=[
        'authlib==1.2.0',
        'google-auth==2.16.2',
        'gspread==5.7.2',
        'oauth2client==4.1.3',
        'pyspark==3.3.2',
        'google-api-python-client==2.80.0',
    ],
)
