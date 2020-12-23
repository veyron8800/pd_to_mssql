from setuptools import setup

setup(
    name='pd_to_mssql',
    packages=['pd_to_mssql'],
    version='0.2.1',
    license='MIT',
    description='Quick upload of pandas dataframes to Microsoft SQL Server',
    author='Andrew Reis',
    author_email='veyron8800@gmail.com',
    url='https://github.com/veyron8800/pd_to_mssql',
    download_url='https://github.com/veyron8800/pd_to_mssql/archive/v0.2.1.tar.gz',
    keywords=['pandas', 'dataframe', 'mssql', 'sql', 'to_sql', 'df', 'pyodbc'],
    install_requires=['pandas', 'pyodbc'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8'
    ],
    long_description_content_type='text/markdown',
    long_description=open('README.md', 'r').read()
)
