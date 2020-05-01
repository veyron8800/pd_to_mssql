from distutils.core import setup
setup(
    name='pd_to_mssql',
    packages=['pd_to_mssql'],
    version='0.1.4',
    license='MIT',
    description='Quick upload of pandas dataframes to Microsoft SQL Server',
    author='Andrew Reis',
    author_email='areis@taylorcorp.com',
    url='https://github.com/areisTaylorCorp/pd_to_mssql',
    download_url='https://github.com/areisTaylorCorp/pd_to_mssql/archive/v0.1.4.tar.gz',
    keywords=['pandas', 'dataframe', 'mssql', 'sql', 'to_sql'],
    install_requires=['pandas', 'pyodbc'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8'
    ]
)
