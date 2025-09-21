from setuptools import setup, find_packages

setup(
    name='visual_asset_browser',
    version='1.0',
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
)
