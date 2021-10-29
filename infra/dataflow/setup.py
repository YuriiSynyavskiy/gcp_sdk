import setuptools

REQUIRED_PACKAGES = [
    'apache-beam',
    'apache-beam[gcp]',
    'google-cloud-storage',
    'google-cloud-logging==2.6.0',
    'python-dotenv==0.19.1',
    'google-cloud-storage==1.42.3'
]

setuptools.setup(
    name='Pipeline_PubSub_BigQuery',
    version='0.0.0',
    setup_requires = REQUIRED_PACKAGES,
    install_requires= REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
    include_package_data=True
)