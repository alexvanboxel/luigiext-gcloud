try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name="luigi-gcloud",
    version="0.4",
    description="Extends Luigi with support for the Google Cloud Platform.",
    url="https://github.com/alexvanboxel/luigiext-gcloud",
    author="Alex Van Boxel",
    author_email="alex@vanboxel.be",
    packages=['luigi_gcloud'],
    install_requires=["luigi", "google-api-python-client"]
)
