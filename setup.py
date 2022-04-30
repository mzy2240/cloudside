from setuptools import setup, find_packages
import pathlib
import pkg_resources

PACKAGE_DATA = {
    "cloudside.tests.data": ["*.txt", "*.png", "*.dat", "*.csv"],
    "cloudside.tests.baseline_images.viz_tests": ["*.png"],
}

DESCRIPTION = "cloudside - download, assess, and visualize weather data"

with pathlib.Path('requirements.txt').open() as requirements_txt:
    install_requires = [
        str(requirement)
        for requirement
        in pkg_resources.parse_requirements(requirements_txt)
    ]


setup(
    name="cloudside",
    version="0.3",
    author="Zeyu Mao",
    author_email="zeyumao2@tamu.edu",
    url="http://python-metar.sourceforge.net/",
    description=DESCRIPTION,
    long_description=DESCRIPTION,
    package_data=PACKAGE_DATA,
    download_url="http://sourceforge.net/project/platformdownload.php?group_id=134052",
    license="BSD 3-Clause",
    packages=find_packages(exclude=[]),
    platforms="Python 3.6 and later.",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.6",
        "Topic :: Scientific/Engineering :: Atmospheric Science",
        "Topic :: Scientific/Engineering :: Visualization",
    ],
    entry_points={"console_scripts": ["cloudside=cloudside.cli:main"]},
    install_requires=install_requires
)
