from setuptools import setup

setup(
    name="aseg_gdf2",
    packages=("aseg_gdf2", ),
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    description="Python package to help read ASEG GDF2 packages ",
    long_description=open("README.md", mode="r").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/kinverarity1/aseg_gdf2",
    author="Kent Inverarity",
    author_email="kinverarity@hotmail.com",
    license="MIT",
    classifiers=(
        "Topic :: Scientific/Engineering",
    ),
    keywords="science geophysics",
    install_requires=("pandas", "dask"),
)
