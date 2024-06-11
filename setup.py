from setuptools import setup, find_packages

long_description = "sintetizador_decomp"

requirements = []
with open("requirements.txt", "r") as fh:
    requirements = fh.readlines()


setup(
    name="sintetizador_decomp",
    version="1.0.0",
    author="Rogerio Alves",
    author_email="rogerioalves.ee@gmail.com",
    description="sintetizador_decomp",
    long_description=long_description,
    install_requires=requirements,
    packages=find_packages(),
    py_modules=["main", "app"],
    python_requires=">=3.8",
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "Operating System :: OS Independent",
    ],
    entry_points="""
        [console_scripts]
        sintetizador-decomp=main:main
    """,
)
