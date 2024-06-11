from app.app import app
from app.utils.log import Log
import os
import pathlib


def main():
    os.environ["APP_INSTALLDIR"] = os.path.dirname(os.path.abspath(__file__))
    BASEDIR = "./test_case"
    os.environ["APP_BASEDIR"] = str(BASEDIR)
    Log.configure_logging(BASEDIR)
    app()


if __name__ == "__main__":
    main()
