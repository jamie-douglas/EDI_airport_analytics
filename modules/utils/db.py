#modules/utils/db.py
import urllib
import sys
from sqlalchemy import create_engine
from modules.config import DSN, USERNAME

def get_engine(dsn: str = DSN, username: str =USERNAME):
    """
    Creates and returns a SQLALchemy engine using an ODBC DSN and an explicit username. 

    Parameters
    ----------
    dsn: str
        The ODBC DSN name configured on the system.
    username: str
        Username to include in the ODBC connection string. 
    
    Returns
    ---------
    sqlalchemy.engine.Engine
        A SQLAlchemy engine configured for the given DSN and username. 
    """

    try:
        conn_str = f"DSN={dsn};UID={username};"
        params = urllib.parse.quote_plus(conn_str)
        return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    except Exception as ex:
        print(f"Error creating SQLAlchemy engine: {ex}")
        sys.exit(1)