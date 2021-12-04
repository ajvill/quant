from cryptography.fernet import Fernet
import os


def get_db_secret():
    """
    prepare dB secret for connection
    :return: plain text
    """
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    with open('../../accounts/postgres_key.bin', 'rb') as key_object:
        for line in key_object:
            encryptedkey = line
    with open('../../accounts/postgres_cipheredpwd.bin', 'rb') as pwd_object:
        for line in pwd_object:
            encryptedpwd = line
    cipher_suite = Fernet(encryptedkey)
    # decrypt and convert to string
    unciphered_text = cipher_suite.decrypt(encryptedpwd)
    plain_text_decryptedpwd = bytes(unciphered_text).decode('utf-8')  # convert to string
    key_object.close()
    pwd_object.close()
    return plain_text_decryptedpwd


def secrets():
    return {"host": "127.0.0.1",
            "port": 5432,
            "database": "quant",
            "user": "quant",
            "pass": get_db_secret()}


def elastic() :
    return {"host": "www.pg4e.com",
            "prefix" : "elasticsearch",
            "port": 443,
            "scheme": "https",
            "user": "pg4e_74ce40cdd5",
            "pass": "2110_59d559fc"}


def readonly():
    return {"host": "pg.pg4e.com",
            "port": 5432,
            "database": "readonly",
            "user": "readonly",
            "pass": "readonly_password"}


# Return a psycopg2 connection string

# import hidden
# secrets = hidden.readonly()
# sql_string = hidden.psycopg2(hidden.readonly())

# 'dbname=pg4e_data user=pg4e_data_read password=pg4e_p_d5fab7440699124 host=pg.pg4e.com port=5432'


def psycopg2(secrets) :
     return ('dbname='+secrets['database']+' user='+secrets['user']+
        ' password='+secrets['pass']+' host='+secrets['host']+
        ' port='+str(secrets['port']))

# Return an SQLAlchemy string

# import hidden
# secrets = hidden.readonly()
# sql_string = hidden.alchemy(hidden.readonly())

# postgresql://pg4e_data_read:pg4e_p_d5fab7440699124@pg.pg4e.com:5432/pg4e_data


def alchemy(secrets) :
    return ('postgresql://'+secrets['user']+':'+secrets['pass']+'@'+secrets['host']+
        ':'+str(secrets['port'])+'/'+secrets['database'])

