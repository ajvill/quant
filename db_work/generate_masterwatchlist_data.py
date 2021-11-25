import time
import psycopg2
import hidden
import myutils
import numpy as np


def get_conn():
    secrets = hidden.secrets()
    connection = psycopg2.connect(host=secrets['host'], port=secrets['port'],
                            database=secrets['database'],
                            user=secrets['user'],
                            password=secrets['pass'],
                            connect_timeout=10)
    return connection


def generate_random_symbols(num_samples, sample_size=10, randomize_sample_size=False):
    """
    Random generator to simulate symbols for securities, ASCII integer 65-90 (A-Z)
    :param num_samples: number of entries returned back in the list
    :param sample_size: number characters in the string
    :param randomize_sample_size: each entries size will be randomized between 1 and sample_size
    :return:
    """
    entry_list = []

    for i in range(1, num_samples+1):
        if not randomize_sample_size:
            entry = ''.join([chr(np.random.randint(65, 90)) for x in range(1, sample_size+1)])
        else:
            entry = ''.join([chr(np.random.randint(65, 90)) for x in range(1, np.random.randint(1, sample_size) + 1)])
        entry_list.append(entry)
    return entry_list


def main(num_samples=100000, sample_size=20, random_sample_size=False):
    conn = get_conn()

    cur = conn.cursor()
    cur.execute('''DROP TABLE master_watchlist2''')
    conn.commit()

    cur = conn.cursor()
    # NOTE symbol should be UNIQUE but for testing purposes removing constraint
    sql = '''
        CREATE TABLE master_watchlist2 (
            id SERIAL, 
            symbol VARCHAR(128) NOT NULL,
            name VARCHAR(128) UNIQUE NOT NULL,
            sector VARCHAR(128) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY(id))
    '''
    cur.execute(sql)
    conn.commit()

    # create a random list of security names for testing only all entries are artificial
    start = time.time()
    entry_list = generate_random_symbols(num_samples, sample_size, random_sample_size)

    count = 0
    for entry in entry_list:
        count += 1
        name = 'fucker#{}'.format(count)
        sector = 'SpaceBalls{}'.format(count)
        cur.execute('''
            INSERT INTO master_watchlist2 (symbol, name, sector)
            VALUES(%s, %s, %s)''', (entry, name, sector))

        if count % 100 == 0:
            conn.commit()
        if count % 1000 == 0:
            time.sleep(1)
            print('{} entries inserted'.format(count))
    conn.commit()
    cur.close()
    end = time.time()
    print('{} entries created, took {}s'.format(count, end-start))


if __name__ == '__main__':
    main()
