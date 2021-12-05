from db_work.quant_db.quant_db_utils import create_index, create_table, drop_index, drop_table,\
    create_watchlist_table, insert_table, quant_db_stmt_dict
from pytest import mark
import time
import logging

logger = logging.getLogger()


@mark.quant_db
class QuantDBTests:

    # Helper methods
    def get_num_found_objects(self, cur, quant_obj, db_type='table'):

        if db_type in 'table':
            col = 0
        else:
            col = 1

        cur.execute(quant_db_stmt_dict['table_index_column_list'])

        found_items = set()
        for row in cur:
            if row[col] in quant_obj:
                found_items.add(row[col])

        return len(found_items)

    # pytest methods
    @mark.quant_db_basic
    def test_db_connection(self, quant_db_conn):
        conn = quant_db_conn
        if conn is not None:
            assert True
        else:
            assert False

    @mark.quant_db_basic
    def test_drop_tables(self, quant_db_conn, quant_tables):
        conn = quant_db_conn
        tables = quant_tables
        cur = conn.cursor()

        for table in tables:
            drop_table(cur, table)
        conn.commit()

        cur = conn.cursor()
        found_tables = self.get_num_found_objects(cur, tables, 'table')

        if found_tables == 0:
            assert True

    @mark.quant_db_basic
    def test_create_tables(self, quant_db_conn, quant_tables):
        conn = quant_db_conn
        tables = quant_tables
        cur = conn.cursor()

        for table in tables:
            create_table(cur, table)
        conn.commit()

        cur = conn.cursor()
        num_of_tables = len(tables)
        found_tables = self.get_num_found_objects(cur, tables, 'table')

        if found_tables == num_of_tables:
            assert True
        else:
            assert False

    @mark.quant_db_basic
    def test_drop_indexes(self, quant_db_conn, quant_indexes):
        conn = quant_db_conn
        indexes = quant_indexes
        cur = conn.cursor()

        for index in indexes:
            drop_index(cur, index)
        conn.commit()

        cur = conn.cursor()
        found_indexes = self.get_num_found_objects(cur, indexes, 'index')

        if found_indexes == 0:
            assert True
        else:
            assert False

    @mark.quant_db_basic
    def test_create_indexes(self, quant_db_conn, quant_indexes):
        conn = quant_db_conn
        indexes = quant_indexes
        cur = conn.cursor()

        for index in indexes:
            create_index(cur, index)
        conn.commit()

        cur = conn.cursor()
        cur.execute(quant_db_stmt_dict['table_index_column_list'])

        found_indexes = self.get_num_found_objects(cur, indexes, 'index')
        num_of_indexes = len(indexes)

        if found_indexes == num_of_indexes:
            assert True
        else:
            assert False

    @mark.quant_db_basic
    def test_insert_watchlist_members_table(self, quant_db_conn, watchlist_members_data):
        params = watchlist_members_data

        conn = quant_db_conn
        table = 'watchlist_members'
        cur = conn.cursor()

        status = insert_table(cur, table, params)
        conn.commit()
        if status is not None:
            assert False

        assert True

    @mark.db_test1
    @mark.quant_db_basic
    def test_create_watchlist_table(self, quant_db_conn):
        conn = quant_db_conn
        table = 'watchlist_members'
        cur = conn.cursor()

        sql = quant_db_stmt_dict[table]['select']
        cur.execute(sql)

        wl_list = []
        for table in cur:
            col = list(table)
            data = {'id': col[0], 'name': col[1]}
            wl_list.append(data)

        assert True
        #status = create_watchlist_table(cur, '0positions')
        #conn.commit()

