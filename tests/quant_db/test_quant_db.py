from src.quant_db.quant_db_utils import QuantDB
from pytest import mark
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

        cur.execute(QuantDB().quant_db_stmt_dict['table_index_column_list'])

        found_items = set()
        for row in cur:
            if row[col] in quant_obj:
                found_items.add(row[col])

        return len(found_items)

    def initial_setup(self, conn_fixture, obj_fixture):
        conn = conn_fixture
        obj = obj_fixture
        cur = conn.cursor()

        return conn, obj, cur

    # Pytest methods
    @mark.db_test1
    @mark.db_test_test1
    @mark.quant_db_basic
    @mark.quant_db_test_basic
    def test_db_connection(self, quant_db_conn):
        conn = quant_db_conn
        if conn is not None:
            assert True
        else:
            assert False

    @mark.quant_db_basic
    @mark.quant_db_test_basic
    def test_drop_tables(self, quant_db_conn, quant_tables):

        conn, tables, cur = self.initial_setup(quant_db_conn, quant_tables)

        for table in tables:
            QuantDB().drop_table(cur, table)
        conn.commit()

        cur = conn.cursor()
        found_tables = self.get_num_found_objects(cur, tables, 'table')

        if found_tables == 0:
            assert True

    @mark.quant_db_basic
    @mark.quant_db_test_basic
    def test_create_tables(self, quant_db_conn, quant_tables):
        conn, tables, cur = self.initial_setup(quant_db_conn, quant_tables)

        for table in tables:
            QuantDB().create_table(cur, table)
        conn.commit()

        cur = conn.cursor()
        num_of_tables = len(tables)
        found_tables = self.get_num_found_objects(cur, tables, 'table')

        if found_tables == num_of_tables:
            assert True
        else:
            assert False

    @mark.quant_db_basic
    @mark.quant_db_test_basic
    def test_drop_indexes(self, quant_db_conn, quant_indexes):
        conn, indexes, cur = self.initial_setup(quant_db_conn, quant_indexes)

        for index in indexes:
            QuantDB().drop_index(cur, index)
        conn.commit()

        cur = conn.cursor()
        found_indexes = self.get_num_found_objects(cur, indexes, 'index')

        if found_indexes == 0:
            assert True
        else:
            assert False

    @mark.quant_db_basic
    @mark.quant_db_test_basic
    def test_create_indexes(self, quant_db_conn, quant_indexes):
        conn, indexes, cur = self.initial_setup(quant_db_conn, quant_indexes)

        for index in indexes:
            QuantDB().create_index(cur, index)
        conn.commit()

        cur = conn.cursor()
        cur.execute(QuantDB().quant_db_stmt_dict['table_index_column_list'])

        found_indexes = self.get_num_found_objects(cur, indexes, 'index')
        num_of_indexes = len(indexes)

        if found_indexes == num_of_indexes:
            assert True
        else:
            assert False

    @mark.quant_db_basic
    @mark.quant_db_test_basic
    def test_create_function_trigger_set_timestamp(self, quant_db_conn):
        conn, table, cur = self.initial_setup(quant_db_conn, None)

        status = QuantDB().create_function_trigger(cur)
        conn.commit()

        if status is not None:
            assert False

        assert True

    @mark.quant_db_basic
    @mark.quant_db_test_basic
    def test_create_trigger_stored_procedure(self, quant_db_conn, quant_tables):
        conn, tables, cur = self.initial_setup(quant_db_conn, quant_tables)

        for table in tables:
            status = QuantDB().create_trigger_stored_procedure(cur, table)
            if status is not None:
                assert False
        conn.commit()

        assert True

    @mark.quant_db_basic
    @mark.quant_db_test_basic
    def test_insert_watchlist_members_table(self, quant_db_conn, watchlist_members_data):

        """
        if isinstance(watchlist_members_data, dict):
            params = list(watchlist_members_data.values())
        else:
            params = watchlist_members_data
        """
        params = watchlist_members_data
        conn, table, cur = self.initial_setup(quant_db_conn, 'watchlist_members')

        status = QuantDB().insert_table(cur, table, params)
        conn.commit()
        if status is not None:
            assert False

        assert True

    @mark.quant_db_basic
    @mark.quant_db_test_basic
    def test_insert_master_watchlist_table(self, quant_db_conn, master_watchlist_data):
        params = master_watchlist_data
        conn, table, cur = self.initial_setup(quant_db_conn, 'master_watchlist')

        status = QuantDB().insert_table(cur, table, params)

        conn.commit()
        if status is not None:
            assert False

        assert True

    @mark.quant_db_basic
    @mark.quant_db_test_basic
    def test_insert_accounts_table(self, quant_db_conn, accounts_data):
        params = accounts_data
        conn, table, cur = self.initial_setup(quant_db_conn, 'accounts')

        status = QuantDB().insert_table(cur, table, params)

        conn.commit()
        if status is not None:
            assert False

        assert True

