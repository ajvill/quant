from pytest import mark


@mark.quant_db
class QuantDBTests:

    def test_db_connection(self, quant_db):
        conn = quant_db
        if conn is not None:
            assert True
        else:
            assert False
