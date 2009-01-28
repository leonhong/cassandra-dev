import os, sys, time

from . import client, root, CassandraTester

from thrift.Thrift import TApplicationException
from ttypes import batch_mutation_t, batch_mutation_super_t, superColumn_t, column_t, NotFoundException

_SIMPLE_COLUMNS = [column_t(columnName='c1', value='value1', timestamp=0),
                   column_t(columnName='c2', value='value2', timestamp=0)]
_SUPER_COLUMNS = [superColumn_t(name='sc1', 
                                columns=[column_t(columnName='c4', value='value4', timestamp=0)]),
                  superColumn_t(name='sc2', 
                                columns=[column_t(columnName='c5', value='value5', timestamp=0),
                                         column_t(columnName='c6', value='value6', timestamp=0)])]

def _insert_simple(method=client.insert_blocking):
    method('Table1', 'key1', 'Standard1:c1', 'value1', 0)
    method('Table1', 'key1', 'Standard1:c2', 'value2', 0)

def _insert_batch(method):
    cfmap = {'Standard1': _SIMPLE_COLUMNS,
             'Standard2': _SIMPLE_COLUMNS}
    method(batch_mutation_t(table='Table1', key='key1', cfmap=cfmap))

def _verify_batch():
    _verify_simple()
    assert client.get_slice('Table1', 'key1', 'Standard2', -1, -1) == _SIMPLE_COLUMNS

def _verify_simple():
    assert client.get_column('Table1', 'key1', 'Standard1:c1') == \
        column_t(columnName='c1', value='value1', timestamp=0)
    assert client.get_slice('Table1', 'key1', 'Standard1', -1, -1) == _SIMPLE_COLUMNS

def _insert_super():
    client.insert('Table1', 'key1', 'Super1:sc1:c4', 'value4', 0)
    client.insert('Table1', 'key1', 'Super1:sc2:c5', 'value5', 0)
    client.insert('Table1', 'key1', 'Super1:sc2:c6', 'value6', 0)
    time.sleep(0.1)

def _verify_super():
    assert client.get_column('Table1', 'key1', 'Super1:sc1:c4') == \
        column_t(columnName='c4', value='value4', timestamp=0)
    slice = client.get_slice_super('Table1', 'key1', 'Super1', -1, -1)
    assert slice == _SUPER_COLUMNS, slice

def _expect_missing(fn):
    try:
        r = fn()
    except NotFoundException:
        pass
    else:
        raise Exception('expected missing result; got %s' % r)


class TestMutations(CassandraTester):
    def test_empty_slice(self):
        assert client.get_slice('Mytable', 'key1', 'Standard2', -1, -1) == []

    def test_empty_slice_super(self):
        assert client.get_slice('Mytable', 'key1', 'Super1', -1, -1) == []

    def test_missing_super(self):
        _expect_missing(lambda: client.get_column('Mytable', 'key1', 'Super1:sc1'))

    def test_count(self):
        assert client.get_column_count('Mytable', 'key1', 'Standard2') == 0

    def test_insert(self):
        _insert_simple(client.insert)
        time.sleep(0.1)
        _verify_simple()

    def test_insert_blocking(self):
        _insert_simple(client.insert_blocking)
        _verify_simple()

    def test_super_insert(self):
        _insert_super()
        _verify_super()

    def test_batch_insert(self):
        _insert_batch(client.batch_insert)
        time.sleep(0.1)
        _verify_batch()

    def test_batch_insert_blocking(self):
        _insert_batch(client.batch_insert_blocking)
        _verify_batch()

# does not pass
#     def test_batch_insert_super(self):
#         cfmap = {'Super1': _SUPER_COLUMNS,
#                  'Standard2': _SUPER_COLUMNS}
#         client.batch_insert_superColumn(batch_mutation_t(table='Table1', key='key1', cfmap=cfmap))
#         time.sleep(0.5)
#         _verify_super()
#         assert client.get_slice_super('Table1', 'key1', 'Super2', -1, -1) == _SUPER_COLUMNS

    def test_cf_remove_column(self):
        _insert_simple()
        client.remove('Table1', 'key1', 'Standard1:c1', 1)
        time.sleep(0.1)
        _expect_missing(lambda: client.get_column('Table1', 'key1', 'Standard1:c1'))
        assert client.get_column('Table1', 'key1', 'Standard1:c2') == \
            column_t(columnName='c2', value='value2', timestamp=0)
        assert client.get_slice('Table1', 'key1', 'Standard1', -1, -1) == \
            [column_t(columnName='c2', value='value2', timestamp=0)]

        # New insert, make sure it shows up post-remove:
        client.insert('Table1', 'key1', 'Standard1:c3', 'value3', 0)
        time.sleep(0.1)
        assert client.get_slice('Table1', 'key1', 'Standard1', -1, -1) == \
            [column_t(columnName='c2', value='value2', timestamp=0), 
             column_t(columnName='c3', value='value3', timestamp=0)]

        # Test resurrection.  First, re-insert the value w/ older timestamp, 
        # and make sure it stays removed:
        client.insert('Table1', 'key1', 'Standard1:c1', 'value1', 0)
        time.sleep(0.1)
        assert client.get_slice('Table1', 'key1', 'Standard1', -1, -1) == \
            [column_t(columnName='c2', value='value2', timestamp=0), 
             column_t(columnName='c3', value='value3', timestamp=0)]
        # Next, w/ a newer timestamp; it should come back:
        client.insert('Table1', 'key1', 'Standard1:c1', 'value1', 2)
        time.sleep(0.1)
        assert client.get_slice('Table1', 'key1', 'Standard1', -1, -1) == \
            [column_t(columnName='c1', value='value1', timestamp=2),
             column_t(columnName='c2', value='value2', timestamp=0), 
             column_t(columnName='c3', value='value3', timestamp=0)]


    def test_cf_remove(self):
        _insert_simple()
        _insert_super()

        # Remove the key1:Standard1 cf:
        client.remove('Table1', 'key1', 'Standard1', 3)
        time.sleep(0.1)
        assert client.get_slice('Table1', 'key1', 'Standard1', -1, -1) == []
        _verify_super()

        # Test resurrection.  First, re-insert a value w/ older timestamp, 
        # and make sure it stays removed:
        client.insert('Table1', 'key1', 'Standard1:c1', 'value1', 0)
        time.sleep(0.1)
        assert client.get_slice('Table1', 'key1', 'Standard1', -1, -1) == []
        # Next, w/ a newer timestamp; it should come back:
        client.insert('Table1', 'key1', 'Standard1:c1', 'value1', 4)
        # time.sleep(0.1)
        assert client.get_slice('Table1', 'key1', 'Standard1', -1, -1) == \
            [column_t(columnName='c1', value='value1', timestamp=4)]


    def test_super_cf_remove_column(self):
        _insert_simple()
        _insert_super()

        # Make sure remove clears out what it's supposed to, and _only_ that:
        client.remove('Table1', 'key1', 'Super1:sc2:c5', 5)
        time.sleep(0.1)
        _expect_missing(lambda: client.get_column('Table1', 'key1', 'Super1:sc2:c5'))
        assert client.get_slice_super('Table1', 'key1', 'Super1', -1, -1) == \
            [superColumn_t(name='sc1', 
                           columns=[column_t(columnName='c4', value='value4', timestamp=0)]),
             superColumn_t(name='sc2', 
                           columns=[column_t(columnName='c6', value='value6', timestamp=0)])]
        _verify_simple()

        # New insert, make sure it shows up post-remove:
        client.insert('Table1', 'key1', 'Super1:sc2:c7', 'value7', 0)
        time.sleep(0.1)
        scs = [superColumn_t(name='sc1', 
                             columns=[column_t(columnName='c4', value='value4', timestamp=0)]),
               superColumn_t(name='sc2', 
                             columns=[column_t(columnName='c6', value='value6', timestamp=0),
                                      column_t(columnName='c7', value='value7', timestamp=0)])]

        assert client.get_slice_super('Table1', 'key1', 'Super1', -1, -1) == scs

        # Test resurrection.  First, re-insert the value w/ older timestamp, 
        # and make sure it stays removed:
        client.insert('Table1', 'key1', 'Super1:sc2:c5', 'value5', 0)
        time.sleep(0.1)
        actual = client.get_slice_super('Table1', 'key1', 'Super1', -1, -1)
        assert actual == scs, actual

        # Next, w/ a newer timestamp; it should come back
        client.insert('Table1', 'key1', 'Super1:sc2:c5', 'value5', 6)
        time.sleep(0.1)
        actual = client.get_slice_super('Table1', 'key1', 'Super1', -1, -1)
        assert actual == \
            [superColumn_t(name='sc1', 
                           columns=[column_t(columnName='c4', value='value4', timestamp=0)]), 
             superColumn_t(name='sc2', 
                           columns=[column_t(columnName='c5', value='value5', timestamp=6), 
                                    column_t(columnName='c6', value='value6', timestamp=0), 
                                    column_t(columnName='c7', value='value7', timestamp=0)])], actual
    def test_super_cf_remove_supercolumn(self):
        _insert_simple()
        _insert_super()

        # Make sure remove clears out what it's supposed to, and _only_ that:
        client.remove('Table1', 'key1', 'Super1:sc2', 5)
        time.sleep(0.1)
        _expect_missing(lambda: client.get_column('Table1', 'key1', 'Super1:sc2:c5'))
        actual = client.get_slice_super('Table1', 'key1', 'Super1:sc2', -1, -1)
        assert actual == [], actual
        actual = client.get_slice_super('Table1', 'key1', 'Super1', -1, -1)
        assert actual == \
            [superColumn_t(name='sc1', 
                           columns=[column_t(columnName='c4', value='value4', timestamp=0)])], actual
        _verify_simple()

        # New insert, make sure it shows up post-remove:
        client.insert('Table1', 'key1', 'Super1:sc2:c7', 'value7', 0)
        time.sleep(0.1)
        scs = [superColumn_t(name='sc1', 
                             columns=[column_t(columnName='c4', value='value4', timestamp=0)]),
               superColumn_t(name='sc2', 
                             columns=[column_t(columnName='c7', value='value7', timestamp=0)])]

        assert client.get_slice_super('Table1', 'key1', 'Super1', -1, -1) == scs

        # Test resurrection.  First, re-insert the value w/ older timestamp, 
        # and make sure it stays removed:
        client.insert('Table1', 'key1', 'Super1:sc2:c5', 'value5', 0)
        time.sleep(0.1)
        actual = client.get_slice_super('Table1', 'key1', 'Super1', -1, -1)
        assert actual == scs, actual

        # Next, w/ a newer timestamp; it should come back
        client.insert('Table1', 'key1', 'Super1:sc2:c5', 'value5', 6)
        time.sleep(0.1)
        actual = client.get_slice_super('Table1', 'key1', 'Super1', -1, -1)
        assert actual == \
            [superColumn_t(name='sc1', 
                           columns=[column_t(columnName='c4', value='value4', timestamp=0)]),
             superColumn_t(name='sc2', 
                           columns=[column_t(columnName='c5', value='value5', timestamp=6),
                                    column_t(columnName='c7', value='value7', timestamp=0)])], actual
