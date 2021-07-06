from mutil.sqlshard import SQLShard


def test_create():
    s = SQLShard()


def test_insert():
    s = SQLShard()
    key = (b"hello", 3, 15)
    give = dict(even="more", fruit="bananas")
    s[key] = give

    assert len(s) == 1
    assert s[key] == give
    assert len(s) == 1

    s.clear()
    assert len(s) == 0
