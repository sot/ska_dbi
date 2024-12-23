import pytest
from testr.test_helper import on_head_network

from ska_dbi.common import NoPasswordError
from ska_dbi.sqsh import Sqsh

ON_HEAD_NETWORK = on_head_network()


@pytest.mark.skipif("not ON_HEAD_NETWORK", reason="Test only runs on HEAD network")
def test_fetch_axafapstat_lines():
    s = Sqsh()
    query = "select * from aspect_1 where obsid=5438"
    lines = s.fetch(query)
    assert len(lines) >= 5


@pytest.mark.skipif("not ON_HEAD_NETWORK", reason="Test only runs on HEAD network")
def test_fetchall_axafapstat():
    s = Sqsh()
    query = "select * from aspect_1 where obsid=5438"
    dat = s.fetchall(query)
    assert len(dat) >= 4
    assert dat["obsid"][0] == 5438
    assert dat[dat["revision"] == 1]["ascdsver"] == "7.6.3"


@pytest.mark.skipif("not ON_HEAD_NETWORK", reason="Test only runs on HEAD network")
def test_fetchall_axafocat_empty():
    s = Sqsh(server="sqlsao", user="aca_ops", database="axafocat")
    query = "select * from target where obsid=80000"
    dat = s.fetchall(query)
    assert len(dat) == 0


@pytest.mark.skipif("not ON_HEAD_NETWORK", reason="Test only runs on HEAD network")
def test_fetchone_axafapstat():
    s = Sqsh()
    query = "select * from aspect_1 where obsid=5438 and revision=1"
    dat = s.fetchone(query)
    assert dat["obsid"] == 5438
    assert dat["ascdsver"] == "7.6.3"


@pytest.mark.skipif("not ON_HEAD_NETWORK", reason="Test only runs on HEAD network")
def test_fetchone_axafocat_empty():
    s = Sqsh(server="sqlsao", user="aca_ops", database="axafocat")
    query = "select * from target where obsid=80000"
    dat = s.fetchone(query)
    assert dat is None


@pytest.mark.skipif("not ON_HEAD_NETWORK", reason="Test only runs on HEAD network")
def test_fetchone_axafapstat_context():
    with Sqsh() as s:
        query = "select * from aspect_1 where obsid=5438 and revision=1"
        dat = s.fetchone(query)
        assert dat["obsid"] == 5438
        assert dat["ascdsver"] == "7.6.3"


@pytest.mark.skipif("not ON_HEAD_NETWORK", reason="Test only runs on HEAD network")
def test_no_passwd():
    with pytest.raises(NoPasswordError):
        Sqsh(server="sqlsao", user="aca_nonexistent", database="axafapstat")
