from ska_dbi import Sqsh


def test_read_axafapstat_table():
    s = Sqsh()
    query = "select * from aspect_1 where obsid=5438"
    dat = s.fetchall(query)
    assert len(dat) >= 4
    assert dat["obsid"][0] == 5438
    assert dat[dat["revision"] == 1]['ascdsver'] == '7.6.3'






