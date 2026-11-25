import unittest

from materials import list_feeds, read_feed

FIELDS = {
    'sources': ('source', 'horizon_ms', 'protected_types', 'reserve_units'),
    'disk': ('at_ms', 'spool_limit', 'total_free', 'online_floor'),
    'records': ('at_ms', 'source', 'trace', 'part', 'record_id', 'kind', 'size_units'),
    'endpoint': ('at_ms', 'max_accept', 'reply'),
}


class MaterialTest(unittest.TestCase):
    def test_feed_fields(self):
        self.assertEqual(list_feeds(), sorted(FIELDS))
        for name, fields in FIELDS.items():
            with self.subTest(feed=name):
                records = list(read_feed(name))
                self.assertTrue(records)
                self.assertTrue(all(set(row) == set(fields) for row in records))
