import unittest

from domain.table import Column


class TestColumn(unittest.TestCase):
    def test_platform_options(self):
        # Both the current and the old (e.g. REL1_43) option key are supported
        for key in ('PlatformOptions', 'CustomSchemaOptions'):
            expiry = Column.newFromAbstractSchema(
                {'type': 'mwtimestamp', 'options': {key: {'allowInfinite': True}}})
            self.assertEqual((expiry.type_, expiry.size_), ('varbinary', 14))
            enum = Column.newFromAbstractSchema(
                {'type': 'mwenum', 'options': {key: {'enum_values': ['a', 'b']}}})
            self.assertEqual((enum.type_, enum.size_), ('enum', {'a', 'b'}))

    def test_timestamp(self):
        ts = Column.newFromAbstractSchema({'type': 'mwtimestamp', 'options': {}})
        self.assertEqual((ts.type_, ts.size_), ('binary', 14))


if __name__ == '__main__':
    unittest.main()
