import unittest
from io import StringIO
from string import ascii_lowercase
from tempfile import TemporaryDirectory
from unittest.mock import patch, MagicMock

import ext_sort


class StringIOPipe(ext_sort.Pipe):
    """Use StringIO for testing."""
    @staticmethod
    def stream(string):
        yield from (line.strip() for line in StringIO(string) if line)


class SimpleOrderCheckTestCase(unittest.TestCase):
    @patch('ext_sort.Pipe', new=StringIOPipe)
    @patch('tempfile.TemporaryDirectory', new=MagicMock(spec=TemporaryDirectory))
    @patch('ext_sort.dump_to_file', new=lambda it, *a, **k: ''.join('%s\n' % x for x in it), spec=ext_sort.dump_to_file)
    @patch('ext_sort.input_stream', spec=ext_sort.input_stream)
    @patch('ext_sort.write_output', spec=ext_sort.write_output)
    def test_ext_sort(self, write_output_mock, input_stream_mock):
        result, elements = [], [x * 10 for x in ascii_lowercase]

        input_stream_mock.return_value = StringIO(''.join('%s\n' % x for x in elements))
        write_output_mock.side_effect = lambda _, it: result.extend(x.strip() for x in it if x)

        # perform sort
        ext_sort.ext_sort(mem_size=4)

        # check order
        self.assertEqual(result, list(sorted(elements)))


if __name__ == '__main__':
    unittest.main()
