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
    @patch('ext_sort.TemporaryDirectory', new=MagicMock(spec=TemporaryDirectory))
    @patch('ext_sort.dump_to_file', new=lambda it, *a, **k: '\n'.join(it), spec=ext_sort.dump_to_file)
    @patch('ext_sort.input_stream', spec=ext_sort.input_stream)
    def test_ext_sort(self, input_stream_mock):
        result, elements = [], [x * 10 for x in ascii_lowercase]

        input_stream_mock.return_value = StringIO('\n'.join(elements))
        with patch('ext_sort.move', autospec=True) as move_mock:
            move_mock.side_effect = lambda s, _: result.extend(line.strip() for line in StringIO(s))

            ext_sort.ext_sort(mem_size=4)

        # check order
        self.assertEqual(result, list(sorted(elements)))


if __name__ == '__main__':
    unittest.main()
