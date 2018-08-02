import heapq
from itertools import chain
from itertools import zip_longest
from shutil import move
from tempfile import TemporaryDirectory, NamedTemporaryFile


def grouper(it, n):
    """Collect data into fixed-length chunks or blocks."""
    yield from (list(filter(None, chunk)) for chunk in zip_longest(fillvalue=None, *[it] * n))


def gauge(it):
    """Yield from iterator with element length."""
    yield from ((len(el), el) for el in it)


class Nothing:
    """Value used as end symbol."""
    pass


class Pipe:
    """Tip-of-the-pipe abstraction for stream from file.

    Can pop values from underlying stream.
    Can be compared with other pipe object using last retrieved from stream value.
    """
    def __init__(self, path):
        self.path, self.it, self.value = path, self.stream(path), None
        self.pop()

    def pop(self):
        el, self.value = self.value, next(self.it, Nothing)
        return el

    @property
    def empty(self):
        return self.value == Nothing

    def __lt__(self, other):
        if self.empty ^ other.empty:
            return not self.empty   # non-Nothing < Nothing
        elif not self.empty:
            return self.value < other.value

        return True     # whatever: Nothing ? Nothing

    @staticmethod
    def stream(path):
        with open(path) as f:
            yield from (line.strip() for line in f if line)


def merge_pipes(pipes):
    """Main k-way merge routine. Build heap and extract values one by one."""
    heapq.heapify(pipes)
    while not pipes[0].empty:
        yield pipes[0].pop()
        heapq._siftup(pipes, 0)


def dump_to_file(it, work_dir):
    """Dump data to temporary file.

    File will not be deleted automatically, do not forget to clean up on your own.
    """
    with NamedTemporaryFile(suffix='.txt', dir=work_dir, delete=False, mode='w') as tmp_file:
        tmp_file.writelines('%s\n' % line for line in it)
        return tmp_file.name


def input_stream(input_path):
    """Iterator over the file lines."""
    with open(input_path) as f:
        yield from f


def yield_sorted_run_paths(input_path, mem_size, work_dir):
    chunks = grouper(input_stream(input_path), mem_size)
    yield from (dump_to_file(sorted(el.strip() for el in chunk if el), work_dir) for chunk in chunks)


def yield_merged_run_paths(run_paths, mem_size, work_dir):
    for length, chunk in gauge(grouper(map(Pipe, run_paths), mem_size - 1)):
        if length == 1:
            # do not perform merge if only one run presents
            yield chunk[0].path
            continue

        yield dump_to_file(merge_pipes(chunk), work_dir)


def ext_sort(input_path='input.txt', output_path='output.txt', mem_size=10, ext_storage_path='.', keep_temp_dir=False):
    """Perform external sorting.

    Read data blocks of fixed size one by one, sort each block in memory and dump sorted block on disk.
    Then perform k-way merge for mem_size-1 blocks, dump result. Repeat merge step if needed.
    """
    if keep_temp_dir:
        TemporaryDirectory._cleanup = lambda cls, name, warn_message: None
        TemporaryDirectory.cleanup = lambda self: None

    with TemporaryDirectory(dir=ext_storage_path) as work_dir:
        # perform in-memory sort for fixed size blocks and dump sorted blocks to disk
        paths = yield_sorted_run_paths(input_path, mem_size, work_dir)
        exhausted, result = False, None

        while not exhausted:
            # iteratively perform k-way merge and dump merge results to disk
            paths = yield_merged_run_paths(paths, mem_size, work_dir)
            first, second, tail = next(paths), next(paths, Nothing), paths
            exhausted, result, paths = second is Nothing, first, chain((first,), (second,), tail)

        else:
            move(result, output_path)
