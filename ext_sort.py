import heapq
from itertools import zip_longest
from tempfile import TemporaryDirectory, NamedTemporaryFile


def grouper(it, n):
    """Collect data into fixed-length chunks or blocks."""
    for chunk in zip_longest(fillvalue=None, *[it] * n):
        yield list(filter(None, chunk))


class Nothing:
    """Value used for end of the stream indication."""
    pass


class Pipe:
    """Tip-of-the-pipe abstraction.

    Can pop values from underlying stream.
    Can be compared with other pipe object using last retrieved from stream value.
    """
    def __init__(self, path):
        self.it, self.value = self.stream(path), None
        self.pop()

    def pop(self):
        el, self.value = self.value, next(self.it, Nothing)
        return el

    @property
    def empty(self):
        return self.value == Nothing

    def __lt__(self, other):
        self_, other_ = [(x.empty, x.value if not x.value == Nothing else '') for x in [self, other]]
        return self_ < other_

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


def dump_to_file(it, work_dir, prefix=''):
    """Dump data to temporary file.

    File will not be deleted automatically, do not forget to clean up on your own.
    """
    with NamedTemporaryFile(prefix=prefix, suffix='.txt', dir=work_dir, delete=False, mode='w') as tmp_file:
        tmp_file.writelines('%s\n' % line for line in it)
    return tmp_file.name


def merge_runs(run_paths, memory_limit, work_dir, iter_num=None):
    """Perform k-way merge of sorted files. Value used for k is equals to memory_limit-1."""
    resulting_run_paths, prefix = [], 'merge_{:03}_'.format(iter_num) if iter_num is not None else ''
    for curr_paths_chunk in grouper(iter(run_paths), memory_limit - 1):
        pipes = list(map(lambda p: Pipe(p), curr_paths_chunk))
        path = dump_to_file(merge_pipes(pipes), work_dir, prefix)
        resulting_run_paths.append(path)

    return resulting_run_paths


def ext_sort(input_path='input.txt', output_path='output.txt', mem_size=10, ext_storage_path='.', keep_temp_dir=False):
    """Perform external sorting.

    Read data blocks of fixed size one by one, sort each block in memory and dump sorted block on disk.
    Then perform k-way merge for mem_size-1 blocks, dump result. Repeat merge step if needed.
    """
    if keep_temp_dir:
        TemporaryDirectory._cleanup = lambda cls, name, warn_message: None
        TemporaryDirectory.cleanup = lambda self: None

    with TemporaryDirectory(dir=ext_storage_path) as work_dir:
        # initial small blocks sort step
        run_paths = []
        with open(input_path) as input_stream:
            for chunk in grouper(input_stream, mem_size):
                sorted_run = sorted(el.strip() for el in chunk if el)
                path = dump_to_file(sorted_run, work_dir, prefix='sorted_')
                run_paths.append(path)

        # k-way pipes merge step. repeated before all pipes fits in memory at once
        last_run_cond, iter_num = len(run_paths) < mem_size, 0
        while not last_run_cond:
            run_paths = merge_runs(run_paths, mem_size, work_dir, iter_num)
            iter_num += 1
            last_run_cond = len(run_paths) < mem_size

        # merge pipes last time and write sorted data to output file
        pipes = list(map(lambda p: Pipe(p), run_paths))
        with open(output_path, 'w') as f:
            f.writelines('%s\n' % line for line in merge_pipes(pipes))
