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
    """Tip-of-the-pipe abstraction for stream from file.

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


def merge_runs(pipes, memory_limit, work_dir, iter_num=None):
    """Perform k-way merge of sorted streams. Value used for k is equals to memory_limit-1."""
    resulting_run_paths, prefix = [], 'merge_{:03}_'.format(iter_num) if iter_num is not None else ''
    for curr_pipes_chunk in grouper(pipes, memory_limit - 1):
        path = dump_to_file(merge_pipes(curr_pipes_chunk), work_dir, prefix)
        resulting_run_paths.append(path)

    return resulting_run_paths


def input_stream(input_path):
    with open(input_path) as f:
        yield from f


def write_output(output_path, it):
    with open(output_path, 'w') as f:
        f.writelines(it)


def ext_sort(input_path='input.txt', output_path='output.txt', mem_size=10, ext_storage_path='.', keep_temp_dir=False):
    """Perform external sorting.

    Read data blocks of fixed size one by one, sort each block in memory and dump sorted block on disk.
    Then perform k-way merge for mem_size-1 blocks, dump result. Repeat merge step if needed.
    """
    if keep_temp_dir:
        TemporaryDirectory._cleanup = lambda cls, name, warn_message: None
        TemporaryDirectory.cleanup = lambda self: None

    with TemporaryDirectory(dir=ext_storage_path) as work_dir:
        # initial small blocks sort
        run_paths = []

        for chunk in grouper(input_stream(input_path), mem_size):
            sorted_run = sorted(el.strip() for el in chunk if el)
            path = dump_to_file(sorted_run, work_dir, prefix='sorted_')
            run_paths.append(path)

        # perform k-way merge of sorted streams. repeated before all pipes fits in memory at once
        # value used for k is equals to mem_size-1
        last_run_cond, iter_num = len(run_paths) < mem_size, 0
        while not last_run_cond:
            pipes = map(Pipe, run_paths)
            run_paths, prefix = [], 'merge_{:03}_'.format(iter_num) if iter_num is not None else ''
            for curr_pipes_chunk in grouper(pipes, mem_size - 1):
                path = dump_to_file(merge_pipes(curr_pipes_chunk), work_dir, prefix)
                run_paths.append(path)

            iter_num += 1
            last_run_cond = len(run_paths) < mem_size

        # merge pipes last time
        pipes = list(map(lambda p: Pipe(p), run_paths))

        # write sorted data to output file
        write_output(output_path, ('%s\n' % line for line in merge_pipes(pipes)))
