from random import choices
from string import ascii_lowercase as letters


def generate_random_input(lines_num, length_limit, file_path='input.txt'):
    chars, weights = list(letters) + [''], [1] * len(letters) + [len(letters)]

    def babble(): return ''.join(choices(chars, k=length_limit, weights=weights))
    with open(file_path, 'w') as f:
        f.writelines('%s\n' % babble() for _ in range(lines_num))
