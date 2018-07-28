# ext_sort

External sorting is a class of sorting algorithms that can handle massive amounts of data. External sorting is required when the data being sorted do not fit into the main memory of a computing device (usually RAM) and instead they must reside in the slower external memory, usually a hard disk drive. 

## Usage

```bash
$ ./generate_input --file input.txt --length 32 --lines-number 1024
$ ./sort --input-path input.txt --output-path output.txt --memory 200 --external-storage="$(pwd)" --keep-temp-dir
```

## Testing
```bash
$ python tests.py 
```
