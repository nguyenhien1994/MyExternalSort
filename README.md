### Simple External Sort
The simple external sort program
The idea is simple:
- Split the file into small chunks, sort them and write the disk
- Merge the sorted files together using MinHeap

#### Build and test:
##### Build:
- Create `build` directory
- Navigate to `build` and call `cmake .. && make`

##### Generate random text file: `./bin/generate_text_file <filename> <size>`
##### Run the test: `./bin/external_sort <input file> <output file> <limit memory>`


### Performance vs Linux sort
- My ex-sort:
```
$ time ./bin/external_sort input1G.txt output.txt 104857600
Sorting with:
	Thread(s): 6
	Limit files: 1024

real	0m44.421s
user	1m36.815s
sys	0m5.286s
```

- Linux sort:
```
$ time sort input1G.txt -S 104857600 -o output2.txt

real	1m52.348s
user	4m48.628s
sys	0m13.810s
```
