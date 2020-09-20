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
