#include <bits/stdc++.h>

#include "MinHeap.h"

size_t getFileSize (const std::string& filename)
{
    std::ifstream in (filename, std::ifstream::ate);
    return in.tellg ();
}

/* Split the input file into small chunks that fit to Maximum available memory */
std::list< std::string > splitChunks (const std::string& input, const size_t memAvail)
{
    auto tmpFiles  = std::list< std::string > ();
    auto inFile    = std::ifstream (input);
    auto words     = std::vector< std::string > ();
    auto chunkSize = 0L;
    auto word      = std::string ();

    while (std::getline (inFile, word)) {
        if (chunkSize + word.length () > memAvail) {

            std::sort (words.begin (), words.end ());

            /* Write buffer to temporary output file */
            auto tmpFile = std::tmpnam (nullptr); /* TODO: use mktmpfs instead */
            /* TODO: use posix_fallocate */
            auto outFile = std::ofstream (tmpFile);

            /* Write sorted words into ouput file */
            std::copy (words.begin (), words.end (),
                       std::ostream_iterator< std::string > (outFile, "\n"));

            tmpFiles.push_back (std::string (tmpFile));
            chunkSize = 0;
            words.clear ();
        }

        words.push_back (word);
        chunkSize += word.length ();
    }

    /* Last chunks */
    {
        std::sort (words.begin (), words.end ());

        /* Write buffer to temporary output file */
        auto tmpFile = std::tmpnam (nullptr); /* TODO: use mktmpfs instead */
        /* TODO: use posix_fallocate */
        auto outFile = std::ofstream (tmpFile);

        std::copy (words.begin (), words.end (),
                   std::ostream_iterator< std::string > (outFile, "\n"));

        tmpFiles.push_back (std::string (tmpFile));
    }

    return tmpFiles;
}

/* Sorted chunks into the final output
 * Used MinHeap will be a little bit faster than std::priority_queue */
void mergeChunks (const std::list< std::string > tmpFiles, const std::string& output)
{
    auto vTmpFiles = std::vector< std::ifstream > (tmpFiles.size ());
    auto outFile   = std::ofstream (output); /* Output result */

    /* Array of min heap node that will use to init the MinHeap */
    auto heapArray = new MinHeapNode[tmpFiles.size ()];
    auto i         = 0; /* Index of the tmp file stream */

    /* Open all temp files and store the file stream in vTmpFiles */
    for (auto const& file : tmpFiles) {
        auto tmpFile = std::ifstream (file);
        auto word    = std::string ();

        std::getline (tmpFile, heapArray[i].word);
        heapArray[i].idx  = i;

        vTmpFiles[i++] = std::move (tmpFile);
    }

    /* Create Min Heap */
    auto heap = MinHeap (heapArray, i);

    while (!heap.empty()) {

        /* Get the smallest word in thee heap and write to output */
        auto root = heap.getMin ();
        outFile << root.word << std::endl;

        /* Get next word */
        if (!std::getline (vTmpFiles[root.idx], root.word)) {
            /* File merge done */
            heap.deleteRoot ();
        } else {
            /* Replace and heapify the root with new node */
            heap.replaceMin (root);
        }
    }

    /* Remove temp files */
    for (auto const& file : tmpFiles) {
        if (std::remove (file.c_str ()) != 0) {
            std::cout << "Could not remove tmp file " << file << std::endl;
        }
    }
}

// For sorting data stored on disk
void externalSort (const std::string& input, const std::string& output, int memAvail)
{
    /* Split the input into sorted chunks files */
    auto tmpFiles = splitChunks (input, memAvail);

    /* Merge the sorted chunks to output */
    mergeChunks (tmpFiles, output);
}

int main (int argc, char** argv)
{
    if (argc != 4) {
        std::cout << "USAGE: " << argv[0] << " <input file> <output file> "
                  << "<memory limit in bytes>" << std::endl;
        return EXIT_FAILURE;
    }

    auto input    = std::string (argv[1]);
    auto output   = std::string (argv[2]);
    auto memAvail = atol (argv[3]); /* Available memory in byte */

    if (memAvail <= 0) {
        return EXIT_FAILURE;
    }

    if (getFileSize (input) > 0) {
        externalSort (input, output, memAvail);
    }

    return EXIT_SUCCESS;
}