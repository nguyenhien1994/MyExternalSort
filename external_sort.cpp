#include <bits/stdc++.h>

#include "MinHeap.h"

#define TMPFILE_PREFIX "/tmp/exsorttmp_"

static inline size_t getFileSize (const std::string& filename)
{
    std::ifstream in (filename, std::ifstream::ate);
    return in.tellg ();
}

/* Split the input file into small chunks that fit to Maximum available memory */
std::list< std::string >
splitChunks (const std::string& input, const size_t memAvail, const size_t numThreads)
{
    auto       tmpFiles       = std::list< std::string > ();
    auto       inFile         = std::ifstream (input);
    auto const threadMemAvail = memAvail / numThreads;

    /* Variables use for multi-threading */
    std::mutex          inputFileMutex;
    std::mutex          tmpFilesMutex;
    std::atomic< long > fileno (0); /* Use atomic to prevent the colission */

    /* Thread function to read and sort a chunk */
    auto threadFunc = [&]() {
        auto words       = std::vector< std::string > ();
        auto chunkSize   = 0L;
        auto word        = std::string ();
        auto notInserted = false;

        while (true) {
            {
                std::lock_guard< std::mutex > lock (inputFileMutex);
                while (std::getline (inFile, word)) {
                    if (chunkSize + word.length () > threadMemAvail) {
                        notInserted = true;
                        break;
                    }
                    words.push_back (word);
                    chunkSize += word.length ();
                }

                if (inFile.eof () && words.size () == 0) {
                    return;
                }
            }

            std::sort (words.begin (), words.end ());

            /* Write buffer to temporary output file */
            auto tmpFile = TMPFILE_PREFIX + std::to_string (fileno++);
            auto outFile = std::ofstream (tmpFile);

            /* Write sorted words into ouput file,
             * it's much faster than write line by line */
            std::copy (words.begin (), words.end (),
                       std::ostream_iterator< std::string > (outFile, "\n"));

            chunkSize = 0;
            words.clear (); /* Clear the word list */

            /* Push the temp file to tmpFiles list */
            {
                std::lock_guard< std::mutex > lock (tmpFilesMutex);
                tmpFiles.push_back (std::string (tmpFile));
            }

            /* Push the word haven't inserted yet */
            if (notInserted) {
                words.push_back (word);
                notInserted = false;
            }
        }
    };

    std::vector< std::thread > threads (numThreads);
    for (auto& thread : threads) {
        thread = std::thread (threadFunc);
    }

    for (auto& thread : threads) {
        thread.join ();
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
        if (!tmpFile.is_open ())
            std::cerr << "Cannot open file: " << file << "error: " << errno << std::endl;
        auto word = std::string ();

        std::getline (tmpFile, heapArray[i].word);
        heapArray[i].idx = i;

        vTmpFiles[i++] = std::move (tmpFile);
    }

    /* Create Min Heap */
    auto heap = MinHeap (heapArray, i);

    while (!heap.empty ()) {

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
            std::cerr << "Could not remove tmp file " << file << std::endl;
        }
    }
}

// For sorting data stored on disk
void externalSort (const std::string& input,
                   const std::string& output,
                   const long         memAvail,
                   const size_t       numThreads)
{
    if (numThreads == 0) {
        std::cerr << "Can't get number of threads\n";
        return;
    }

    std::cout << "Sorting with " << numThreads << "thread(s)\n";

    /* Split the input into sorted chunks files */
    auto tmpFiles = splitChunks (input, memAvail, numThreads);

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
        externalSort (input, output, memAvail, std::thread::hardware_concurrency ());
    }

    return EXIT_SUCCESS;
}