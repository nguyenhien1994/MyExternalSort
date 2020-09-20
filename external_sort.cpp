#include <bits/stdc++.h>
#include <sys/resource.h>

#include "MinHeap.h"

#define TMPFILE_PREFIX "/tmp/exsorttmp_"
#define TMPOUTPUT_PREFIX "/tmp/exsorttmpoutput_"

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
    /* Use atomic to prevent the colission */
    std::atomic< long > fileno (0);

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

                /* No more word need to sort */
                if (inFile.eof () && words.size () == 0) {
                    return;
                }
            }

            std::sort (words.begin (), words.end ());

            /* Write buffer to temporary output file */
            auto tmpFile = TMPFILE_PREFIX + std::to_string (fileno++);
            auto outFile = std::ofstream (tmpFile);

            /* Write sorted words into ouput file */
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

    /* Create the threads to split the input and sort to temp files */
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
 * Using the MinHeap with replaceMin() will be a little bit faster than std::priority_queue
 */
void mergeSomeChunks (const std::list< std::string > tmpFiles, const std::string& output)
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
            std::cerr << "Cannot open file: " << file << "error: " << errno << "tmpFiles size: " << tmpFiles.size() << std::endl;
        auto word = std::string ();

        std::getline (tmpFile, heapArray[i].word);
        heapArray[i].idx = i;

        vTmpFiles[i++] = std::move (tmpFile);
    }

    /* Create MinHeap */
    auto heap = MinHeap (heapArray, i);

    while (!heap.empty ()) {

        /* Get the smallest word in thee heap and write to output */
        auto root = heap.getMin ();

        /* It's better to use '\n' to let the OS flush the buffer at the optimum time */
        outFile << root.word << '\n';

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

    /* Free heap memory */
    delete [] heapArray;
}

#if 0
/* The function used to merge the temp files into final output file.
 * This is the multi-threading version, but it's not really efficient.
 * If total temp files is greater than system limit open file,
 * then merge a part of temp file to create a bigger temp file
 * before merging to final output file
 */
void mergeChunks (const std::list< std::string > tmpFiles,
                  const std::string&             output,
                  const size_t                   fileLimit)
{
    if (tmpFiles.size () > fileLimit) {
        auto tmpOutputs = std::list< std::string > ();
        auto lastPos    = tmpFiles.begin ();

        /* Variables use for multi-threading */
        std::mutex            mergeMutex;
        /* Use atomic to prevent the colission */
        std::atomic< long >   tmpOutputIdx (0);
        std::atomic< size_t > remainFiles (tmpFiles.size ());

        /* Thread function to read and sort a chunk */
        auto threadFunc = [&]() {
            while (true) {
                auto const tmpOutputName = TMPOUTPUT_PREFIX + std::to_string (tmpOutputIdx++);
                std::list< std::string > subList;
                {
                    std::lock_guard< std::mutex > lock (mergeMutex);
                    if (remainFiles <= 0) {
                        return;
                    }
                    auto subListSize = (size_t) remainFiles;
                    if (remainFiles > fileLimit) {
                        subListSize = fileLimit;
                    }

                    /* Get the sub list to be merged */
                    auto nextPos = std::next (lastPos, subListSize);
                    subList      = std::list< std::string > (lastPos, nextPos);

                    lastPos = nextPos;

                    tmpOutputs.push_back (tmpOutputName);
                    remainFiles -= subListSize;
                }

                mergeSomeChunks (subList, tmpOutputName);
            }
        };

        std::vector< std::thread > threads (6);
        for (auto& thread : threads) {
            thread = std::thread (threadFunc);
        }

        for (auto& thread : threads) {
            thread.join ();
        }

        mergeSomeChunks (tmpOutputs, output);

    } else {
        mergeSomeChunks (tmpFiles, output);
    }
}
#endif

/* The function used to merge the temp files into final output file
 * If total temp files is greater than system limit open file,
 * then merge a part of temp file to create a bigger temp file
 * before merging to final output file
 */
void mergeChunks (const std::list< std::string > tmpFiles,
                  const std::string&             output,
                  const size_t                   fileLimit)
{
    if (tmpFiles.size () > fileLimit) {
        auto remainFiles  = tmpFiles.size ();
        auto tmpOutputs   = std::list< std::string > ();
        auto tmpOutputIdx = 0;
        auto lastPos      = tmpFiles.begin ();

        while (remainFiles > 0) {
            auto const tmpOutputName = TMPOUTPUT_PREFIX + std::to_string (tmpOutputIdx++);
            auto       subListSize   = remainFiles;
            if (remainFiles > fileLimit) {
                subListSize = fileLimit;
            }

            /* Get the sub list to be merged */
            auto nextPos = std::next (lastPos, subListSize);
            auto subList = std::list< std::string > (lastPos, nextPos);

            mergeSomeChunks (subList, tmpOutputName);

            lastPos = nextPos;

            tmpOutputs.push_back (tmpOutputName);
            remainFiles -= subListSize;
        }

        mergeSomeChunks (tmpOutputs, output);

    } else {
        mergeSomeChunks (tmpFiles, output);
    }
}

/* For sorting data stored on disk */
void externalSort (const std::string& input,
                   const std::string& output,
                   const long         memAvail,
                   const size_t       numThreads)
{
    if (numThreads == 0) {
        std::cerr << "Can't get number of threads\n";
        return;
    }

    /* Get limit of open files */
    struct rlimit lim;
    getrlimit (RLIMIT_NOFILE, &lim);

    std::cout << "Sorting with:\n";
    std::cout << "\tThread(s): " << numThreads << std::endl;
    std::cout << "\tLimit files: " << lim.rlim_cur << std::endl;

    /* Split the input into sorted chunks files */
    auto tmpFiles = splitChunks (input, memAvail, numThreads);

    /* Merge the sorted chunks to output
     * rlim_cur-1, where 4 is reserved for std::in, std::out, std::err, and output file */
    mergeChunks (tmpFiles, output, lim.rlim_cur - 4);
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