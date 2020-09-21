#include <bits/stdc++.h>
#include <sys/resource.h>

#include "ExternalSort.h"

#include "MinHeap.h"

#define TMPFILE_PREFIX "/tmp/exsorttmp_"
#define TMPOUTPUT_PREFIX "/tmp/exsorttmpoutput_"

ExternalSort::ExternalSort (const std::string& inputFilename,
                            const std::string& outputFilename,
                            const size_t       memoryAvailable)
    : mInputFilename (inputFilename),
      mOutputFilename (outputFilename),
      mMemoryAvailble (memoryAvailable),
      mNumThreads (std::thread::hardware_concurrency ())
{
}

ExternalSort::~ExternalSort ()
{
}

/* Split the input file into small chunks that fit to Maximum available memory */
void ExternalSort::splitChunks ()
{
    auto       inFile         = std::ifstream (this->mInputFilename);
    auto const threadMemAvail = this->mMemoryAvailble / this->mNumThreads;

    /* Variables use for multi-threading */
    std::mutex inputFileMutex;
    std::mutex tmpFilesMutex;
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
                this->mTmpFiles.push_back (std::string (tmpFile));
            }

            /* Push the word haven't inserted yet */
            if (notInserted) {
                words.push_back (word);
                notInserted = false;
            }
        }
    };

    /* Create the threads to split the input and sort to temp files */
    std::vector< std::thread > threads (this->mNumThreads);
    for (auto& thread : threads) {
        thread = std::thread (threadFunc);
    }

    for (auto& thread : threads) {
        thread.join ();
    }
}

/* Merging some sorted chunks into the output file
 * Using the MinHeap with replaceMin() will be a little bit faster than std::priority_queue
 */
void ExternalSort::mergeSomeChunks (const std::list< std::string > tmpFiles,
                                    const std::string&             outputFilename)
{
    auto vTmpFiles = std::vector< std::ifstream > (tmpFiles.size ());
    auto outFile   = std::ofstream (outputFilename); /* Output result */

    /* Array of min heap node that will use to init the MinHeap */
    auto heapArray = new MinHeapNode[tmpFiles.size ()];
    auto i         = 0; /* Index of the tmp file stream */

    /* Open all temp files and store the file stream in vTmpFiles */
    for (auto const& file : tmpFiles) {
        auto tmpFile = std::ifstream (file);
        if (!tmpFile.is_open ())
            std::cerr << "Cannot open file: " << file << "error: " << errno
                      << "tmpFiles size: " << tmpFiles.size () << std::endl;
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
    delete[] heapArray;
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
void ExternalSort::mergeChunks (const size_t fileLimit)
{
    if (this->mTmpFiles.size () > fileLimit) {
        auto remainFiles  = this->mTmpFiles.size ();
        auto tmpOutputs   = std::list< std::string > ();
        auto tmpOutputIdx = 0;
        auto lastPos      = this->mTmpFiles.begin ();

        while (remainFiles > 0) {
            auto const tmpOutputName = TMPOUTPUT_PREFIX + std::to_string (tmpOutputIdx++);
            auto       subListSize   = remainFiles;
            if (remainFiles > fileLimit) {
                subListSize = fileLimit;
            }

            /* Get the sub list to be merged */
            auto nextPos = std::next (lastPos, subListSize);
            auto subList = std::list< std::string > (lastPos, nextPos);

            this->mergeSomeChunks (subList, tmpOutputName);

            lastPos = nextPos;

            tmpOutputs.push_back (tmpOutputName);
            remainFiles -= subListSize;
        }

        this->mergeSomeChunks (tmpOutputs, mOutputFilename);

    } else {
        this->mergeSomeChunks (this->mTmpFiles, mOutputFilename);
    }
}

static inline size_t getFileSize (const std::string& filename)
{
    std::ifstream in (filename, std::ifstream::ate);
    return in.tellg ();
}

/* For sorting data stored on disk */
void ExternalSort::Sort ()
{
    if (getFileSize (this->mInputFilename) > 0) {
        if (this->mNumThreads == 0) {
            std::cerr << "Can't get number of threads\n";
            return;
        }

        /* Get limit of open files */
        struct rlimit lim;
        getrlimit (RLIMIT_NOFILE, &lim);

        std::cout << "Sorting with:\n";
        std::cout << "\tThread(s): " << this->mNumThreads << std::endl;
        std::cout << "\tLimit files: " << lim.rlim_cur << std::endl;

        /* Split the input into sorted chunks files */
        this->splitChunks ();

        /* Merge the sorted chunks to output
        * rlim_cur-1, where 4 is reserved for std::in, std::out, std::err, and output file */
        this->mergeChunks (lim.rlim_cur - 4);
    }
}
