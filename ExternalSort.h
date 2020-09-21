#pragma once

class ExternalSort {
  private:
    /* Input and output filename */
    std::string mInputFilename;
    std::string mOutputFilename;
    /* Maximum memory available for sorting */
    size_t mMemoryAvailble;
    /* List to store the temp filenames */
    std::list< std::string > mTmpFiles;
    /* Number of threads to be used for splitting the input to tempfiles */
    size_t mNumThreads;
    /* Input file size */
    size_t mInputFileSize;

    void splitChunks ();

    void mergeSomeChunks (const std::list< std::string > tmpFiles,
                          const std::string&             outputFilename);

    void mergeChunks (const size_t fileLimit);

  public:
    ExternalSort (const std::string& inputFilename,
                  const std::string& outputFilename,
                  const size_t       memoryAvailable);
    ~ExternalSort ();

    void Sort ();
};
