#include <bits/stdc++.h>

#include "ExternalSort.h"

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

    auto externlSort = ExternalSort (input, output, memAvail);

    externlSort.Sort ();

    return EXIT_SUCCESS;
}