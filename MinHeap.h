
#pragma once

typedef struct MinHeapNode
{
    std::string word; /* The word of the heap */
    int         idx;  /* Index of the word in the heap array */

    /* overloading of assignment operator */
    void operator= (const MinHeapNode& node)
    {
        this->word = node.word;
        this->idx  = node.idx;
    }
} MinHeapNode;

class MinHeap {
  public:
    MinHeap (MinHeapNode* heapArray, size_t heapSize);

    void Heapify (int idx);

    int root (int idx)
    {
        return ((idx - 1) / 2);
    };

    int left (int idx)
    {
        return (2 * idx + 1);
    };

    int right (int idx)
    {
        return (2 * idx + 2);
    };

    MinHeapNode getMin ()
    {
        return heapArray[0];
    }

    bool empty();

    void replaceMin (MinHeapNode node);

    void deleteRoot ();

  private:
    MinHeapNode* heapArray;
    int          heapSize;
};
