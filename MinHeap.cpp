#include <bits/stdc++.h>

#include "MinHeap.h"

static inline void swap(MinHeapNode &a, MinHeapNode &b) {
    a.word.swap(b.word);
    int tmp = a.idx;
    a.idx = b.idx;
    b.idx = tmp;
}

MinHeap::MinHeap(MinHeapNode *heapArray, size_t heapSize) {
    this->heapArray = heapArray;
    this->heapSize = heapSize;
    int i = (heapSize-1) / 2;

    while (i >= 0) {
        Heapify(i--);
    }
}

void MinHeap::Heapify(int idx) {
    int lidx = this->left(idx);
    int ridx = this->right(idx);
    int min = idx;

    /* Find the min word */
    if (lidx < this->heapSize && this->heapArray[lidx].word < this->heapArray[min].word) {
        min = lidx;
    }

    if (ridx < this->heapSize && this->heapArray[ridx].word < this->heapArray[min].word) {
        min = ridx;
    }

    /* Min word is not the root */
    if (min != idx) {
        /* Swap the min word to the root */
        swap(this->heapArray[idx], this->heapArray[min]);

        /* Continue with upper level */
        this->Heapify(min);
    }
}

void MinHeap::replaceMin(MinHeapNode node)
{
    this->heapArray[0] = node;
    this->Heapify(0);
}

bool MinHeap::empty()
{
    return (this->heapSize == 0);
}

void MinHeap::deleteRoot()
{
    if (this->heapSize <= 0)
        return;

    this->heapArray[0] = this->heapArray[this->heapSize-1];
    this->heapSize--;
    this->Heapify(0);
}