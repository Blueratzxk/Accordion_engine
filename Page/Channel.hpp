//
// Created by zxk on 5/20/23.
//

#ifndef OLVP_CHANNEL_HPP
#define OLVP_CHANNEL_HPP

#include "arrow/array.h"

class Channel
{

    std::vector<std::shared_ptr<arrow::Array>> chunks;
    std::shared_ptr<arrow::ChunkedArray> chunkedArray = NULL;

public:
    Channel(){

    }

    void addChunk(std::shared_ptr<arrow::Array> chunk)
    {
        this->chunks.push_back(chunk);
    }

    std::vector<std::shared_ptr<arrow::Array>> getChunks()
    {
        return this->chunks;
    }

    std::shared_ptr<arrow::ChunkedArray> toChunkedArray()
    {
        if(this->chunkedArray != NULL)
            return this->chunkedArray;

        std::shared_ptr<arrow::ChunkedArray> chunked_array = std::make_shared<arrow::ChunkedArray>(this->chunks);
        this->chunkedArray = chunked_array;
        return this->chunkedArray;
    }

};


#endif //OLVP_CHANNEL_HPP
