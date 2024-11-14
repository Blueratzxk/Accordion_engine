//
// Created by zxk on 5/22/23.
//

#ifndef OLVP_SIMPLEPAGESHASHSTRATEGY_HPP
#define OLVP_SIMPLEPAGESHASHSTRATEGY_HPP

#include "../../../common.h"
#include "PagesHashStrategy.hpp"
#include "arrow/api.h"
#include "../../../Page/Channel.hpp"
#include "../../../Page/DataPage.hpp"

#include "arrow/util/hashing.h"
#include "../../../Utils/TypeUtils.hpp"
using namespace std;
class SimplePagesHashStrategy:public PagesHashStrategy {

    vector<shared_ptr<arrow::DataType>> types;
    vector<int> outputChannels;
    vector<int> hashChannels;
    vector<Channel> channels;
    vector<std::shared_ptr<arrow::ChunkedArray>> arrays;


public:
    SimplePagesHashStrategy(vector<shared_ptr<arrow::DataType>>  types,vector<Channel> channels,vector<int> hashChannels,vector<int> outputChannels) {
        this->types = types;
        this->outputChannels = outputChannels;
        this->channels = channels;
        this->hashChannels = hashChannels;

        for(int i = 0 ; i < this->channels.size() ; i++)
        {
            this->arrays.push_back(this->channels[i].toChunkedArray());
        }
    }

    int getChannelCount()
    {
        return this->channels.size();
    }

    int64_t hashPosition(int position)
    {
        int64_t result = 0;
        for (int hashChannel : this->hashChannels) {
            shared_ptr<arrow::DataType> type = types[hashChannel];
            result = result * 31 + TypeUtils::hashPosition(type,this->arrays[hashChannel], position);
        }
        return result;
    }

    int64_t hashRow(int position,shared_ptr<DataPage> page)
    {
        vector<std::shared_ptr<arrow::Array>> cols = page->get()->columns();
        int64_t result = 0;

        for (int i = 0 ; i < this->hashChannels.size() ; i++) {
            shared_ptr<arrow::DataType> type = types[hashChannels[i]];
            result = result * 31 + TypeUtils::hashPosition(type,cols[i], position);
        }
        return result;
    }


    bool positionEqualsPosition(int leftPosition,int rightPosition)
    {
        for (int hashChannel : hashChannels) {
            shared_ptr<arrow::DataType> type = types[hashChannel];

            if (!TypeUtils::positionEqualsPosition(type,this->arrays[hashChannel],leftPosition,rightPosition)) {
                return false;
            }
        }
        return true;
    }

    bool positionEqualsRowIgnoreNulls(int leftPosition, int rightPosition, shared_ptr<DataPage> rightPage)
    {
        for (int i = 0; i < hashChannels.size(); i++) {
            int hashChannel = hashChannels[i];
            shared_ptr<arrow::DataType> type = types[hashChannel];
            std::shared_ptr<arrow::ChunkedArray> leftArray = channels[hashChannel].toChunkedArray();
            std::shared_ptr<arrow::Array> rightArray = rightPage->get()->column(i);

            if (!TypeUtils::equalTo(leftArray, leftPosition, rightArray, rightPosition)) {
                return false;
            }
        }

        return true;
    }


    bool positionEqualsPositionIgnoreNulls(int leftPosition,int rightPosition)
    {
        for (int hashChannel : hashChannels) {
            shared_ptr<arrow::DataType> type = types[hashChannel];

            if(TypeUtils::isPositionNull(type,this->arrays[hashChannel],leftPosition) ||
            TypeUtils::isPositionNull(type,this->arrays[hashChannel],rightPosition))

                return false;
        }
        return true;
    }

    void appendTo(int position, std::shared_ptr<DataPageBuilder> pageBuilder, int outputChannelOffset)
    {
        for (int outputIndex : outputChannels) {

            shared_ptr<arrow::DataType> type = this->types[outputIndex];
            std::shared_ptr<arrow::ChunkedArray> array = this->channels[outputIndex].toChunkedArray();
            TypeUtils::appendTo(type,array, position, pageBuilder->getArrayBuilder(outputChannelOffset));
            outputChannelOffset++;
        }

    }


    bool isPositionNull(int position)
    {
        for (int hashChannel : hashChannels) {

            shared_ptr<arrow::DataType> type = types[hashChannel];
            if(TypeUtils::isPositionNull(type,this->arrays[hashChannel],position))
                return true;
        }
        return false;
    }


};


#endif //OLVP_SIMPLEPAGESHASHSTRATEGY_HPP
