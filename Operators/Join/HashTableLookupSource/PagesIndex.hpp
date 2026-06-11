//
// Created by zxk on 5/22/23.
//

#ifndef OLVP_PAGESINDEX_HPP
#define OLVP_PAGESINDEX_HPP

#include "../../../common.h"
#include "arrow/record_batch.h"
#include "arrow/array.h"
#include "arrow/chunked_array.h"
#include "../../../Page/Channel.hpp"
#include "../../../Page/DataPage.hpp"
#include "../LookupSource.hpp"
#include "JoinHashSupplier.hpp"
#include "SimplePagesHashStrategy.hpp"
using namespace std;
class PagesIndex
{
    vector<Channel> channels;
    shared_ptr<arrow::Schema> pageSchema;
    int positionCount;


public:

    PagesIndex(shared_ptr<arrow::Schema> pageSchema){
        this->pageSchema = pageSchema;
        this->positionCount = 0;

        for(int i = 0 ; i < this->pageSchema->num_fields() ; i++)
        {
            this->channels.push_back(Channel());
        }


    }


    void addPage(std::shared_ptr<DataPage> page)
    {
        if(page->getElementsCount() == 0)
            return;

        for(int i = 0 ; i < this->channels.size() ; i++)
        {
            this->channels[i].addChunk(page->get()->column(i));
        }
        this->positionCount += page->getElementsCount();

    }

    void addPage(std::shared_ptr<arrow::Table> table)
    {
        if(table->num_rows() == 0)
            return;

        for(int i = 0 ; i < this->channels.size() ; i++)
        {
            for (auto chunk : table->column(i)->chunks())
                this->channels[i].addChunk(chunk);
        }
        this->positionCount += table->num_rows();

    }

    int getPositionCount()
    {
        return this->positionCount;
    }

    std::shared_ptr<LookupSourceSupplier> createLookupSourceSupplier(vector<int> hashChannels,vector<int> outputChannels,atomic<long> &buildProgress)
    {
        if(this->positionCount == 0)
            return NULL;

        vector<std::shared_ptr<arrow::DataType>> types;
        for(int i = 0 ; i < this->pageSchema->num_fields() ; i++)
        {
            types.push_back(this->pageSchema->field(i)->type());
        }

        std::shared_ptr<PagesHashStrategy> hashStrategy = std::make_shared<SimplePagesHashStrategy>(types,this->channels,hashChannels,outputChannels);

        return std::make_shared<JoinHashSupplier>(hashStrategy,this->positionCount,buildProgress);
    }


    std::shared_ptr<LookupSourceSupplier> createLookupSourceSupplierFromComponents(vector<int> hashChannels,vector<int> outputChannels,atomic<long> &buildProgress,shared_ptr<JoinBuildComponents> joinBuildComponents)
    {
        if(this->positionCount == 0)
            return NULL;

        vector<std::shared_ptr<arrow::DataType>> types;
        for(int i = 0 ; i < this->pageSchema->num_fields() ; i++)
        {
            types.push_back(this->pageSchema->field(i)->type());
        }

        std::shared_ptr<PagesHashStrategy> hashStrategy = std::make_shared<SimplePagesHashStrategy>(types,this->channels,hashChannels,outputChannels);

        return std::make_shared<JoinHashSupplier>(hashStrategy,this->positionCount,buildProgress,joinBuildComponents);
    }




};
#endif //OLVP_PAGESINDEX_HPP
