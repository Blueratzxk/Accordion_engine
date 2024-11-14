//
// Created by zxk on 5/22/23.
//

#ifndef OLVP_DATAPAGEBUILDER_HPP
#define OLVP_DATAPAGEBUILDER_HPP

#include "../common.h"
#include "../Utils/ArrowArrayBuilder.hpp"
#include "DataPage.hpp"
using namespace std;
class DataPageBuilder
{
   std::shared_ptr<arrow::Schema> schema;

   vector<std::shared_ptr<ArrowArrayBuilder>> builders;

   int declaredPositions = 0;

   int maxBytes = 1024*1024;

public:

    DataPageBuilder(std::shared_ptr<arrow::Schema> schema)
    {
        this->schema = schema;


        for(int i = 0 ; i < this->schema->num_fields() ; i++)
        {
            builders.push_back(std::make_shared<ArrowArrayBuilder>(this->schema->field(i)->type()));

        }
    }


    bool is_Full()
    {
        int allBytes = 0;
        for(int i = 0 ; i < this->builders.size() ; i++)
            allBytes += this->builders[i]->getBytesSize();

        return allBytes > maxBytes;
    }

    int getSizeInBytes()
    {
        int allBytes = 0;
        for(int i = 0 ; i < this->builders.size() ; i++)
            allBytes += this->builders[i]->getBytesSize();

        return allBytes;
    }

    std::shared_ptr<ArrowArrayBuilder> getArrayBuilder(int channel)
    {
        return this->builders[channel];
    }

    std::shared_ptr<arrow::DataType> getType(int channel)
    {
        return this->schema->field(channel)->type();
    }

    void appendRow(int position,shared_ptr<DataPage> page)
    {
        for(int i = 0 ; i < this->schema->num_fields() ; i++)
        {
            this->builders[i]->addScalar(page->get()->column(i)->GetScalar(position).ValueOrDie());
        }
        declarePosition();
    }
    void appendRows(vector<int> positions,shared_ptr<DataPage> page)
    {

        for(int i = 0 ; i < this->schema->num_fields() ; i++)
        {
            for(auto p : positions) {
                this->builders[i]->addScalar(page->get()->column(i)->GetScalar(p).ValueOrDie());
            }
        }

        declarePositions(positions.size());
    }

    void declarePosition()
    {
        this->declaredPositions++;
    }
    void declarePositions(int positions)
    {
        this->declaredPositions+=positions;
    }

    void Reset()
    {
        this->declaredPositions = 0;
        for(int i = 0 ; i < this->builders.size() ; i++)
        {
            this->builders[i]->Reset();
        }
    }

    bool isFull()
    {
        return false;
    }

    bool isEmpty()
    {
        return this->declaredPositions == 0;
    }

    std::shared_ptr<DataPage> build()
    {
        vector<std::shared_ptr<arrow::Array>> arrays;

        for(int i = 0 ; i < this->builders.size() ; i++)
        {
            arrow::Result<std::shared_ptr<arrow::Array>> result = this->builders[i]->buildFinish();

            if(!result.ok())
            {
                spdlog::critical("DataPage Builder build page failed!");
                return NULL;
            }
            arrays.push_back(result.ValueOrDie());

        }

        std::shared_ptr<arrow::RecordBatch> batch = arrow::RecordBatch::Make(this->schema,arrays[0]->length(),arrays);

        return std::make_shared<DataPage>(batch);
    }


};

#endif //OLVP_DATAPAGEBUILDER_HPP
