//
// Created by zxk on 5/16/23.
//

#ifndef OLVP_DATAPAGE_HPP
#define OLVP_DATAPAGE_HPP

#include "../common.h"
#include "arrow/record_batch.h"
#include "../Utils/ArrowArrayBuilder.hpp"

using namespace std;

class DataPage
{
public:
    enum ExtensionToken{CPU,GPU};
private:
    ExtensionToken loc = CPU;
    std::shared_ptr<arrow::RecordBatch> page = NULL;
    void *extensionPage = NULL;
    int64_t elementsCount = -1;
public:
    DataPage(){}

    DataPage(int64_t elementsCount){
        this->page = NULL;
        this->elementsCount = elementsCount;
    }

    ExtensionToken getExtensionToken()
    {
        return this->loc;
    }

    bool isExtension()
    {
        return getExtensionToken() != CPU;
    }

    DataPage(std::shared_ptr<arrow::RecordBatch> dataPage)
    {
            this->page = dataPage;
            this->elementsCount = page->num_rows();
    }

    DataPage(void *dataPage,shared_ptr<arrow::Schema> schema, int rows,ExtensionToken token)
    {
        vector<shared_ptr<arrow::Array>> arrays;
        this->page = arrow::RecordBatch::Make(schema,0,arrays);
        this->extensionPage = dataPage;
        this->elementsCount = rows;
        this->loc = token;
    }
    void *getExtensionPage()
    {
        return this->extensionPage;
    }

    std::shared_ptr<arrow::RecordBatch> get()
    {
        return this->page;
    }

    bool isEndPage()
    {
        if(this->page == NULL && this->elementsCount == -1)
            return true;
        else
            return false;
    }

    bool isAbortPage()
    {
        if(this->page == NULL && this->elementsCount == -3)
            return true;
        else
            return false;
    }

    bool isEmptyPage()
    {
        if(this->page != NULL && this->elementsCount == 0)
            return true;
        else
            return false;
    }

    bool isShuffleExecutorExitPage()
    {
        if(this->page == NULL && this->elementsCount == -2)
            return true;
        else
            return false;
    }
    static std::shared_ptr<DataPage> getEndPage()
    {
        return std::make_shared<DataPage>();
    }
    static std::shared_ptr<DataPage> getShuffleExecutorExitPage()
    {
        return std::make_shared<DataPage>(-2);
    }

    static std::shared_ptr<DataPage> getAbortPage()
    {
        return std::make_shared<DataPage>(-3);
    }
    int64_t getElementsCount()
    {
        return this->elementsCount;
    }

    std::shared_ptr<DataPage> getLoadedPage(vector<int> channels)
    {
        vector<std::shared_ptr<arrow::Field>> fields;
        vector<std::shared_ptr<arrow::Array>> arrays;

        for(int i = 0 ; i < channels.size() ; i++)
        {
            fields.push_back(this->page->schema()->field(channels[i]));
            arrays.push_back(this->page->column(channels[i]));
        }

        auto schema = std::make_shared<arrow::Schema>(fields);
        auto batch = arrow::RecordBatch::Make(schema,this->elementsCount,arrays);
        auto loadedPage = std::make_shared<DataPage>(batch);

        return loadedPage;

    }

    shared_ptr<arrow::Array> getSingleValueArray(int arrayIndex,int position,int count)
    {
        shared_ptr<arrow::Array> array = this->page->column(arrayIndex);

        shared_ptr<arrow::Scalar> scalar = array->GetScalar(position).ValueOrDie();

        shared_ptr<ArrowArrayBuilder> builder = make_shared<ArrowArrayBuilder>(array->type());

        for(int i = 0 ; i < count ; i++)
            builder->addScalar(scalar);

        return builder->buildFinish().ValueOrDie();

    }


    std::shared_ptr<arrow::Array> getSlice(int channelIndex,std::shared_ptr<arrow::Int32Array> indexArray)
    {
        std::shared_ptr<arrow::Array> channel = this->page->column(channelIndex);

        auto builder = std::make_shared<ArrowArrayBuilder>(channel->type());

        for(int i = 0 ; i < indexArray->length() ; i++)
        {
            int index = indexArray->Value(i);

            arrow::Result<shared_ptr<arrow::Scalar>> result = channel->GetScalar(index);
            if(!result.ok())
                spdlog::critical(result.status().ToString());

            builder->addScalar(channel->GetScalar(index).ValueOrDie());
        }


        return builder->buildFinish().ValueOrDie();
    }

    ~DataPage()
    {

    }


};


#endif //OLVP_DATAPAGE_HPP
