//
// Created by zxk on 6/4/23.
//

#ifndef OLVP_PAGETRANSFORMER_HPP
#define OLVP_PAGETRANSFORMER_HPP

#include <arrow/api.h>
#include "../../Page/DataPage.hpp"


class DataPageToArrowTable
{
    std::shared_ptr<arrow::Schema> schema;
public:
    DataPageToArrowTable(){


    }

    std::vector<std::shared_ptr<arrow::RecordBatch>> ToBatches(vector<shared_ptr<DataPage>> pages)
    {

        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

        for(int i = 0 ; i < pages.size() ; i++)
        {
            if(pages[i]->isEndPage())
            {
                batches.push_back(createAnEndBatch());
            }
            else {
                std::shared_ptr<arrow::RecordBatch> batch = transformPageToBatch(pages[i]);
                batches.push_back(batch);
            }
        }

        if(pages.size() == 0)
        {
            batches.push_back(createAnEmptyBatch());
        }

        return batches;
    }

    std::shared_ptr<arrow::RecordBatch> createAnEndBatch()
    {
        arrow::NullBuilder nb;
        arrow::Status result = nb.Append(NULL);
        std::shared_ptr<arrow::Array> nullarray = nb.Finish().ValueOrDie();
        std::shared_ptr<arrow::Field> field = std::make_shared<arrow::Field>("EndPage",arrow::null());
        this->schema = arrow::schema({field});
        std::shared_ptr<arrow::RecordBatch> batch = arrow::RecordBatch::Make(schema,nullarray->length(),{nullarray});
        return batch;
    }

    std::shared_ptr<arrow::RecordBatch> createAnEmptyBatch()
    {
        arrow::NullBuilder nb;
        arrow::Status result = nb.Append(NULL);
        std::shared_ptr<arrow::Array> nullarray = nb.Finish().ValueOrDie();
        std::shared_ptr<arrow::Field> field = std::make_shared<arrow::Field>("EmptyPage",arrow::null());
        this->schema = arrow::schema({field});
        std::shared_ptr<arrow::RecordBatch> batch = arrow::RecordBatch::Make(schema,nullarray->length(),{nullarray});
        return batch;
    }



    std::shared_ptr<arrow::RecordBatch> transformPageToBatch(shared_ptr<DataPage> page)
    {
        this->schema = page->get()->schema();
        return page->get();
    }
    std::shared_ptr<arrow::Schema> getSchema()
    {
        return this->schema;
    }
};

class ArrowTableToDataPage
{
public:
    ArrowTableToDataPage(){}


    vector<shared_ptr<DataPage>> ToPages(vector<std::shared_ptr<arrow::RecordBatch>> batches,int *tag)
    {
        vector<shared_ptr<DataPage>> pages;
        for(int i = 0 ; i < batches.size() ; i++)
        {
            if(batches[i]->GetColumnByName("EmptyPage") != NULL && batches[i]->GetColumnByName("EmptyPage")->length() == 1 && batches[i]->num_columns() == 1)
            {

                *tag = 1;
                return pages;
            }

            if(batches[i]->GetColumnByName("EndPage") != NULL && batches[i]->GetColumnByName("EndPage")->length() == 1 && batches[i]->num_columns() == 1)
            {
                *tag = 2;
                pages.push_back(DataPage::getEndPage());
            }
            else {
                pages.push_back(transformBatchToPage(batches[i]));
            }
        }
        return pages;
    }

    shared_ptr<DataPage> transformBatchToPage(std::shared_ptr<arrow::RecordBatch> batch)
    {
        return make_shared<DataPage>(batch);
    }

};
#endif //OLVP_PAGETRANSFORMER_HPP
