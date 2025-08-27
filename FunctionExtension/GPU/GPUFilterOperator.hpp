//
// Created by zxk on 8/16/25.
//

#ifndef OLVP_GPUFILTEROPERATOR_HPP
#define OLVP_GPUFILTEROPERATOR_HPP


#include "../Operators/Operator.hpp"

#include "../../Execution/Task/Context/DriverContext.h"
#include "GPUExprFilter.hpp"


class GPUFilterOperator:public Operator
{

    bool finished;

    string name = "GPUFilterOperator";
    string operatorId;


    std::shared_ptr<DataPage> inputPage = NULL;
    std::shared_ptr<DataPage> outPutPage = NULL;



    std::shared_ptr<GPUExprFilter> filter = NULL;
    std::shared_ptr<arrow::Schema> input_schema;

    FilterDescriptor filterDesc;

    GPUFunctions gpuFunctions;
    shared_ptr<DriverContext> driverContext;

    int count = 0;
public:


    GPUFilterOperator(string operatorId,shared_ptr<DriverContext> driverContext,FilterDescriptor filterDesc):Operator("GPUFilterOperator") {


        this->operatorId = operatorId;
        this->filterDesc = filterDesc;
        this->finished = false;

        this->driverContext = driverContext;

        std::vector<std::shared_ptr<arrow::Field>> arrowInputFields;

        for(auto field : this->filterDesc.getInputFields())
        {
            arrowInputFields.push_back(arrow::field(field.getFieldName(),Typer::getType(field.getFieldType())));
        }
        this->input_schema = arrow::schema(arrowInputFields);


        this->filter = std::make_shared<GPUExprFilter>(this->input_schema,this->filterDesc.getFilterExpr());


    }

    void addInput(std::shared_ptr<DataPage> input) override {
        if(input != NULL) {
            this->inputPage = input;
            this->count++;
        }
        if(input->isEndPage())
        {
            spdlog::debug("FilterOperator process "+ to_string(count) + " pages.");
        }

    }


    void process()
    {
        if(this->filter != NULL )
        {
            if(!this->inputPage->isEmptyPage()) {
                this->outPutPage = this->filter->evaluate(this->inputPage);
                gpuFunctions.freeGPUPage(this->inputPage->getExtensionPage());
            }
            else
                this->outPutPage = NULL;
        }
    }


    std::shared_ptr<DataPage> getOutput() override {


        if(this->inputPage == NULL)
            return NULL;

        if(this->inputPage->isEndPage()) {
            spdlog::debug("FilterOperator process "+ to_string(this->count) +" pages");
            this->finished = true;
        }


        if(this->finished)
        {
            this->outPutPage = this->inputPage;
        }
        else
        {
            process();
        }

        this->inputPage = NULL;
        if(this->outPutPage == NULL)
            return NULL;
        if(this->outPutPage->getElementsCount() == 0 )
            return NULL;

        else
            return this->outPutPage;

    }


    bool needsInput() override {
        if(this->inputPage == NULL)
            return true;
        else
            return false;
    }


    bool isFinished()
    {
        return this->finished;
    }

    int isExtension() override
    {
        return true;
    }
    shared_ptr<DataPage> downloadToCPU(shared_ptr<DataPage> page) override
    {
        if(page->isEndPage())
            return page;
        auto table = gpuFunctions.getCPUPageFromGPU(page->getExtensionPage(),input_schema);
        return make_shared<DataPage>(table->CombineChunksToBatch().ValueOrDie());
    }
    shared_ptr<DataPage> uploadToExtension(shared_ptr<DataPage> page) override
    {
        spdlog::info("filter uploadToExtension:"+ to_string(page->getElementsCount()));
        if(page->isEndPage())
            return page;
        return make_shared<DataPage>(gpuFunctions.pushCPUPageToGPU(page->get()),page->get()->schema(),page->getElementsCount(),DataPage::GPU);
    }

    string getOperatorId() override
    {
        return this->operatorId;
    }

};






#endif //OLVP_GPUFILTEROPERATOR_HPP
