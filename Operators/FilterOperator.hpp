//
// Created by zxk on 5/20/23.
//

#ifndef OLVP_FILTEROPERATOR_HPP
#define OLVP_FILTEROPERATOR_HPP

#include "../Operators/Operator.hpp"

#include "../Descriptor/FilterDescriptor.hpp"

class FilterOperator:public Operator
{

    bool finished;

    string name = "FilterOperator";



    std::shared_ptr<DataPage> inputPage = NULL;
    std::shared_ptr<DataPage> outPutPage = NULL;



    std::shared_ptr<ExprAstFilter> filter = NULL;
    std::shared_ptr<arrow::Schema> input_schema;

    FilterDescriptor filterDesc;


    shared_ptr<DriverContext> driverContext;

    int count = 0;
public:
    string getOperatorId() { return this->name; }

    FilterOperator(shared_ptr<DriverContext> driverContext,FilterDescriptor filterDesc) {


        this->filterDesc = filterDesc;
        this->finished = false;

        this->driverContext = driverContext;

        std::vector<std::shared_ptr<arrow::Field>> arrowInputFields;

        for(auto field : this->filterDesc.getInputFields())
        {
            arrowInputFields.push_back(arrow::field(field.getFieldName(),Typer::getType(field.getFieldType())));
        }
        this->input_schema = arrow::schema(arrowInputFields);


        this->filter = std::make_shared<ExprAstFilter>(this->input_schema,this->filterDesc.getFilterExpr());


        this->filter->Parse();

        arrow::Status status;
        status = this->filter->MakeFilter();
        if(!status.ok())
        {
            spdlog::critical("FilterOperator create filter failed!"+status.ToString());
            this->filter = NULL;
        }

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
            if(!this->inputPage->isEmptyPage())
                this->outPutPage = make_shared<DataPage>(this->filter->DoFilter(this->inputPage->get()));
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


};


#endif //OLVP_FILTEROPERATOR_HPP
