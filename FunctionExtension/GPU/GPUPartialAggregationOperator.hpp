//
// Created by zxk on 8/19/25.
//

#ifndef OLVP_GPUPARTIALAGGREGATIONOPERATOR_HPP
#define OLVP_GPUPARTIALAGGREGATIONOPERATOR_HPP

#include "../Operators/Operator.hpp"
#include "../Descriptor/AggregationDescriptor.hpp"
#include "../../Execution/Task/Context/DriverContext.h"
#include "GPUFunctions.h"

class GPUPartialAggregationOperator:public Operator {

    bool finished;
    string operatorId;
    string name = "GPUPartialAggregationOperator";


    std::shared_ptr<DataPage> inputPage = NULL;
    std::shared_ptr<DataPage> outPutPage = NULL;


    std::shared_ptr<arrow::Schema> input_schema;

    std::shared_ptr<arrow::Schema> output_schema;

    AggregationDesc desc;


    vector<int> group_columns;
    vector<int> agg_columns;
    vector<string> agg_types;
    bool parametersOk = false;


    GPUFunctions gpuFunctions;

    shared_ptr<arrow::Table> aggResult = NULL;



    bool sendEndPage = false;

    int count = 0;

    shared_ptr<DriverContext> driverContext;
public:


    GPUPartialAggregationOperator(string operatorId,shared_ptr<DriverContext> driverContext,AggregationDesc desc):Operator("GPUPartialAggregationOperator"){

        this->operatorId = operatorId;
        this->desc = desc;
        this->finished = false;
        this->driverContext = driverContext;

        vector<AggregateDesc> aggregateDesc = this->desc.getAggregates();

    }


    void prepareAggregationParameters(std::shared_ptr<DataPage> input)
    {
        auto schema_input = input->get()->schema();

        vector<string> groupKeys = this->desc.getGroupByKeys();
        auto aggKeys = this->desc.getAggregates();
        for(auto key : groupKeys) {
            group_columns.push_back(schema_input->GetFieldIndex(key));
        }

        for(auto key : aggKeys) {
            if(key.getInputKey()!="")
                agg_columns.push_back(schema_input->GetFieldIndex(key.getInputKey()));
            else
                agg_columns.push_back(0);
            agg_types.push_back(key.getFunctionName());
        }
        parametersOk = true;
    }


    void addInput(std::shared_ptr<DataPage> input) override {
        if (input != NULL) {
            this->inputPage = input;
            count++;

            if(!parametersOk)
                prepareAggregationParameters(input);

            if (this->inputPage->isEndPage()) {

                this->sendEndPage = true;
            }

        }

    }

    shared_ptr<arrow::DataType> transCudfTypeToArrowType(string cudf)
    {
        if(cudf == "cudf::string_view")
            return arrow::utf8();
        else if(cudf == "string")
            return arrow::utf8();
        else if(cudf == "int32_t")
            return arrow::int32();
        else if(cudf == "int64_t")
            return arrow::int64();
        else if(cudf == "double")
            return arrow::float64();
        else if(cudf == "cudf::timestamp_D")
            return arrow::date32();
        else
            spdlog::critical("GPUPartialAggregation: unsupported cudf type to arrow "+cudf);

        return NULL;
    }
    void makeOutputSchema(vector<string> outputTypes)
    {
        vector<shared_ptr<arrow::Field>> fields;

        for(int i = 0 ; i < group_columns.size() ; i++)
        {
            auto field = make_shared<arrow::Field>(this->desc.getGroupByKeys()[i], transCudfTypeToArrowType(outputTypes[i]));
            fields.push_back(field);
        }
        int startIndex = group_columns.size();
        for(int i = 0 ; i < agg_columns.size() ; i++)
        {
            auto outputName = this->desc.getAggregates()[i].getOutputName();
            auto outputType = transCudfTypeToArrowType(outputTypes[startIndex+i]);
            auto field = make_shared<arrow::Field>(outputName,outputType);
            fields.push_back(field);
        }
        shared_ptr<arrow::Schema> outputschema = arrow::schema(fields);

        this->output_schema = outputschema;
    }

    void process() {

        vector<string> outputTypes;
        int elementCount = 0;
        auto re = gpuFunctions.cudfAggregation(this->inputPage->getExtensionPage(),this->group_columns,this->agg_columns,this->agg_types,outputTypes,elementCount);

        makeOutputSchema(outputTypes);

        shared_ptr<DataPage> outpage = make_shared<DataPage>(re,output_schema,elementCount,DataPage::GPU);
        this->outPutPage = outpage;
        gpuFunctions.freeGPUPage(this->inputPage->getExtensionPage());
        this->inputPage = NULL;
    }



    std::shared_ptr<DataPage> getOutput() override {

        if (this->sendEndPage) {

            this->finished = true;
            spdlog::debug("PartialAgg process " + to_string(this->count) + " pages.");
            return DataPage::getEndPage();
        }


        if (this->inputPage == NULL)
            return NULL;


        if (!this->inputPage->isEndPage()) {
            process();

        }


        return this->outPutPage;

    }


    bool needsInput() override {
        if (this->inputPage == NULL)
            return true;
        else
            return false;
    }


    bool isFinished() override {
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
        auto table = gpuFunctions.getCPUPageFromGPU(page->getExtensionPage(),this->output_schema);
        return make_shared<DataPage>(table->CombineChunksToBatch().ValueOrDie());
    }
    shared_ptr<DataPage> uploadToExtension(shared_ptr<DataPage> page) override
    {
        spdlog::info("pagg uploadToExtension:"+ to_string(page->getElementsCount()));
        if(page->isEndPage())
            return page;
        return make_shared<DataPage>(gpuFunctions.pushCPUPageToGPU(page->get()),page->get()->schema(),page->getElementsCount(),DataPage::GPU);
    }


    string getOperatorId() override
    {
        return this->operatorId;
    }

};






#endif //OLVP_GPUPARTIALAGGREGATIONOPERATOR_HPP
