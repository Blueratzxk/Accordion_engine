//
// Created by zxk on 3/4/25.
//

#ifndef OLVP_LOGICAL_GPUHASHJOINOPERATOR_HPP
#define OLVP_LOGICAL_GPUHASHJOINOPERATOR_HPP




#include "GPUHashJoinOperator.hpp"
#include "../../Operators/LogicalOperators/LogicalOperator.hpp"
#include "../../Operators/Join/LookupSourceFactory.hpp"

class Logical_GPUHashJoinOperator :public LogicalOperator{




    string name = "Logical_GPUHashJoinOperator";


    shared_ptr<arrow::Schema>probeInputSchema;
    shared_ptr<arrow::Schema>buildInputSchema;
    shared_ptr<arrow::Schema> buildOutputSchema;
    vector<int> probeHashChannels;
    vector<int> buildHashChannels;
    vector<int> probeOutputChannels;
    vector<int> buildOutputChannels;
    shared_ptr<arrow::Table> buildTable;
    std::shared_ptr<LookupSourceFactory> lookupSourceFactory;

    string operatorId;
public:


    Logical_GPUHashJoinOperator(string operatorId,
                                shared_ptr<arrow::Schema>probeInputSchema,shared_ptr<arrow::Schema>buildInputSchema,
                                shared_ptr<arrow::Schema> buildOutputSchema,vector<int> probeHashChannels,vector<int> buildHashChannels,vector<int> probeOutputChannels,vector<int> buildOutputChannels,
                                std::shared_ptr<LookupSourceFactory> lookupSourceFactory)  :LogicalOperator("Logical_GPUHashJoinOperator"){


        this->probeInputSchema = probeInputSchema;
        this->buildInputSchema = buildInputSchema;
        this->buildOutputSchema = buildOutputSchema;
        this->probeHashChannels = probeHashChannels;
        this->buildHashChannels = buildHashChannels;

        this->probeOutputChannels = probeOutputChannels;
        this->buildOutputChannels = buildOutputChannels;
        this->lookupSourceFactory = lookupSourceFactory;

        this->operatorId = operatorId;
    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<GPUHashJoinOperator>(this->operatorId,driverContext,probeInputSchema,buildInputSchema,buildOutputSchema,probeHashChannels,buildHashChannels,probeOutputChannels,buildOutputChannels,lookupSourceFactory);
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<GPUHashJoinOperator>(this->operatorId,driverContext,probeInputSchema,buildInputSchema,buildOutputSchema,probeHashChannels,buildHashChannels,probeOutputChannels,buildOutputChannels,lookupSourceFactory);
    }

    string getLogicalOperatorId() override
    {
        return this->operatorId;
    }


};



#endif //OLVP_LOGICAL_GPUHASHJOINOPERATOR_HPP
