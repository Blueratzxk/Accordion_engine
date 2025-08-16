//
// Created by zxk on 8/16/25.
//


#include "OperatorExtension.h"

#include "../Operators/LogicalOperators/Logical_LookupJoinOperator.hpp"
#include "../Operators/LogicalOperators/Logical_HashBuilderOperator.hpp"
#include "../Operators/LogicalOperators/Logical_FilterOperator.hpp"
#include "GPU/Logical_GPUHashJoinOperator.hpp"
#include "GPU/Logical_GPUFilterOperator.hpp"





shared_ptr<Logical_GPUHashJoinOperator> OperatorExtension::extendHashJoin(shared_ptr<Logical_LookupJoinOperator> lookupJoinOp)
{
    shared_ptr<Logical_HashBuilderOperator> hashBuilderOperator = static_pointer_cast<Logical_HashBuilderOperator>(lookupJoinOp->getHashBuilderLogicalOperator());
    shared_ptr<arrow::Schema>probeInputSchema = lookupJoinOp->getProbeSchema();
    shared_ptr<arrow::Schema>buildInputSchema = lookupJoinOp->getBuildInputSchema();
    shared_ptr<arrow::Schema> buildOutputSchema = lookupJoinOp->getBuildOutputSchema();
    vector<int> probeHashChannels = lookupJoinOp->getJoinProbeFactory()->getProbeJoinChannels();
    vector<int> buildHashChannels = hashBuilderOperator->getHashChannels();
    vector<int> probeOutputChannels = lookupJoinOp->getJoinProbeFactory()->getProbeOutputChannels();
    vector<int> buildOutputChannels = hashBuilderOperator->getOutputChannels();
    shared_ptr<arrow::Table> buildTable;

    shared_ptr<Logical_GPUHashJoinOperator> logicalGpuHashJoinOperator = make_shared<Logical_GPUHashJoinOperator>(
            probeInputSchema,buildInputSchema,buildOutputSchema,probeHashChannels,buildHashChannels,probeOutputChannels,buildOutputChannels,
            lookupJoinOp->getLookupSourceFactory());

    return logicalGpuHashJoinOperator;
}

shared_ptr<Logical_GPUFilterOperator> OperatorExtension::extendFilter(shared_ptr<Logical_FilterOperator> filterOp)
{

    shared_ptr<Logical_GPUFilterOperator> logicalGpuFilterOperator = make_shared<Logical_GPUFilterOperator>(filterOp->getFilterDesc());
    return logicalGpuFilterOperator;
}


vector<std::shared_ptr<LogicalOperator>> OperatorExtension::extendPipelineTemplate( vector<std::shared_ptr<LogicalOperator>> pipelineTemplate)
{
    vector<std::shared_ptr<LogicalOperator>> extendedTemplate;
    for(auto logicalOp : pipelineTemplate)
    {
        if(logicalOp->getTypeId() == "Logical_LookupJoinOperator"){

            shared_ptr<Logical_LookupJoinOperator> join = static_pointer_cast<Logical_LookupJoinOperator>(logicalOp);
            extendedTemplate.push_back(extendHashJoin(join));
        }
        else if(logicalOp->getTypeId() == "Logical_FilterOperator")
        {
            shared_ptr<Logical_FilterOperator> filter = static_pointer_cast<Logical_FilterOperator>(logicalOp);
            extendedTemplate.push_back(extendFilter(filter));
        }
        else
            extendedTemplate.push_back(logicalOp);
    }
    return extendedTemplate;
}

