//
// Created by zxk on 8/16/25.
//


#include "OperatorExtension.h"

#include "../Operators/LogicalOperators/Logical_LookupJoinOperator.hpp"
#include "../Operators/LogicalOperators/Logical_HashBuilderOperator.hpp"
#include "../Operators/LogicalOperators/Logical_FilterOperator.hpp"
#include "../Operators/LogicalOperators/Logical_ProjectOperator.hpp"
#include "../Operators/LogicalOperators/Logical_PartialAggregationOperator.hpp"
#include "GPU/Logical_GPUHashJoinOperator.hpp"
#include "GPU/Logical_GPUFilterOperator.hpp"
#include "GPU/Logical_GPUProjectOperator.hpp"
#include "GPU/Logical_GPUPartialAggregationOperator.hpp"
#include "GPU/Logical_GPUBatchAssembleOperator.hpp"


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

    shared_ptr<Logical_GPUHashJoinOperator> logicalGpuHashJoinOperator = make_shared<Logical_GPUHashJoinOperator>(lookupJoinOp->getLogicalOperatorId(),
            probeInputSchema,buildInputSchema,buildOutputSchema,probeHashChannels,buildHashChannels,probeOutputChannels,buildOutputChannels,
            lookupJoinOp->getLookupSourceFactory());

    return logicalGpuHashJoinOperator;
}

shared_ptr<Logical_GPUFilterOperator> OperatorExtension::extendFilter(shared_ptr<Logical_FilterOperator> filterOp)
{

    shared_ptr<Logical_GPUFilterOperator> logicalGpuFilterOperator = make_shared<Logical_GPUFilterOperator>(filterOp->getLogicalOperatorId(),filterOp->getFilterDesc());
    return logicalGpuFilterOperator;
}

shared_ptr<Logical_GPUProjectOperator> OperatorExtension::extendProject(shared_ptr<Logical_ProjectOperator> proOp)
{

    shared_ptr<Logical_GPUProjectOperator> logicalGpuProjectOperator = make_shared<Logical_GPUProjectOperator>(proOp->getLogicalOperatorId(),proOp->getAssignments());
    return logicalGpuProjectOperator;
}

shared_ptr<Logical_GPUPartialAggregationOperator> OperatorExtension::extendPartialAggregation(shared_ptr<Logical_PartialAggregationOperator> parOp)
{

    shared_ptr<Logical_GPUPartialAggregationOperator> logicalGPUPartialAggOperator = make_shared<Logical_GPUPartialAggregationOperator>(parOp->getLogicalOperatorId(),parOp->getDesc());
    return logicalGPUPartialAggOperator;
}




vector<std::shared_ptr<LogicalOperator>> OperatorExtension::extendPipelineTemplate( vector<std::shared_ptr<LogicalOperator>> pipelineTemplate)
{
    vector<std::shared_ptr<LogicalOperator>> extendedTemplate;
    for(auto logicalOp : pipelineTemplate)
    {
        if(logicalOp->getLogicalOperatorType() == "Logical_LookupJoinOperator"){

            shared_ptr<Logical_LookupJoinOperator> join = static_pointer_cast<Logical_LookupJoinOperator>(logicalOp);
            extendedTemplate.push_back(extendHashJoin(join));
        }
        else if(logicalOp->getLogicalOperatorType() == "Logical_FilterOperator")
        {
            shared_ptr<Logical_FilterOperator> filter = static_pointer_cast<Logical_FilterOperator>(logicalOp);
            extendedTemplate.push_back(extendFilter(filter));
        }
        else if(logicalOp->getLogicalOperatorType() == "Logical_ProjectOperator")
        {
            shared_ptr<Logical_ProjectOperator> proj = static_pointer_cast<Logical_ProjectOperator>(logicalOp);
            extendedTemplate.push_back(extendProject(proj));
        }
        else if(logicalOp->getLogicalOperatorType() == "Logical_PartialAggregationOperator")
        {
            shared_ptr<Logical_PartialAggregationOperator> paragg = static_pointer_cast<Logical_PartialAggregationOperator>(logicalOp);
            extendedTemplate.push_back(extendPartialAggregation(paragg));
        }
        else
            extendedTemplate.push_back(logicalOp);
    }



    if(gpuExecutionConfig.isUseGPUBatchAssembleOperator()) {
        for (int i = 0; i < extendedTemplate.size(); i++) {
            if (extendedTemplate[i]->getLogicalOperatorType() == "Logical_GPUHashJoinOperator" ||
                extendedTemplate[i]->getLogicalOperatorType() == "Logical_GPUFilterOperator" ||
                extendedTemplate[i]->getLogicalOperatorType() == "Logical_GPUProjectOperator" ||
                extendedTemplate[i]->getLogicalOperatorType() == "Logical_GPUPartialAggregationOperator") {
                        extendedTemplate.insert(extendedTemplate.begin() + i, make_shared<Logical_GPUBatchAssembleOperator>("-1"));
                        break;
            }
        }
    }


    return extendedTemplate;
}
