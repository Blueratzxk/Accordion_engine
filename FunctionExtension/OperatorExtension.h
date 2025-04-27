//
// Created by zxk on 3/5/25.
//

#ifndef OLVP_OPERATOREXTENSION_H
#define OLVP_OPERATOREXTENSION_H



#include "../Operators/LogicalOperators/Logical_LookupJoinOperator.hpp"
#include "../Operators/LogicalOperators/Logical_HashBuilderOperator.hpp"
#include "GPU/Logical_GPUHashJoinOperator.hpp"
class OperatorExtension
{
    set<string> extensionSupport = {"Logical_LookupJoinOperator"};
public:

    OperatorExtension()
    {

    }

    shared_ptr<Logical_GPUHashJoinOperator> extendHashJoin(shared_ptr<Logical_LookupJoinOperator> lookupJoinOp)
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




    vector<std::shared_ptr<LogicalOperator>> extendPipelineTemplate( vector<std::shared_ptr<LogicalOperator>> pipelineTemplate)
    {
        vector<std::shared_ptr<LogicalOperator>> extendedTemplate;
        for(auto logicalOp : pipelineTemplate)
        {
            if(logicalOp->getTypeId() == "Logical_LookupJoinOperator"){

                shared_ptr<Logical_LookupJoinOperator> join = static_pointer_cast<Logical_LookupJoinOperator>(logicalOp);
                extendedTemplate.push_back(extendHashJoin(join));
            }
            else
                extendedTemplate.push_back(logicalOp);
        }
        return extendedTemplate;
    }




};

#endif //OLVP_OPERATOREXTENSION_H
