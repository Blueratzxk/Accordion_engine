//
// Created by zxk on 3/5/25.
//

#ifndef OLVP_OPERATOREXTENSION_H
#define OLVP_OPERATOREXTENSION_H


#include <string>
#include <set>
#include <memory>
#include <vector>

#include "GPU/Config/GPUExecutionConfig.hpp"

class Logical_GPUHashJoinOperator;
class Logical_LookupJoinOperator;
class Logical_GPUFilterOperator;
class Logical_FilterOperator;
class Logical_GPUProjectOperator;
class Logical_ProjectOperator;
class Logical_GPUPartialAggregationOperator;
class Logical_PartialAggregationOperator;
class LogicalOperator;



using namespace std;
class OperatorExtension
{
    set<string> extensionSupport = {"Logical_LookupJoinOperator"};
    GPUExecutionConfig gpuExecutionConfig;
public:

    OperatorExtension()
    {

    }

    shared_ptr<Logical_GPUHashJoinOperator> extendHashJoin(shared_ptr<Logical_LookupJoinOperator> lookupJoinOp);

   shared_ptr<Logical_GPUFilterOperator> extendFilter(shared_ptr<Logical_FilterOperator> filterOp);

    shared_ptr<Logical_GPUProjectOperator> extendProject(shared_ptr<Logical_ProjectOperator> proOp);

    shared_ptr<Logical_GPUPartialAggregationOperator> extendPartialAggregation(shared_ptr<Logical_PartialAggregationOperator> parOp);


    vector<std::shared_ptr<LogicalOperator>> extendPipelineTemplate(vector<std::shared_ptr<LogicalOperator>> pipelineTemplate);



};

#endif //OLVP_OPERATOREXTENSION_H
