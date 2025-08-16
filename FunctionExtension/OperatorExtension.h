//
// Created by zxk on 3/5/25.
//

#ifndef OLVP_OPERATOREXTENSION_H
#define OLVP_OPERATOREXTENSION_H


#include <string>
#include <set>
#include <memory>
#include <vector>

class Logical_GPUHashJoinOperator;
class Logical_LookupJoinOperator;
class Logical_GPUFilterOperator;
class Logical_FilterOperator;
class LogicalOperator;

using namespace std;
class OperatorExtension
{
    set<string> extensionSupport = {"Logical_LookupJoinOperator"};
public:

    OperatorExtension()
    {

    }

    shared_ptr<Logical_GPUHashJoinOperator> extendHashJoin(shared_ptr<Logical_LookupJoinOperator> lookupJoinOp);

   shared_ptr<Logical_GPUFilterOperator> extendFilter(shared_ptr<Logical_FilterOperator> filterOp);

    vector<std::shared_ptr<LogicalOperator>> extendPipelineTemplate(vector<std::shared_ptr<LogicalOperator>> pipelineTemplate);



};

#endif //OLVP_OPERATOREXTENSION_H
