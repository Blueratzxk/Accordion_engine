//
// Created by zxk on 11/4/24.
//

#ifndef FRONTEND_PLANOPTIMIZER_H
#define FRONTEND_PLANOPTIMIZER_H

#include "../PlanNode/PlanNode.hpp"
#include "../Planner/VariableAllocator.hpp"
#include "../Planner/PlanNodeIdAllocator.hpp"
#include "../PlanNode/PlanNodeAllocator.hpp"
class PlanOptimizer
{
public:
    virtual PlanNode* optimize(PlanNode *plan,shared_ptr<PlanNodeAllocator> planNodeAllocator,
                               shared_ptr<VariableAllocator> variableAllocator, shared_ptr<PlanNodeIdAllocator> idAllocator) = 0;

};

#endif //FRONTEND_PLANOPTIMIZER_H
