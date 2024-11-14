//
// Created by zxk on 11/5/24.
//

#ifndef FRONTEND_ADDTASKOUTPUT_HPP
#define FRONTEND_ADDTASKOUTPUT_HPP



#include "PlanOptimizer.h"
#include "SimplePlanNodeRewriter.hpp"

class AddTaskOutput : public PlanOptimizer
{

public:
    AddTaskOutput()
    {

    }

    PlanNode* optimize(PlanNode *plan,shared_ptr<PlanNodeAllocator> planNodeAllocator,
                       shared_ptr<VariableAllocator> variableAllocator, shared_ptr<PlanNodeIdAllocator> idAllocator)
    {
        TaskOutputNode *taskOutput = planNodeAllocator->new_TaskOutputNode(idAllocator->getNextId());
        taskOutput->addSource(plan);




        return taskOutput;
    }




};





#endif //FRONTEND_ADDTASKOUTPUT_HPP
