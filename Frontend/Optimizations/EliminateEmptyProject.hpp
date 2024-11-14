//
// Created by zxk on 11/5/24.
//

#ifndef FRONTEND_ELIMINATEEMPTYPROJECT_HPP
#define FRONTEND_ELIMINATEEMPTYPROJECT_HPP

#include "PlanOptimizer.h"
#include "SimplePlanNodeRewriter.hpp"

class EliminateEmptyProject : public PlanOptimizer
{
public:
    EliminateEmptyProject()
    {

    }

    PlanNode* optimize(PlanNode *plan,shared_ptr<PlanNodeAllocator> planNodeAllocator,
                       shared_ptr<VariableAllocator> variableAllocator, shared_ptr<PlanNodeIdAllocator> idAllocator)
    {
        shared_ptr<Visitor> visitor = make_shared<Visitor>(planNodeAllocator);
        return (PlanNode *)visitor->Visit(plan,NULL);
    }


    class Visitor : public SimplePlanNodeRewriter
    {
        shared_ptr<PlanNodeAllocator> planNodeAllocator;


    public:
        Visitor(shared_ptr<PlanNodeAllocator> planNodeAllocator) : SimplePlanNodeRewriter(planNodeAllocator)
        {
            this->planNodeAllocator = planNodeAllocator;
        }


        void * VisitProjectNode(ProjectNode *node, void *context) override
        {

            auto result = (PlanNode*)Visit(node->getSource(),context);



            shared_ptr<Assignments> assignments = node->getAssignments();

            bool isRedundantProject = true;

            for(auto assignment : assignments->getMap())
            {
                string leftName = assignment.first->getName();
                string rightName = assignment.second->getNameOrValue();

                if(leftName != rightName)
                    isRedundantProject = false;
            }

            PlanNode *parent = getParent();
            if(parent->getType() == "TaskOutputNode")
            {
                if(result->getOutputVariables().size() != node->getAssignments()->getOutputs().size())
                    return node->replaceChildren({result});
            }
            if(isRedundantProject) {
                spdlog::info("Find empty project, eliminate it.");
                return result;
            }



            return this->planNodeAllocator->record(node->replaceChildren({result}));
        }


    };


};



#endif //FRONTEND_ELIMINATEEMPTYPROJECT_HPP
