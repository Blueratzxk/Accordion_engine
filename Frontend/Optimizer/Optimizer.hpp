//
// Created by zxk on 11/5/24.
//

#ifndef FRONTEND_OPTIMIZER_HPP
#define FRONTEND_OPTIMIZER_HPP

#include "../Optimizations/EliminateEmptyProject.hpp"
#include "../Optimizations/EliminateRedundantProject.hpp"

#include "../Optimizations/AddTaskOutput.hpp"
#include <memory>
#include "../Optimizations/PruneJoinRedundantColumns.hpp"
#include "../Optimizations/AddDistributedAggregation.hpp"
#include "../Optimizations/AddExchange.hpp"

class Optimizer
{
    list<shared_ptr<PlanOptimizer>> optimizations;
    shared_ptr<PlanNodeIdAllocator> idAllocator;
    shared_ptr<PlanNodeAllocator> planNodeAllocator;
    shared_ptr<VariableAllocator> variableAllocator;
    shared_ptr<Analysis> analysis;

public:
    Optimizer(shared_ptr<Analysis> analysis,shared_ptr<PlanNodeIdAllocator> idAllocator,shared_ptr<PlanNodeAllocator> planNodeAllocator)
    {
        this->planNodeAllocator = planNodeAllocator;
        this->variableAllocator = make_shared<VariableAllocator>();
        this->idAllocator = idAllocator;
        this->analysis = analysis;


        optimizations.push_back(make_shared<AddTaskOutput>());
        optimizations.push_back(make_shared<AddDistributedAggregation>());

        optimizations.push_back(make_shared<EliminateRedundantProject>());
        optimizations.push_back(make_shared<PruneJoinRedundantColumns>());
        optimizations.push_back(make_shared<EliminateEmptyProject>());
        optimizations.push_back(make_shared<AddExchange>());

    }

    PlanNode* optimize(PlanNode *root){

        PlanNode *result = root;
        for(auto opti : this->optimizations)
        {
            result = opti->optimize(result,planNodeAllocator,variableAllocator,idAllocator);
        }
        return result;
    }




};
#endif //FRONTEND_OPTIMIZER_HPP
