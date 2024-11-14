//
// Created by zxk on 5/28/23.
//

#ifndef OLVP_PHYSICALPIPELINECREATER_HPP
#define OLVP_PHYSICALPIPELINECREATER_HPP

#include "LogicalOperator.hpp"
#include <vector>

class PhysicalPipelineCreater
{

public:

    static std::vector<std::shared_ptr<Operator>> createOperatorPipeline(std::vector<std::shared_ptr<LogicalOperator>> logicalOperators)
    {
        std::vector<std::shared_ptr<Operator>> physicalOperators;

        for(auto op : logicalOperators)
        {
            physicalOperators.push_back(op->getOperator());
        }
        return physicalOperators;
    }

    static std::vector<std::shared_ptr<void>> createNonTypePipeline(std::vector<std::shared_ptr<LogicalOperator>> logicalOperators)
    {
        std::vector<std::shared_ptr<void>> physicalOperators;

        for(auto op : logicalOperators)
        {
            physicalOperators.push_back(op->getOperatorNonType());
        }
        return physicalOperators;
    }

    static std::shared_ptr<std::vector<std::shared_ptr<Operator>>> createOperatorPipelinePtr(std::vector<std::shared_ptr<LogicalOperator>> logicalOperators)
    {
        std::shared_ptr<std::vector<std::shared_ptr<Operator>>> physicalOperators = std::make_shared<std::vector<std::shared_ptr<Operator>>>();

        for(auto op : logicalOperators)
        {
            physicalOperators->push_back(op->getOperator());
        }
        return physicalOperators;
    }


};


#endif //OLVP_PHYSICALPIPELINECREATER_HPP
