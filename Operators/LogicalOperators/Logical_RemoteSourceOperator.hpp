//
// Created by zxk on 6/1/23.
//

#ifndef OLVP_LOGICAL_REMOTESOURCEOPERATOR_HPP
#define OLVP_LOGICAL_REMOTESOURCEOPERATOR_HPP

#include "LogicalOperator.hpp"
#include "../RemoteSourceOperator.hpp"



using namespace std;

class Logical_RemoteSourceOperator :public LogicalOperator
{

    string name = "Logical_RemoteSourceOperator";
    string downStreamIsProbeOrBuild;
    string operatorId;

public:
    ~Logical_RemoteSourceOperator(){}
    Logical_RemoteSourceOperator(string operatorId,string downStreamIsProbeOrBuild) :LogicalOperator("Logical_RemoteSourceOperator"){
        this->downStreamIsProbeOrBuild = downStreamIsProbeOrBuild;
        this->operatorId = operatorId;
    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {
        driverContext->setDownStreamHaveJoin(this->downStreamIsProbeOrBuild);
        return std::make_shared<RemoteSourceOperator>(this->operatorId,driverContext);
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {
        driverContext->setDownStreamHaveJoin(this->downStreamIsProbeOrBuild);
        return std::make_shared<RemoteSourceOperator>(this->operatorId,driverContext);
    }

    string getLogicalOperatorId() override
    {
        return this->operatorId;
    }

};




#endif //OLVP_LOGICAL_REMOTESOURCEOPERATOR_HPP
