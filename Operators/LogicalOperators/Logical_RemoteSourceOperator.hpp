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
    bool sidewayDataSync;

public:
    ~Logical_RemoteSourceOperator(){}
    Logical_RemoteSourceOperator(string operatorId,string downStreamIsProbeOrBuild,bool sidewayDataSync = false) :LogicalOperator("Logical_RemoteSourceOperator"){
        this->downStreamIsProbeOrBuild = downStreamIsProbeOrBuild;
        this->operatorId = operatorId;
        this->sidewayDataSync = sidewayDataSync;
    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {
        driverContext->setDownStreamHaveJoin(this->downStreamIsProbeOrBuild);
        auto op = std::make_shared<RemoteSourceOperator>(this->operatorId,driverContext);
        if(this->sidewayDataSync)
            op->waitSidewayDataSync();
        return op;
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {
        driverContext->setDownStreamHaveJoin(this->downStreamIsProbeOrBuild);
        auto op = std::make_shared<RemoteSourceOperator>(this->operatorId,driverContext);
        if(this->sidewayDataSync)
            op->waitSidewayDataSync();
        return op;
    }

    string getLogicalOperatorId() override
    {
        return this->operatorId;
    }

};




#endif //OLVP_LOGICAL_REMOTESOURCEOPERATOR_HPP
