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

public:
    ~Logical_RemoteSourceOperator(){}
    Logical_RemoteSourceOperator(string downStreamIsProbeOrBuild) {
        this->downStreamIsProbeOrBuild = downStreamIsProbeOrBuild;
    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {
        driverContext->setDownStreamHaveJoin(this->downStreamIsProbeOrBuild);
        return std::make_shared<RemoteSourceOperator>(driverContext);
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {
        driverContext->setDownStreamHaveJoin(this->downStreamIsProbeOrBuild);
        return std::make_shared<RemoteSourceOperator>(driverContext);
    }
    string getTypeId(){return name;}


};




#endif //OLVP_LOGICAL_REMOTESOURCEOPERATOR_HPP
