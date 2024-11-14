//
// Created by zxk on 5/28/23.
//

#ifndef OLVP_LOGICALOPERATOR_HPP
#define OLVP_LOGICALOPERATOR_HPP

#include <string>
#include <memory>
#include "../../Execution/Task/Context/DriverContext.h"
class Operator;

class LogicalOperator {
public:

    virtual std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) { return NULL; };
    virtual std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) { return NULL; };
    virtual std::string getTypeId(){return "";};

};


#endif //OLVP_LOGICALOPERATOR_HPP
