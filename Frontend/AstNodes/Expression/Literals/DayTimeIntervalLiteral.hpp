//
// Created by zxk on 6/17/23.
//

#ifndef OLVP_DAYTIMEINTERVALLITERAL_HPP
#define OLVP_DAYTIMEINTERVALLITERAL_HPP


#include "Literal.h"
class DayTimeIntervalLiteral:public Literal
{
    int32_t Value;
public:
    DayTimeIntervalLiteral(string location,int32_t value): Literal(location,"DayTimeIntervalLiteral")
    {
        this->Value = value;
    }

    bool equals(Expression *node) override
    {
        return (node->getExpressionId() == "Literal" &&
                ((Literal *) node)->getLiteralId() == "DayTimeIntervalLiteral" &&
                this->Value == ((DayTimeIntervalLiteral *) node)->Value);
    }
    int32_t getValue()
    {
        return this->Value;
    }
    void* accept(AstNodeVisitor *visitor,void* context) {return visitor->VisitDayTimeIntervalLiteral(this,context);}

    Literal * createNewOne() override{
        return new DayTimeIntervalLiteral(this->getLocation(),this->getValue());
    }
};



#endif //OLVP_DAYTIMEINTERVALLITERAL_HPP
