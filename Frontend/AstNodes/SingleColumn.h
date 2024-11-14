//
// Created by zxk on 10/2/24.
//

#ifndef FRONTEND_SINGLECOLUMN_H
#define FRONTEND_SINGLECOLUMN_H

#include "SelectItem.h"
#include "Expression/Expression.h"
class SingleColumn : public SelectItem
{
    Expression *expression;
public:
    SingleColumn(string location,Expression *expression) : SelectItem(location,"SingleColumn"){
        this->expression = expression;
    }

    void* accept(AstNodeVisitor* visitor,void* context)  {
        return visitor->VisitSingleColumn(this,context);
    }

    Expression *getExpression(){
        return this->expression;
    }

    vector<Node *> getChildren() override
    {
        return {expression};
    }

};


#endif //FRONTEND_SINGLECOLUMN_H
