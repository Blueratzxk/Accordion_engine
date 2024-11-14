//
// Created by zxk on 10/1/24.
//

#ifndef FRONTEND_GROUPINGELEMENT_H
#define FRONTEND_GROUPINGELEMENT_H

#include "Node.h"
#include "Expression/Expression.h"
class GroupingElement:public Node
{
    string groupingElementName;
public:
    GroupingElement(string location,string groupingElementName):Node(location,"GroupingElement"){
        this->groupingElementName = groupingElementName;
    }

    string getGroupingElementName()
    {
        return this->groupingElementName;
    }


    void* accept(AstNodeVisitor* visitor,void* context)  {
        return visitor->VisitGroupingElement(this,context);
    }

    ~GroupingElement() override = default;

};


#endif //FRONTEND_GROUPINGELEMENT_H
