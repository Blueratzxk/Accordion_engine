//
// Created by zxk on 10/1/24.
//

#ifndef FRONTEND_GROUPBY_H
#define FRONTEND_GROUPBY_H


class GroupBy : public Node
{

    bool isDistinct;
    list<GroupingElement*> groupingElements;

public:
    GroupBy(string location, bool isDistinct,list<GroupingElement*> groupingElements) : Node(location,"GroupBy")
    {
        this->isDistinct = isDistinct;
        this->groupingElements = groupingElements;
    }
    list<GroupingElement*> getGroupingElements()
    {
        return this->groupingElements;
    }

    void* accept(AstNodeVisitor* visitor,void* context)  {
        return visitor->VisitGroupBy(this,context);
    }

    vector<Node *> getChildren() override
    {
        vector<Node *> result;
        for(auto ele : groupingElements)
            result.push_back(ele);
        return result;
    }



};
#endif //FRONTEND_GROUPBY_H
