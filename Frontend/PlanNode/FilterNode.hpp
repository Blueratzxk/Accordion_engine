//
// Created by zxk on 5/30/23.
//

#ifndef OLVP_FILTERNODE_HPP
#define OLVP_FILTERNODE_HPP

#include "../../Descriptor/FilterDescriptor.hpp"

#include "../RowExpressionToAstExpression.hpp"

class NodeVisitor;
class FilterNode:public PlanNode
{

    PlanNode *source;

    string location;
    string id;
    shared_ptr<RowExpression> predicates;

    FilterDescriptor fdesc;

    bool planPhase = false;
public:

    FilterNode(string location,string id, PlanNode *source,shared_ptr<RowExpression> predicates):PlanNode("FilterNode",id)
    {
        this->location = location;
        this->id = id;
        this->source = source;
        this->predicates = predicates;
        this->planPhase = true;
    }

    FilterNode(string id,FilterDescriptor fdesc):PlanNode("FilterNode",id)
    {
        this->fdesc = fdesc;
    }

    FilterDescriptor getFilterDesc()
    {
        return this->fdesc;
    }

    void transform()
    {
        this->planPhase = false;
        RowExpressionToAstExpression rowExpressionToAstExpression;
        auto result = (Node*)rowExpressionToAstExpression.Visit(this->predicates.get(),NULL);

        auto inputVariables = this->source->getOutputVariables();

        vector<FieldDesc> inputFields;

        for(auto input : inputVariables)
        {
            string type = input->getType()->getType();
            type = rowExpressionToAstExpression.getArrowType(type);
            FieldDesc field(input->getName(),type);
            inputFields.push_back(field);
        }

        this->fdesc = FilterDescriptor(inputFields, std::make_shared<AstNodePtr>(result));

    }

    void* accept(NodeVisitor* visitor,void*context)  {
        return visitor->VisitFilterNode(this,context);
    }


    shared_ptr<RowExpression> getPredicates()
    {
        return this->predicates;
    }

    void addSource(PlanNode *node)
    {
        this->source = node;
    }
    void addSources(PlanNode *node){}
    PlanNode* getSource(){
        return this->source;
    }

    vector<PlanNode*> getSources(){
        vector<PlanNode*> sources{this->source};
        return sources;
    }
    vector<shared_ptr<VariableReferenceExpression>> getOutputVariables() override
    {
        return source->getOutputVariables();
    }

    string getSourceLocation()
    {
        return this->location;
    }
    string getId ()
    {
        return PlanNode::getId();
    }

    PlanNode* replaceChildren(vector<PlanNode*> newChildren){

        if(!planPhase)
        {
            FilterNode *filter = new FilterNode(this->getId(),this->getFilterDesc());
            filter->addSource(newChildren[0]);
            return filter;
        }


        FilterNode *filter = new FilterNode(location,id,source,predicates);
        filter->addSource(newChildren[0]);
        return filter;
    }



};




#endif //OLVP_FILTERNODE_HPP
