//
// Created by zxk on 5/30/23.
//

#ifndef OLVP_PROJECTNODE_HPP
#define OLVP_PROJECTNODE_HPP

#include "PlanNode.hpp"
#include "../Planner/Assignments.hpp"
#include "../../Descriptor/ProjectDescriptor.hpp"
#include "../RowExpressionToAstExpression.hpp"
class ProjectNode :public PlanNode
{
    PlanNode *source;
    string id;
    shared_ptr<Assignments> assignments;
    ProjectAssignments projectAssignments;
    bool planPhase = false;
public:


    ProjectNode (string id,PlanNode *source,shared_ptr<Assignments> assignments):PlanNode("ProjectNode",id)
    {
        this->id = id;
        this->source = source;
        this->assignments = assignments;
        planPhase = true;
    }

    ProjectNode (string id,ProjectAssignments assignments):PlanNode("ProjectNode",id)
    {
        this->projectAssignments = assignments;
    }

    void transform() override
    {
        RowExpressionToAstExpression rowExpressionToAstExpression;

        planPhase = false;
        ProjectAssignments projectAssignmentsBuilder;

        auto assignmentsMap = this->assignments->getMap();

        for(auto assign : assignmentsMap) {
            auto result = (Node *) rowExpressionToAstExpression.Visit(assign.second.get(), NULL);

            if (assign.second->getExpressionName() == "CallExpression") {

                auto var = rowExpressionToAstExpression.getAllVariables().front();

                auto call = dynamic_pointer_cast<CallExpression>(assign.second);

                string afterArrowType = rowExpressionToAstExpression.getArrowType(var->getType()->getType());
                string beforeArrowType = rowExpressionToAstExpression.getArrowType(call->getReturnType()->getType());
                FieldDesc before(var->getName(), beforeArrowType);
                FieldDesc after(call->getDisplayName(),afterArrowType);
                projectAssignmentsBuilder.addAssignment(before,after,result);

            } else if(assign.second->getExpressionName() == "VariableReferenceExpression")
            {
                string afterArrowType = rowExpressionToAstExpression.getArrowType(assign.first->getType()->getType());


                FieldDesc after(assign.first->getName(), afterArrowType);

                auto var = dynamic_pointer_cast<VariableReferenceExpression>(assign.second);

                string beforeArrowType = rowExpressionToAstExpression.getArrowType(var->getType()->getType());

                FieldDesc before(var->getName(), afterArrowType);
                projectAssignmentsBuilder.addAssignment(before,after,NULL);
            }
            else
                spdlog::error("Project Node : transform error ! Wrong node type " + result->getNodeId() + "!");
        }
        this->projectAssignments = projectAssignmentsBuilder;
    }


    void* accept(NodeVisitor* visitor,void* context)  {
        return visitor->VisitProjectNode(this,context);
    }
    void addSource(PlanNode *node)
    {
        this->source = node;
    }
    void addSources(PlanNode *node){}
    PlanNode* getSource(){
        return this->source;
    }

    ProjectAssignments getProjectAssignments(){
        return this->projectAssignments;
    }

    shared_ptr<Assignments> getAssignments(){
        return this->assignments;
    }
    vector<shared_ptr<VariableReferenceExpression>> getOutputVariables() override{

        vector<shared_ptr<VariableReferenceExpression>> outputVariables;

        for(auto var : this->assignments->getOutputs())
            outputVariables.push_back(var);

        return outputVariables;
    }

    vector<PlanNode*> getSources(){
        vector<PlanNode*> sources{this->source};
        return sources;
    }
    string getId()
    {
        return PlanNode::getId();
    }

    PlanNode* replaceChildren(vector<PlanNode*> newChildren){

        if(!planPhase)
        {
            ProjectNode *proj = new ProjectNode(this->getId(),this->projectAssignments);
            proj->addSource(newChildren[0]);
            return proj;
        }

        ProjectNode *proj = new ProjectNode(id,source,assignments);
        proj->addSource(newChildren[0]);
        return proj;
    }



};




#endif //OLVP_PROJECTNODE_HPP
