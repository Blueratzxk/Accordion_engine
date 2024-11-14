//
// Created by zxk on 5/30/23.
//

#ifndef OLVP_LOOKUPJOINNODE_HPP
#define OLVP_LOOKUPJOINNODE_HPP


#include "../Planner/Expression/EquiJoinClause.hpp"

#include "../../Descriptor/LookupJoinDescriptor.hpp"

class LookupJoinNode : public PlanNode
{

    PlanNode *probe;
    PlanNode *build;
    list<shared_ptr<EquiJoinClause>> joinClauses;
    vector<shared_ptr<VariableReferenceExpression>> outputVariables;

    LookupJoinDescriptor desc;

    bool planPhase = false;
public:


    LookupJoinNode(string id, PlanNode *probe, PlanNode *build,list<shared_ptr<EquiJoinClause>> joinClauses,vector<shared_ptr<VariableReferenceExpression>> outputVariables):PlanNode("LookupJoinNode",id)
    {
        this->probe = probe;
        this->build = build;
        this->joinClauses = joinClauses;
        this->outputVariables = outputVariables;
        planPhase = true;
    }

    LookupJoinNode(string id,LookupJoinDescriptor desc):PlanNode("LookupJoinNode",id)
    {

        this->desc = desc;
    }

    LookupJoinDescriptor getLookupJoinDescriptor()
    {
        return this->desc;
    }

    list<shared_ptr<EquiJoinClause>> getJoinClauses(){
        return this->joinClauses;
    }

    void transform() override {
        this->planPhase = false;

        vector<FieldDesc> probeInputSchema;
        vector<FieldDesc> buildInputSchema;
        vector<FieldDesc> buildOutputSchema;
        vector<int> probeOutputChannels;
        vector<int> probeHashChannels;
        vector<int> buildOutputChannels;
        vector<int> buildHashChannels;

        map<string, int> probeVariable;
        map<string, int> buildVariable;

        RowExpressionToAstExpression rowExpressionToAstExpression;

        for (int i = 0; i < this->probe->getOutputVariables().size(); i++) {
            auto field = this->probe->getOutputVariables()[i];

            string arrowType = rowExpressionToAstExpression.getArrowType(field->getType()->getType());

            probeInputSchema.push_back(FieldDesc(field->getName(), arrowType));
            probeVariable[field->getName()] = i;
        }
        for (int i = 0; i < this->build->getOutputVariables().size(); i++) {
            auto field = this->build->getOutputVariables()[i];

            string arrowType = rowExpressionToAstExpression.getArrowType(field->getType()->getType());

            buildInputSchema.push_back(FieldDesc(field->getName(),arrowType));
            buildVariable[field->getName()] = i;
        }

        auto joinOutputVariabes = this->outputVariables;

        for (int i = 0; i < joinOutputVariabes.size(); i++) {
            if (probeVariable.contains(joinOutputVariabes[i]->getName()))
                probeOutputChannels.push_back(probeVariable[joinOutputVariabes[i]->getName()]);
            else if (buildVariable.contains(joinOutputVariabes[i]->getName())) {
                buildOutputChannels.push_back(buildVariable[joinOutputVariabes[i]->getName()]);
                buildOutputSchema.push_back(buildInputSchema[buildVariable[joinOutputVariabes[i]->getName()]]);
            } else
                spdlog::error("Variable cannot be find in probe or build ?");
        }

        for (auto joinClause: this->joinClauses) {
            if (probeVariable.contains(joinClause->getLeft()->getName()))
                probeHashChannels.push_back(probeVariable[joinClause->getLeft()->getName()]);
            else if (buildVariable.contains(joinClause->getLeft()->getName()))
                buildHashChannels.push_back(buildVariable[joinClause->getLeft()->getName()]);
            else
                spdlog::error("Variable cannot be find in probe or build ?");

            if (probeVariable.contains(joinClause->getRight()->getName()))
                probeHashChannels.push_back(probeVariable[joinClause->getRight()->getName()]);
            else if (buildVariable.contains(joinClause->getRight()->getName()))
                buildHashChannels.push_back(buildVariable[joinClause->getRight()->getName()]);
            else
                spdlog::error("Variable cannot be find in probe or build ?");
        }

        LookupJoinDescriptor lookupJoinDescriptor(probeInputSchema, probeHashChannels, probeOutputChannels,
                                                  buildInputSchema,
                                                  buildHashChannels, buildOutputChannels, buildOutputSchema);

        this->desc = lookupJoinDescriptor;
    }

    void* accept(NodeVisitor* visitor,void* context)  {
        return visitor->VisitLookupJoinNode(this,context);
    }
    void addProbe(PlanNode *node)
    {
        this->probe = node;
    }
    void addBuild(PlanNode *node)
    {
        this->build = node;
    }

    PlanNode* getProbe(){
        return this->probe;
    }
    PlanNode* getBuild(){
        return this->build;
    }

    vector<shared_ptr<VariableReferenceExpression>> getOutputVariables() override{
        return this->outputVariables;
    }
    void addSources(vector<PlanNode*> nodes)
    {
        if(nodes.size() != 2){cout << "hash join node need 2 source!" << endl;exit(0);}
        this->probe = nodes[0];
        this->build = nodes[1];
    }
    void addSources(PlanNode *node){}
    vector<PlanNode*> getSources(){
        vector<PlanNode*> sources{this->probe,this->build};
        return sources;
    }
    string getId()
    {
        return PlanNode::getId();
    }

    PlanNode* replaceChildren(vector<PlanNode*> newChildren){

        if(!planPhase) {
            LookupJoinNode *hashJoin = new LookupJoinNode(this->getId(), desc);
            hashJoin->addProbe(newChildren[0]);
            hashJoin->addBuild(newChildren[1]);
            return hashJoin;
        }
        LookupJoinNode *hashJoin = new LookupJoinNode(this->getId(), probe, build, joinClauses, outputVariables);
        hashJoin->addProbe(newChildren[0]);
        hashJoin->addBuild(newChildren[1]);
        return hashJoin;
    }
};







#endif //OLVP_LOOKUPJOINNODE_HPP
