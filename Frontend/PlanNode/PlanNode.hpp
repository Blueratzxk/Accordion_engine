//
// Created by zxk on 10/21/24.
//

#ifndef FRONTEND_PLANNODE_HPP
#define FRONTEND_PLANNODE_HPP

#include <iostream>
#include <string>
#include <vector>


using namespace std;

class VariableReferenceExpression;
class NodeVisitor;

class PlanNode {
private:
    string type;
    string id = "-1";
    string label;
public:


    PlanNode(string type, string id) {
        this->type = type;
        this->id = id;
        this->label = "";
    }


    string getId() {
        return id;
    }

    string getType() {
        return type;
    }
    string getLabel()
    {
        return this->label;
    }
    void setLabel(string label)
    {
        this->label = label;
    }

    virtual void transform(){}

    virtual PlanNode *replaceChildren(vector<PlanNode *> newChildren) { return NULL; };

    virtual vector<PlanNode *> getSources() {
        vector<PlanNode *> null;
        return null;
    };

    virtual void addSources(vector<PlanNode *> nodes) {};

    virtual void addSource(PlanNode *) {};

    virtual PlanNode *getSource() { return NULL; };

    virtual void *accept(NodeVisitor *visitor, void *context) { return NULL; };

    virtual vector<shared_ptr<VariableReferenceExpression>> getOutputVariables(){return {};};

    virtual ~PlanNode() = default;

};


#endif //FRONTEND_PLANNODE_HPP
