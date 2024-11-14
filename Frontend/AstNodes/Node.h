//
// Created by zxk on 10/1/24.
//

#ifndef FRONTEND_NODE_H
#define FRONTEND_NODE_H
#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <list>
using namespace std;
class AstNodeVisitor;

class Node
{
    string location;
    string NodeId;
public:
    Node(string location,string NodeId)
    {
        this->location = location;
        this->NodeId = NodeId;
    }

    string getLocation(){
        return this->location;
    }

    string getNodeId()
    {
        return this->NodeId;
    }

    virtual void* accept(AstNodeVisitor *visitor,void* context) {return NULL;}

    virtual vector<Node*> getChildren() {return {};}

    virtual void addChilds(vector<Node*> nodes){}

    virtual ~Node()= default;
};


#endif //FRONTEND_NODE_H
