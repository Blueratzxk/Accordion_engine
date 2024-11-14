//
// Created by zxk on 10/1/24.
//

#ifndef FRONTEND_FUNCTIONCALL_H
#define FRONTEND_FUNCTIONCALL_H

#include "Expression.h"
class FunctionCall : public Expression
{
    string functionName;
    vector<Expression *> arguments;

    string outputType;
public:
    FunctionCall(string location, string functionName,vector<Expression *> arguments):Expression(location,"FunctionCall")
    {
        this->functionName = functionName;
        this->arguments = arguments;
    }
    FunctionCall(string location,string Name,string outputType): Expression(location,"FunctionCall"){
        this->functionName = Name;
        this->outputType = outputType;
    }

    string getOutputType()
    {
        return this->outputType;
    }

    void addChilds(vector<Node*> childs) override
    {
        for(int i = 0 ; i < childs.size() ; i++)
            this->arguments.push_back((Expression*)childs[i]);
    }

    string getFuncName()
    {
        return functionName;
    }
    vector<Node *> getChildren() override
    {
        vector<Node *>results;
        for(auto arg : arguments)
        {
            results.push_back(arg);
        }
        return results;
    }

    bool equals(Expression *node) override
    {
        if(node->getExpressionId() != "FunctionCall")
            return false;
        if(this->functionName != ((FunctionCall*)node)->functionName)
            return false;

        if(this->arguments.size() != ((FunctionCall*)node)->arguments.size())
            return false;

        for(int i = 0 ; i < this->arguments.size() ; i++)
        {
            if(!this->arguments[i]->equals(((FunctionCall*)node)->arguments[i]))
                return false;
        }
        return true;
    }

    void* accept(AstNodeVisitor* visitor,void* context)  {
        return visitor->VisitFunctionCall(this,context);
    }


};


#endif //FRONTEND_FUNCTIONCALL_H
