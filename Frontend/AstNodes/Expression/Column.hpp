//
// Created by zxk on 5/18/23.
//

#ifndef OLVP_COLUMN_HPP
#define OLVP_COLUMN_HPP

#include "Expression.h"
class Column:public Expression
{
    string Value;
    string Type;


public:
    Column(string location,string value,string type): Expression(location,"Column"){
        this->Value = value;
        this->Type = type;
    }


    string getValue()
    {
        return this->Value;
    }
    string getColumnType()
    {
        return this->Type;
    }


    void* accept(AstNodeVisitor *visitor,void* context) {return visitor->VisitColumn(this,context);}

    vector<Node*> getChildren(){
      return {};
    }
};

#endif //OLVP_COLUMN_HPP
