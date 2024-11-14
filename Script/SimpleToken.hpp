//
// Created by zxk on 11/17/23.
//

#ifndef OLVP_SIMPLETOKEN_HPP
#define OLVP_SIMPLETOKEN_HPP




#define CODE_KEYWORD 0
#define CODE_NUMBER 1
#define CODE_DELIMETER 2
#define CODE_OPERATOR 3
#define CODE_IDENTIFIER 4
#define CODE_END 5

#include <string>

using namespace  std;

class SimpleToken
{
    int type;
    string value;

public:
    SimpleToken(int type,string value)
    {
        this->type = type;
        this->value = value;
    }
    string getType()
    {
        if (this->type == CODE_KEYWORD)
        {
            return "keyword";
        }
        else if (this->type == CODE_NUMBER)
        {
            return "number";
        }
        else if (this->type == CODE_DELIMETER)
        {
            return "delimeter";
        }
        else if (this->type == CODE_OPERATOR)
        {
            return "operator";
        }
        else if (this->type == CODE_IDENTIFIER)
        {
            return "identifier";
        }
        else
        {
            return "unknown";
        }
    }
    string getValue()
    {
        return this->value;
    }
    bool isKeyword()
    {
        return this->type == CODE_KEYWORD;
    }
    bool isDelimeter()
    {
        return this->type == CODE_DELIMETER;

    }
    bool isNumber()
    {
        return this->type == CODE_NUMBER;
    }
    bool isOperator()
    {
        return this->type == CODE_OPERATOR;
    }
    bool isIdentifier()
    {
        return this->type == CODE_IDENTIFIER;
    }
    bool isEnd()
    {
        return this->type == CODE_END;
    }

};



#endif //OLVP_SIMPLETOKEN_HPP
