//
// Created by zxk on 11/9/24.
//

#ifndef OLVP_EXPROUTPUT_HPP
#define OLVP_EXPROUTPUT_HPP
#include <string>
class ExprOutput
{
    std::string outputName;
    std::string outputType;
public:
    ExprOutput(){}
    ExprOutput(std::string name,std::string type)
    {
        this->outputName = name;
        this->outputType = type;
    }

    std::string getName()
    {
        return this->outputName;
    }

    std::string getType()
    {
        return this->outputType;
    }
};

#endif //OLVP_EXPROUTPUT_HPP
