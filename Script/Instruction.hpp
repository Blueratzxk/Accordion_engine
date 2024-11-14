//
// Created by zxk on 11/17/23.
//

#ifndef OLVP_INSTRUCTION_HPP
#define OLVP_INSTRUCTION_HPP

#include "../common.h"

class Instruction
{
    std::string ins;
    std::vector<std::string> paras;
public:
    Instruction(){}
    Instruction(std::string ins,std::vector<std::string> paras){
        this->ins = ins;
        this->paras = paras;
    }
    std::string getInstruction()
    {
        return this->ins;
    }
    std::vector<std::string> getParameters()
    {
        return this->paras;
    }



};


#endif //OLVP_INSTRUCTION_HPP
