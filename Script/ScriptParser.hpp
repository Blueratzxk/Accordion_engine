//
// Created by zxk on 11/17/23.
//

#ifndef OLVP_SCRIPTPARSER_HPP
#define OLVP_SCRIPTPARSER_HPP

#include "Script.hpp"
class ScriptParser
{
public:
    virtual bool Parse(std::vector<Script> &scripts) = 0;
};
#endif //OLVP_SCRIPTPARSER_HPP
