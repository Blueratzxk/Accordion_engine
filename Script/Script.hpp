//
// Created by zxk on 11/17/23.
//

#ifndef OLVP_SCRIPT_HPP
#define OLVP_SCRIPT_HPP

#include "Instruction.hpp"
using namespace std;
class Script
{
    vector<Instruction> instructions;
public:
    Script(vector<Instruction> ins){
        this->instructions = ins;
        this->instructions.insert(this->instructions.begin(),Instruction("BEGIN",{}));
        this->instructions.push_back(Instruction("END",{}));
    }

    vector<Instruction> getInstructions()
    {
        return this->instructions;
    }
    void view()
    {
        cout << endl;
        for(auto i : instructions)
        {
            cout <<"("<<i.getInstruction() << ")==>[";
            for(auto aa : i.getParameters())
            {
                cout << aa << " ";
            }
            cout << "]";
            cout << endl;
        }

    }


};
#endif //OLVP_SCRIPT_HPP
