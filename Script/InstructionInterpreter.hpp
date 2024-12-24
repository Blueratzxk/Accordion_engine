//
// Created by zxk on 11/17/23.
//

#ifndef OLVP_INSTRUCTIONINTERPRETER_HPP
#define OLVP_INSTRUCTIONINTERPRETER_HPP



#include "Instruction.hpp"

using namespace std;
class InstructionInterpreter
{

    typedef bool  (InstructionInterpreter::*Fun_ptr)(Instruction instruction);
    map<string, Fun_ptr> funcMap;


    bool isNum(string str) {
        stringstream sin(str);
        double d;
        char c;
        if (!(sin >> d)) {
            return false;
        }
        if (sin >> c) {
            return false;
        }
        return true;
    }


        public:
    InstructionInterpreter()
    {
        initInstructions();
    }
    void initInstructions()
    {
        funcMap.insert(make_pair("WAIT_QUERY", &InstructionInterpreter::WAIT_QUERY));
        funcMap.insert(make_pair("START_QUERY", &InstructionInterpreter::START_QUERY));
        funcMap.insert(make_pair("BEGIN", &InstructionInterpreter::BEGIN));
        funcMap.insert(make_pair("END", &InstructionInterpreter::END));
        funcMap.insert(make_pair("ADD_CONCURRENCY", &InstructionInterpreter::ADD_CONCURRENCY));
        funcMap.insert(make_pair("ADD_PARALLELISM", &InstructionInterpreter::ADD_PARALLELISM));
        funcMap.insert(make_pair("ADD_PARALLELISM_TO_INIT", &InstructionInterpreter::ADD_PARALLELISM_TO_INIT));

        funcMap.insert(make_pair("R_CONFIG", &InstructionInterpreter::RUNTIME_CONFIG));
        funcMap.insert(make_pair("LISTEN", &InstructionInterpreter::LISTEN));

        funcMap.insert(make_pair("ECHO", &InstructionInterpreter::ECHO_INFO));

        funcMap.insert(make_pair("PREDICT_TIME", &InstructionInterpreter::PREDICT_TIME));

        funcMap.insert(make_pair("START_AND_COLLECT", &InstructionInterpreter::START_AND_COLLECT));

        funcMap.insert(make_pair("START_AUTO_TUNE", &InstructionInterpreter::START_AUTO_TUNE));


    }
    bool RUNTIME_CONFIG(Instruction instruction)
    {
        vector<string> parameters = instruction.getParameters();
        if(parameters.size() !=3)
        {
            spdlog::error("R_CONFIG Instruction needs three parameters.Like \"R_CONFIG SET_TABLE_SCAN_SIZE,tpch_test_tpch_1_lineitem,3;\"");
            return false;
        }

        if(parameters[0]=="SET_TABLE_SCAN_SIZE" && parameters.size() == 3 && isNum(parameters[2]) && atoi(parameters[2].c_str()) > 0)
        {

            spdlog::debug("SET table "+parameters[1]+" scanning size "+parameters[2]+".");
            return true;

        }
        else
        {
            string info;
            info+=instruction.getInstruction()+" ";
            for(auto i : instruction.getParameters())
            {
                info+=i;
                info+=",";
            }
            info.pop_back();

            spdlog::error("["+info+"] Instruction or parameters ERROR!");
            return false;
        }
    }

    bool LISTEN(Instruction instruction)
    {
        vector<string> parameters = instruction.getParameters();
        if(parameters.size() !=3 || parameters[0] != "BUILD" || parameters[1] != "STAGE" || !isNum(parameters[2]))
        {
            spdlog::error("LISTEN Instruction need three parameters.Like \"LISTEN BUILD,STAGE,3;\"");
            return false;
        }

        spdlog::debug("Listen the event of stage "+parameters[2]+" build Hash Table!");
        return true;
    }

    bool BEGIN(Instruction instruction)
    {
        spdlog::debug("Script begins!");
        return true;
    }
    bool END(Instruction instruction)
    {
        spdlog::debug("Script ends!");
        return true;
    }
    bool START_QUERY(Instruction instruction)
    {
        vector<string> parameters = instruction.getParameters();
        if(parameters.size() != 1)
        {
            spdlog::error("Start_Query Instruction only need one parameter.");
            return false;
        }
        else
        {
            spdlog::debug("Query "+parameters[0]+" start!");
            return true;
        }
    }

    bool START_AND_COLLECT(Instruction instruction)
    {
        vector<string> parameters = instruction.getParameters();
        if(parameters.size() != 1)
        {
            spdlog::error("START_AND_COLLECT Instruction only need one parameter.");
            return false;
        }
        else
        {
            spdlog::debug("Query "+parameters[0]+" start!");
            return true;
        }
    }

    bool START_AUTO_TUNE(Instruction instruction)
    {
        vector<string> parameters = instruction.getParameters();
        if(parameters.size() != 1)
        {
            spdlog::error("START_AUTO_TUNE Instruction only need one parameter.");
            return false;
        }
        else
        {
            spdlog::debug("Query "+parameters[0]+" start!");
            return true;
        }
    }

    bool WAIT_QUERY(Instruction instruction)
    {
        vector<string> parameters = instruction.getParameters();
        if(parameters.size() != 1)
        {
            spdlog::error("WAIT_Query Instruction only need one parameter.");
            return false;
        }
        else if(!isNum(parameters[0]))
        {
            spdlog::error("WAITING TIME must be a number!");
            return false;
        }
        else
        {
            spdlog::debug("Waiting the query "+parameters[0]+" ms.");
            return true;
        }
    }

    bool ECHO_INFO(Instruction instruction)
    {
        vector<string> parameters = instruction.getParameters();
        if(parameters.size() != 1)
        {
            spdlog::error("ECHO Instruction only need one parameter.");
            return false;
        }
        else
        {
            spdlog::debug("Waiting the query "+parameters[0]+" ms.");
            return true;
        }
    }

    bool ADD_CONCURRENCY(Instruction instruction)
    {
        vector<string> parameters = instruction.getParameters();
        if(parameters.size() != 2 && parameters.size() !=3)
        {
            spdlog::error("ADD_PARALLELISM Instruction need two or three parameters.Like \"ADD_PARALLELISM QUERY,3;\"");
            return false;
        }

        if(parameters[0]=="QUERY" && parameters.size() == 2 && isNum(parameters[1]) && atoi(parameters[1].c_str()) > 0)
        {

            spdlog::debug("Add parallelism for all stages of query with degree of "+parameters[1]+".");
            return true;

        }
        else if(parameters[0]=="STAGE" && parameters.size() == 3 && isNum(parameters[1]) && isNum(parameters[2]) && atoi(parameters[1].c_str()) > 0 && atoi(parameters[2].c_str()) > 0)
        {
            spdlog::debug("Add concurrency for stage "+parameters[1]+" with degree of "+parameters[2]+".");
            return true;
        }
        else
        {
            string info;
            info+=instruction.getInstruction()+" ";
            for(auto i : instruction.getParameters())
            {
                info+=i;
                info+=",";
            }
            info.pop_back();

            spdlog::error("["+info+"] Instruction or parameters ERROR!");
            return false;
        }
    }

    bool ADD_PARALLELISM(Instruction instruction)
    {
        vector<string> parameters = instruction.getParameters();
        if(parameters.size() != 2 && parameters.size() !=3)
        {
            spdlog::error("ADD_PARALLELISM Instruction need two or three parameters.Like \"ADD_PARALLELISM QUERY,3;\"");
            return false;
        }

        if(parameters[0]=="QUERY" && parameters.size() == 2 && isNum(parameters[1]) && atoi(parameters[1].c_str()) > 0)
        {

            spdlog::debug("Add parallelism for all stages of query with degree of "+parameters[1]+".");
            return true;

        }
        else if(parameters[0]=="STAGE" && parameters.size() == 3 && isNum(parameters[1]) && isNum(parameters[2]) && atoi(parameters[1].c_str()) > 0 && atoi(parameters[2].c_str()) > 0)
        {
            spdlog::debug("Add parallelism for stage "+parameters[1]+" with degree of "+parameters[2]+".");
            return true;
        }
        else
        {
            string info;
            info+=instruction.getInstruction()+" ";
            for(auto i : instruction.getParameters())
            {
                info+=i;
                info+=",";
            }
            info.pop_back();

            spdlog::error("["+info+"] Instruction or parameters ERROR!");
            return false;
        }
    }

    bool ADD_PARALLELISM_TO_INIT(Instruction instruction)
    {
        vector<string> parameters = instruction.getParameters();
        if(parameters.size() != 2 && parameters.size() !=3)
        {
            spdlog::error("ADD_PARALLELISM_TO_INIT Instruction need two or three parameters.Like \"ADD_PARALLELISM_TO_INIT QUERY,3;\"");
            return false;
        }

        if(parameters[0]=="QUERY" && parameters.size() == 2 && isNum(parameters[1]) && atoi(parameters[1].c_str()) > 0)
        {

            spdlog::debug("Add parallelism for all stages of query with degree of "+parameters[1]+".");
            return true;

        }
        else if(parameters[0]=="STAGE" && parameters.size() == 3 && isNum(parameters[1]) && isNum(parameters[2]) && atoi(parameters[1].c_str()) > 0 && atoi(parameters[2].c_str()) > 0)
        {
            spdlog::debug("Add parallelism for stage "+parameters[1]+" with degree of "+parameters[2]+".");
            return true;
        }
        else
        {
            string info;
            info+=instruction.getInstruction()+" ";
            for(auto i : instruction.getParameters())
            {
                info+=i;
                info+=",";
            }
            info.pop_back();

            spdlog::error("["+info+"] Instruction or parameters ERROR!");
            return false;
        }
    }


    bool PREDICT_TIME(Instruction instruction)
    {
        vector<string> parameters = instruction.getParameters();
        if(parameters.size() !=3)
        {
            spdlog::error("PREDICT_TIME Instruction need three parameters.Like \"PREDICT_TIME STAGE,3,2;\"");
            return false;
        }


        if(parameters[0]=="STAGE" && parameters.size() == 3 && isNum(parameters[1]) && isNum(parameters[2]) && atoi(parameters[1].c_str()) > 0 && atoi(parameters[2].c_str()) > 0)
        {
            spdlog::debug("Predict time for stage "+parameters[1]+" when DOP expands "+parameters[2]+"times.");
            return true;
        }
        else
        {
            string info;
            info+=instruction.getInstruction()+" ";
            for(auto i : instruction.getParameters())
            {
                info+=i;
                info+=",";
            }
            info.pop_back();

            spdlog::error("["+info+"] Instruction or parameters ERROR!");
            return false;
        }
    }

    bool interpretInstruction(Instruction ins)
    {
        if(this->funcMap.count(ins.getInstruction()) == 0)
        {
            spdlog::error("Interpret ERROR! Unknown Instruction "+ins.getInstruction()+"!");
            return false;
        }
        return (this->*funcMap[ins.getInstruction()])(ins);
    }

};

#endif //OLVP_INSTRUCTIONINTERPRETER_HPP
