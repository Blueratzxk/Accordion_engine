//
// Created by zxk on 11/17/23.
//

#ifndef OLVP_SCRIPTEXECUTOR_HPP
#define OLVP_SCRIPTEXECUTOR_HPP

#include "ScriptParser.hpp"
#include "SimpleLexer.hpp"
#include "SimpleParser.hpp"
#include "../Config/ExecutionConfig.hpp"
#include "InstructionInterpreter.hpp"
#include "InstructionExecutor.hpp"

class ScriptExecutor
{
    string scripts;
    shared_ptr<SimpleLexer> lexer;
    shared_ptr<ScriptParser> parser;
    vector<Script> ins;
    InstructionInterpreter instructionInterpreter;
    shared_ptr<InstructionExecutor> instructionExecutor;
    shared_ptr<QueryManager> queryManager;
    shared_ptr<ScriptQueryMonitor> monitor;

    atomic<bool> isScriptRunning = false;

    string readFileIntoString(char * filename)
    {
        ifstream ifile(filename);
        ostringstream buf;
        char ch;
        while (buf&&ifile.get(ch))
            buf.put(ch);
        ifile.close();
        return buf.str();
    }

public:
    ScriptExecutor(string scripts,shared_ptr<QueryManager> queryManager){
        this->scripts = scripts;
        this->lexer = make_shared<SimpleLexer>(this->scripts);
        this->parser = make_shared<SimpleParser>(this->lexer);
        this->queryManager = queryManager;
        this->monitor = make_shared<ScriptQueryMonitor>();
        this->monitor->setQueryManager(this->queryManager);
        this->instructionExecutor = make_shared<InstructionExecutor>(this->queryManager,this->monitor);
    }

    ScriptExecutor(shared_ptr<QueryManager> queryManager){


        this->queryManager = queryManager;
        this->monitor = make_shared<ScriptQueryMonitor>();
        this->monitor->setQueryManager(this->queryManager);
        this->instructionExecutor = make_shared<InstructionExecutor>(this->queryManager,this->monitor);
    }

    bool interpretScript(Script script)
    {
        for(auto instr : script.getInstructions())
        {
            if(this->instructionInterpreter.interpretInstruction(instr))
                ;
            else
            {
                spdlog::error("Script Interpret ERROR!");
                return false;
            }
        }
        return true;
    }


    bool executeScript(Script script)
    {
        ScriptExecutionContext context;
        for(auto instr : script.getInstructions())
        {
            if(this->instructionExecutor->executeInstruction(instr,context))
                ;
            else
            {
                spdlog::error("Script Execution ERROR!");
                return false;
            }
        }
        return true;
    }
    bool isRunning(){
        return this->isScriptRunning;
    }

    bool executeByCommand(string QueryName)
    {
        string script;
        script.append("BEGIN;");
        script.append("START_QUERY ");
        script.append(QueryName);
        script.append(";END;");

        this->isScriptRunning = true;

        ExecutionConfig config;
        string path = config.getScript_path();

        this->lexer = make_shared<SimpleLexer>(script);
        this->parser = make_shared<SimpleParser>(this->lexer);
        this->ins.clear();

        if(parser->Parse(ins))
            ;
        else {
            spdlog::error("Parse ERROR! Script Execution Aborted!");
            this->isScriptRunning = false;
            return false;
        }
        for(auto in : this->ins)
        {
            if(!this->interpretScript(in)) {
                spdlog::error("Interpret ERROR! Script Execution Aborted!");
                this->isScriptRunning = false;
                return false;
            }
        }
        for(auto in : this->ins)
        {
            if(!this->executeScript(in)) {
                spdlog::error("Execution ERROR! Script Execution Aborted!");
                this->isScriptRunning = false;
                return false;
            }
        }
        this->isScriptRunning = false;

        return true;

    }

    bool execute()
    {
        this->isScriptRunning = true;

        ExecutionConfig config;
        string path = config.getScript_path();
        this->scripts = readFileIntoString(const_cast<char *>(path.c_str()));
        this->lexer = make_shared<SimpleLexer>(this->scripts);
        this->parser = make_shared<SimpleParser>(this->lexer);
        this->ins.clear();

        if(parser->Parse(ins))
            ;
        else {
            spdlog::error("Parse ERROR! Script Execution Aborted!");
            this->isScriptRunning = false;
            return false;
        }
        for(auto in : this->ins)
        {
            if(!this->interpretScript(in)) {
                spdlog::error("Interpret ERROR! Script Execution Aborted!");
                this->isScriptRunning = false;
                return false;
            }
        }
        for(auto in : this->ins)
        {
            if(!this->executeScript(in)) {
                spdlog::error("Execution ERROR! Script Execution Aborted!");
                this->isScriptRunning = false;
                return false;
            }
        }
        this->isScriptRunning = false;

        return true;
    }


};


#endif //OLVP_SCRIPTEXECUTOR_HPP
