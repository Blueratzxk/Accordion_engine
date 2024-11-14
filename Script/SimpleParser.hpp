//
// Created by zxk on 11/17/23.
//

#ifndef OLVP_SIMPLEPARSER_HPP
#define OLVP_SIMPLEPARSER_HPP


#include "SimpleLexer.hpp"
#include "spdlog/spdlog.h"

#include "Instruction.hpp"
#include "Script.hpp"
#include "ScriptParser.hpp"

//TraslationUnit -> BEGIN;Instuctions;END;
//Instructions => Instruction;Instuctions | Instruction;
//Instruction => Identifier Parameters
//Parameters => Parameter,Parameters | Parameter
//Parameter => Identifer | number

class SimpleParser:public ScriptParser
{
    shared_ptr<SimpleLexer> lexer;
public:
    SimpleParser(shared_ptr<SimpleLexer> lexer)
    {
        this->lexer = lexer;
    }

    bool parseParameter(string &paraValue)
    {
        SimpleToken token = this->lexer->ConsumeToken();
        if (!token.isIdentifier() && !token.isNumber())
        {
            spdlog::error("Parameter must be identifier or number.");
            return false;
        }
        paraValue = token.getValue();
        return true;

    }

    bool parseParameters(vector<string> &parameterValues)
    {

        string parameterValue;

        if (!parseParameter(parameterValue))
            return false;

        parameterValues.push_back(parameterValue);

        if (lexer->peekcurrent().getValue() == ",") {

            lexer->ConsumeToken(",");
            return parseParameters(parameterValues);
        }

        return true;
    }

    bool parseInstruction(Instruction &ins)
    {
        SimpleToken token = lexer->ConsumeToken();
        if (!token.isIdentifier()) {

            spdlog::error("Instruction name must be indentifier.");
            return false;
        }

        vector<string> parameterValues;
        if (!parseParameters(parameterValues))
            return false;

        Instruction instr(token.getValue(),parameterValues);
        ins = instr;

        return true;

    }

    bool parseInstructions(vector<Instruction> &instructions)
    {
        Instruction ins;
        if (!parseInstruction(ins))
        {
            return false;
        }
        if (!this->lexer->ConsumeToken(";"))
        {
            spdlog::error("Need ';' behind the Instuction.");
            return false;
        }
        instructions.push_back(ins);
        if (this->lexer->peekcurrent().isIdentifier())
        {
            return parseInstructions(instructions);
        }


        return true;
    }


    bool parseTranslationUnit(vector<Instruction> &instructions)
    {
        if (lexer->peekcurrent().isEnd())
            return true;

        if (lexer->peekcurrent().getValue() == "BEGIN")
        {
            lexer->ConsumeToken();
            if (!lexer->ConsumeToken(";"))
            {
                spdlog::error("Need ';' behind the 'BEGIN'.");
                return false;
            }
            if (lexer->peekcurrent().isIdentifier()) {
                if (!parseInstructions(instructions))
                {
                    return false;
                }
            }
            else if (lexer->peekcurrent().getValue() == "END")
            {
                lexer->ConsumeToken();
                if (!(this->lexer->ConsumeToken().getValue() == ";"))
                {
                    spdlog::error("Need ';' in the end.");
                    return false;
                }

            }
            else
            {
                spdlog::error("Instruction must start with identifier.");
                return false;
            }

            if (!(this->lexer->ConsumeToken().getValue() == "END"))
            {
                spdlog::error("Need 'END' in the end.");
                return false;
            }

            if (!(this->lexer->ConsumeToken().getValue() == ";"))
            {
                spdlog::error("Need ';' in the end.");
                return false;
            }


        }
        else
        {
            spdlog::error("TranslationUnit must start with 'BEGIN'.");
            return false;
        }

        return true;


    }

    bool Parse(vector<Script> &scripts)
    {
        if (!lexer->isSuccess()) {
            spdlog::error("LEX ERROR!");
            return false;
        }

        while (!this->lexer->isFinished()) {
            vector<Instruction> ins;
            if (parseTranslationUnit(ins)) {
                scripts.push_back(Script(ins));
            }
            else
            {
                spdlog::error("PARSE ERROR!");
                return false;
            }
        }

        return true;
    }

};

#endif //OLVP_SIMPLEPARSER_HPP
