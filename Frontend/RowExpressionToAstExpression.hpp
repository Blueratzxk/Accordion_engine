//
// Created by zxk on 11/10/24.
//

#ifndef OLVP_ROWEXPRESSIONTOASTEXPRESSION_HPP
#define OLVP_ROWEXPRESSIONTOASTEXPRESSION_HPP

#include "Planner/Expression/DefaultRowExpressionVisitor.hpp"
#include <map>
#include "AstNodes/Expression/Literals/Literals.h"
#include "Analyzer/FunctionCalls/AggregationFunctionHandle.hpp"
#include "Analyzer/FunctionCalls/SqlFunctionHandle.hpp"
#include "AstNodes/Expression/FunctionCall.h"
#include "AstNodes/Expression/Column.hpp"

class RowExpressionToAstExpression : public DefaultRowExpressionVisitor
{
    map<string,string> ToArrowFunctionCallMap = {
            {"BigInt_Add",                 "add"},
            {"BigInt_Subtract",            "subtract"},
            {"BigInt_Divide",              "divide"},
            {"BigInt_Multiply",            "multiply"},
            {"BigInt_Modules",             "mod"},
            {"BigInt_GreaterThan",         "greater_than"},
            {"BigInt_GreaterThanOrEqual",  "greater_than_or_equal_to"},
            {"BigInt_LessThan",            "less_than"},
            {"BigInt_LessThanOrEqual",     "less_than_or_equal_to"},
            {"BigInt_NotEqual",            "not_equal"},
            {"BigInt_Equal",               "equal"},
            {"BigInt_And",                 "and"},
            {"BigInt_Or",                  "or"},
            {"BigIntCastToDouble",         "castFLOAT8"},


            {"Double_Add",                 "add"},
            {"Double_Subtract",            "subtract"},
            {"Double_Divide",              "divide"},
            {"Double_Multiply",            "multiply"},
            {"Double_Modules",             "mod"},
            {"Double_GreaterThan",         "greater_than"},
            {"Double_GreaterThanOrEqual",  "greater_than_or_equal_to"},
            {"Double_LessThan",            "less_than"},
            {"Double_LessThanOrEqual",     "less_than_or_equal_to"},
            {"Double_NotEqual",            "not_equal"},
            {"Double_Equal",               "equal"},
            {"Double_And",                 "and"},
            {"Double_Or",                  "or"},
            {"DoubleCastToBigInt",         "castBIGINT"},


            {"Boolean_Add",                "add"},
            {"Boolean_Subtract",           "subtract"},
            {"Boolean_Divide",             "divide"},
            {"Boolean_Multiply",           "multiply"},
            {"Boolean_Modules",            "mod"},
            {"Boolean_GreaterThan",        "greater_than"},
            {"Boolean_GreaterThanOrEqual", "greater_than_or_equal_to"},
            {"Boolean_LessThan",           "less_than"},
            {"Boolean_LessThanOrEqual",    "less_than_or_equal_to"},
            {"Boolean_NotEqual",           "not_equal"},
            {"Boolean_Equal",              "equal"},
            {"Boolean_And",                "and"},
            {"Boolean_Or",                 "or"},
            {"BooleanCastToBigInt",        "castBIGINT"},
            {"BooleanCastToDouble",        "castFLOAT8"},



            {"sum_BigInt",        "sum"},
            {"sum_Double",        "sum"},
            {"sum_BigInt_groupBy",        "hash_sum"},
            {"sum_Double_groupBy",        "hash_sum"},
            {"count",        "count"},
            {"count_groupBy",        "hash_count"},
            {"countStar",        "count_all"}
    };


    map<string,string> ToArrowTypeMap = {
            {"bigint", "int64"},
            {"double", "double"},
            {"decimal", "double"},
            {"date", "date32"},
            {"varchar", "string"},
            {"char", "string"},
            {"int64", "int64"},
            {"int32", "int64"},
            {"bool", "bool"},
            {"boolean", "bool"}
    };

    vector<VariableReferenceExpression*> allVariables;
    string expressionString;

public:
    string getArrowType(string origin)
    {
        if(ToArrowTypeMap.contains(origin))
            return ToArrowTypeMap[origin];

        spdlog::error("Cannot find corresponding arrow type for "+origin + "!");
        return "unknown";
    }

    string getArrowFunctionName(string origin)
    {
        if(this->ToArrowFunctionCallMap.contains(origin))
            return ToArrowFunctionCallMap[origin];
        else {
            spdlog::error("Cannot find corresponding arrow function name for "+origin + "!");
            return "unknown";
        }
    }


    class LiteralProvider
    {
    public:
        LiteralProvider(){

        }
        static Literal * getLiteral(string value,string type)
        {
            if(type == "bigint")
                return new Int64Literal("0",value);
            else if(type == "double")
                return new DoubleLiteral("0",value);
            else if(type == "decimal")
                return new DoubleLiteral("0",value);
            else if(type == "int")
                return new Int64Literal("0",value);
            else if(type == "int32")
                return new Int64Literal("0",value);
            else if(type == "int64")
                return new Int64Literal("0",value);
            else if(type == "smallint")
                return new Int64Literal("0",value);
            else if(type == "varchar")
                return new StringLiteral("0",value);
            else if(type == "date")
                return new Date32Literal("0",value);
            else
                spdlog::error("RowExpressionToAstExpression encounters unknown type "+type+"!");
            return NULL;
        }
    };

    string getExpressionString(){
        return this->expressionString;
    }

    void resetExpressionString(){
        this->expressionString = "";
    }


    class FunctionCallProvider
    {
        RowExpressionToAstExpression *rowExpressionToAstExpression;
    public:
        FunctionCallProvider(RowExpressionToAstExpression *rowExpressionToAstExpression){
            this->rowExpressionToAstExpression = rowExpressionToAstExpression;
        }
        FunctionCall * getFunction(string functionName,string returnType,vector<Node*> arguments)
        {

            string arrowReturnType = rowExpressionToAstExpression->getArrowType(returnType);
            auto function = new FunctionCall("0",functionName,returnType);
            function->addChilds(arguments);
            return function;
        }
    };




public:
    RowExpressionToAstExpression(){

    }
    vector<VariableReferenceExpression*> getAllVariables()
    {
        return this->allVariables;
    }

    void * VisitConstant(ConstantExpression *literal, void *context) override
    {
        string arrowType = getArrowType(literal->getType()->getType());

        expressionString.append(literal->getNameOrValue()+",");

        return LiteralProvider::getLiteral(literal->getNameOrValue(),arrowType);
    }

     void * VisitCall(CallExpression *call, void *context) override {
         FunctionCallProvider functionCallProvider(this);

         string funcName;
         if (call->getFunctionHandle()->getName() == "AggregationFunctionHandle")
             funcName = dynamic_pointer_cast<AggregationFunctionHandle>(call->getFunctionHandle())->getFunctionId();

         else if (call->getFunctionHandle()->getName() == "SqlFunctionHandle")
             funcName = dynamic_pointer_cast<SqlFunctionHandle>(call->getFunctionHandle())->getFunctionId();

         string arrowName = ToArrowFunctionCallMap[funcName];

         vector<Node *> childNodes;

         expressionString.append(arrowName+"(");

         for (auto argument: call->getArguments())
             childNodes.push_back((Node *) Visit(argument.get(), context));

         if(expressionString.back() == ',')
             expressionString.pop_back();
         expressionString.append(")");


         string arrowType = getArrowType(call->getReturnType()->getType());


         return functionCallProvider.getFunction(arrowName, arrowType, childNodes);

     }

     void * VisitInputReference(InputReferenceExpression *reference, void *context) override
     {
        return NULL;
     }

     void * VisitVariableReference(VariableReferenceExpression *reference, void *context) override
     {

         allVariables.push_back(reference);
         string arrowType = getArrowType(reference->getType()->getType());

         expressionString.append(reference->getName() + ",");
         return new Column("0", reference->getName(), arrowType);
     }

};



#endif //OLVP_ROWEXPRESSIONTOASTEXPRESSION_HPP
