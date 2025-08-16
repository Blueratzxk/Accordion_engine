//
// Created by zxk on 8/16/25.
//

#ifndef OLVP_GPUFILTEREXPREVALUATOR_HPP
#define OLVP_GPUFILTEREXPREVALUATOR_HPP


#include "../Frontend/AstNodes/tree.h"
#include "../Frontend/AstNodes/DefaultAstNodeVisitor.hpp"

#include "../Utils/TimeCommon.hpp"
#include "../Utils/ArrowDicts.hpp"
#include "GPUFunctions.h"
#include "../Page/DataPage.hpp"
class EvaResult
{
    void* ptr = nullptr;
    GPUFunctions gpuFunctions;

public:

    EvaResult(void* ptr)
    {
        this->ptr = ptr;
    }

    void freeResult()
    {
        if(this->ptr != NULL) {

        }
    }


    void* getColumn()
    {
        return this->ptr;
    }

};


class AstTreeEvaluator: public DefaultAstNodeVisitor
{

    std::shared_ptr<arrow::Schema> input_schemaIn;
    shared_ptr<DataPage> page;
    GPUFunctions gpuFunctions;

public:



    AstTreeEvaluator(std::shared_ptr<arrow::Schema> input_schemaIn, shared_ptr<DataPage> page){
        this->input_schemaIn = input_schemaIn;
        this->page = page;

    }


    void* VisitFunctionCall(FunctionCall* node,void* context) {


        vector<Node*> arguments = node->getChildren();

        vector<EvaResult*> results;
        for(int i = 0 ; i < arguments.size() ; i++)
        {
            auto re = Visit(arguments[i],NULL);
            results.push_back((EvaResult*)re);
        }

        EvaResult *outputResult = NULL;

        if(node->getFuncName() == "subtract")
        {
            auto re = gpuFunctions.cudfFunctionCall(node->getFuncName(),{results[0]->getColumn(),results[1]->getColumn()},node->getOutputType());
            outputResult = new EvaResult(re);
        }
        else if(node->getFuncName() == "less_than_or_equal_to")
        {
            auto re = gpuFunctions.cudfFunctionCall(node->getFuncName(),{results[0]->getColumn(),results[1]->getColumn()},node->getOutputType());
            outputResult = new EvaResult(re);
        }
        else if(node->getFuncName() == "less_than")
        {
            auto re = gpuFunctions.cudfFunctionCall(node->getFuncName(),{results[0]->getColumn(),results[1]->getColumn()},node->getOutputType());
            outputResult = new EvaResult(re);
        }
        else if(node->getFuncName() == "greater_than")
        {
            auto re = gpuFunctions.cudfFunctionCall(node->getFuncName(),{results[0]->getColumn(),results[1]->getColumn()},node->getOutputType());
            outputResult = new EvaResult(re);
        }
        else if(node->getFuncName() == "greater_than_or_equal_to")
        {
            auto re = gpuFunctions.cudfFunctionCall(node->getFuncName(),{results[0]->getColumn(),results[1]->getColumn()},node->getOutputType());
            outputResult = new EvaResult(re);
        }
        else
        {
            printf("Unsupported filter op %s!",node->getFuncName().c_str());
            return NULL;
        }

        for(auto re : results)
        {
            re->freeResult();
            delete(re);
        }
        return outputResult;
    }

    void* VisitDoubleLiteral(DoubleLiteral* node,void* context) {

        auto re = gpuFunctions.cudfMakeColumnFromDoubleScalar(node->getValue(), page->getElementsCount());

        return new EvaResult(re);
    }
    void* VisitInt32Literal(Int32Literal* node,void* context) {


        auto re = gpuFunctions.cudfMakeColumnFromInt32Scalar(node->getValue(), page->getElementsCount());;

        return new EvaResult(re);
    }
    void* VisitInt64Literal(Int64Literal* node,void* context) {


        auto re = gpuFunctions.cudfMakeColumnFromInt64Scalar(node->getValue(), page->getElementsCount());;

        return new EvaResult(re);
    }
    void* VisitStringLiteral(StringLiteral* node,void* context){

        auto re = gpuFunctions.cudfMakeColumnFromStringScalar(node->getValue(), page->getElementsCount());;

        return new EvaResult(re);
    }



    void* VisitDate32Literal(Date32Literal* node,void* context){


        auto re = gpuFunctions.cudfMakeColumnFromDate32Scalar(node->getValue(), page->getElementsCount());;

        return new EvaResult(re);
    }
    void* VisitIdentifier(Identifier* node,void* context){

        return NULL;


    }
    void *VisitDayTimeIntervalLiteral(DayTimeIntervalLiteral* node,void* context)
    {

        auto re = gpuFunctions.cudfMakeColumnFromInt32Scalar(node->getValue(), page->getElementsCount());;

        return new EvaResult(re);
    }

    void* VisitColumn(Column* node,void* context){

        int columnIndex = -1;
        auto allFields = this->input_schemaIn->field_names();

        for(int i = 0 ; i < allFields.size() ; i++)
            if(allFields[i] == node->getValue())
                columnIndex = i;



        auto re = gpuFunctions.cudfGetColumnByIndex(page->getExtensionPage(),columnIndex);

        return new EvaResult(re);

    }

    void* VisitIfExpression(IfExpression *node,void* context)
    {
        vector<Node*> arguments = node->getChildren();
        for(int i = 0 ; i < arguments.size() ; i++)
        {
            Visit(arguments[i],NULL);
        }

        return NULL;
    }

    void* VisitInExpression(InExpression *node,void* context)
    {
        vector<Node*> arguments = node->getChildren();
        for(int i = 0 ; i < arguments.size() ; i++)
        {
            Visit(arguments[i],NULL);
        }

        return NULL;
    }



    void* filter(void* finalMask, int &elementCount)
    {
        auto filtered = gpuFunctions.cudfApplyBooleanMask(page->getExtensionPage(),finalMask,elementCount);
        return filtered;
    }

};


#endif //OLVP_GPUFILTEREXPREVALUATOR_HPP
