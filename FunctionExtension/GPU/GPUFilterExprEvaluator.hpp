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
    bool releaseAble = true;
    GPUFunctions gpuFunctions;

public:

    EvaResult(void* ptr,bool releaseAble)
    {
        this->ptr = ptr;
        this->releaseAble = releaseAble;
    }
    EvaResult(void* ptr)
    {
        this->ptr = ptr;
    }

    void freeResult()
    {
        if(this->ptr != NULL && this->canRelease()) {
            gpuFunctions.freeGPUColumn(this->ptr);
        }
    }

    bool canRelease()
    {
        return this->releaseAble;
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
        spdlog::info("FunctionCall"+node->getFuncName());
        if(node->getFuncName() == "subtract")
        {
            auto re = gpuFunctions.cudfFunctionCall(node->getFuncName(),{results[0]->getColumn(),results[1]->getColumn()},node->getOutputType());
            cout << "sub res:" << re << endl;
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
        else if(node->getFuncName() == "castDATE")
        {
            auto re = gpuFunctions.cudfFunctionCall(node->getFuncName(),{results[0]->getColumn()},node->getOutputType());
            outputResult = new EvaResult(re);
        }
        else
        {
            spdlog::info("Unsupported filter op " + node->getFuncName());
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

        spdlog::info("DoubleLiteral");
        auto re = gpuFunctions.cudfMakeColumnFromDoubleScalar(node->getValue(), page->getElementsCount());

        return new EvaResult(re);
    }
    void* VisitInt32Literal(Int32Literal* node,void* context) {

        spdlog::info("Int32Literal");
        auto re = gpuFunctions.cudfMakeColumnFromInt32Scalar(node->getValue(), page->getElementsCount());;

        return new EvaResult(re);
    }
    void* VisitInt64Literal(Int64Literal* node,void* context) {

        spdlog::info("Int64Literal");
        auto re = gpuFunctions.cudfMakeColumnFromInt64Scalar(node->getValue(), page->getElementsCount());;

        return new EvaResult(re);
    }
    void* VisitStringLiteral(StringLiteral* node,void* context){

        spdlog::info("StringLiteral");
        auto re = gpuFunctions.cudfMakeColumnFromStringScalar(node->getValue(), page->getElementsCount());;

        return new EvaResult(re);
    }



    void* VisitDate32Literal(Date32Literal* node,void* context){


        spdlog::info("Date32Literal");
        int32_t date;
        TimeCommon::getDate32(node->getValue(),&date);

        auto re = gpuFunctions.cudfMakeColumnFromInt32Scalar(date, page->getElementsCount());;
        cout <<  page->getElementsCount() << endl;
        cout << re << endl;

        return new EvaResult(re);
    }
    void* VisitIdentifier(Identifier* node,void* context){

        return NULL;


    }
    void *VisitDayTimeIntervalLiteral(DayTimeIntervalLiteral* node,void* context)
    {
        spdlog::info("DayTimeIntervalLiteral");
        cout <<  page->getElementsCount() << endl;
        auto re = gpuFunctions.cudfMakeColumnFromInt32Scalar(node->getValue(), page->getElementsCount());;
        cout << re << endl;

        return new EvaResult(re);
    }

    void* VisitColumn(Column* node,void* context){

        spdlog::info("Column "+node->getValue());
        int columnIndex = -1;
        auto allFields = this->input_schemaIn->field_names();

        for(int i = 0 ; i < allFields.size() ; i++)
            if(allFields[i] == node->getValue())
                columnIndex = i;



        auto re = gpuFunctions.cudfGetColumnByIndex(page->getExtensionPage(),columnIndex);

        return new EvaResult(re,false);

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
