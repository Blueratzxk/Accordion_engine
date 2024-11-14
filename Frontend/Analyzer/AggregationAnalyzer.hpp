//
// Created by zxk on 11/4/24.
//

#ifndef FRONTEND_AGGREGATIONANALYZER_HPP
#define FRONTEND_AGGREGATIONANALYZER_HPP

#include "../AstNodes/DefaultAstExpressionVisitor.hpp"
#include "FieldId.hpp"
#include "spdlog/spdlog.h"


class AggregationAnalyzer : public enable_shared_from_this<AggregationAnalyzer>
{

set<long> groupingFields;
map<Expression*, shared_ptr<FieldId>> columnReferences;
shared_ptr<FunctionAndTypeResolver> functionAndTypeResolver;
shared_ptr<Analysis> analysis;
shared_ptr<ExceptionCollector> exceptionCollector;

    class Visitor : public DefaultAstExpressionVisitor
    {
        shared_ptr<AggregationAnalyzer> aggregationAnalyzer;
    public:
        Visitor(shared_ptr<AggregationAnalyzer> aggregationAnalyzer){
            this->aggregationAnalyzer = aggregationAnalyzer;
        }

        class ProcessResult
        {
            bool result;
        public:
            ProcessResult(bool result)
            {
                this->result = result;
            }
            bool get(){return result;}
        };

        void* VisitExpression(Expression *node, void *context) override
        {
            return NULL;
        }

        void * VisitCast(Cast *node, void *context) override
        {
            return Visit(node->getExpression(),context);
        }

        void * VisitArithmeticBinaryExpression(ArithmeticBinaryExpression *node, void *context) override
        {

            auto left = ((ProcessResult*)Visit(node->getLeft(), context));
            auto right = ((ProcessResult*)Visit(node->getRight(), context));

            auto leftResult = left->get();
            auto rightResult = right->get();

            delete left;
            delete right;
            return new ProcessResult(leftResult && rightResult);
        }

        void * VisitComparisionExpression(ComparisionExpression *node, void *context) override
        {
            auto left = ((ProcessResult*)Visit(node->getLeft(), context));
            auto right = ((ProcessResult*)Visit(node->getRight(), context));

            auto leftResult = left->get();
            auto rightResult = right->get();

            delete left;
            delete right;
            return new ProcessResult(leftResult && rightResult);
        }

        void * VisitLogicalBinaryExpression(LogicalBinaryExpression *node, void *context) override
        {
            auto left = ((ProcessResult*)Visit(node->getLeft(), context));
            auto right = ((ProcessResult*)Visit(node->getRight(), context));

            auto leftResult = left->get();
            auto rightResult = right->get();

            delete left;
            delete right;
            return new ProcessResult(leftResult && rightResult);
        }

        void * VisitLiteral(Literal *node, void *context) override
        {
            return new ProcessResult(true);
        }

        void * VisitNotExpression(NotExpression *node, void *context) override{


            auto re = ((ProcessResult*)Visit(node->getExpression(), context));
            auto result = re->get();

            delete re;
            return new ProcessResult(result);
        }

        void * VisitFunctionCall(FunctionCall *node, void *context) override
        {

            auto functionHandle = this->aggregationAnalyzer->analysis->getFunctionHandle(node);

            if(functionHandle == NULL){
                return new ProcessResult(true);
            }

            if(this->aggregationAnalyzer->functionAndTypeResolver->getFunctionMetadata(functionHandle)->getFunctionKind() == "AGGREGATE")
                return new ProcessResult(true);


            for(auto argument : node->getChildren())
            {
                auto re = ((ProcessResult*)Visit(argument,context));
                auto result = re->get();
                delete re;

                if(!result)
                    return new ProcessResult(false);
            }

            return new ProcessResult(true);
        }

        void * VisitIdentifier(Identifier *node, void *context) override{

            return new ProcessResult(isGroupingKey(node));
        }

        void * VisitFieldReference(FieldReference *node, void *context) override
        {
            auto fieldId = checkAndGetColumnReferenceField(node, aggregationAnalyzer->columnReferences);
            bool inGroup = aggregationAnalyzer->groupingFields.contains(fieldId->hashCode());
            return new ProcessResult(inGroup);
        }

        bool isGroupingKey(Expression *node)
        {
            shared_ptr<FieldId> fieldId = checkAndGetColumnReferenceField(node, aggregationAnalyzer->columnReferences);

            auto result = this->aggregationAnalyzer->groupingFields.contains(fieldId->hashCode());
            return result;
        }

        shared_ptr<FieldId> checkAndGetColumnReferenceField(Expression *expression, map<Expression*, shared_ptr<FieldId>> columnReferences)
        {
            if(!columnReferences.contains(expression))
            this->aggregationAnalyzer->exceptionCollector->recordError("Missing field reference for expression");

            return columnReferences[expression];
        }

    };



public:
    AggregationAnalyzer(list<Expression*> groupByExpressions,
    shared_ptr<FunctionAndTypeResolver> functionAndTypeResolver,shared_ptr<Analysis> analysis,
                        shared_ptr<ExceptionCollector> exceptionCollector)
    {

        this->functionAndTypeResolver = functionAndTypeResolver;
        this->analysis = analysis;
        this->exceptionCollector = exceptionCollector;
        this->columnReferences = this->analysis->getColumnReferences();

        for(auto groupByExpression : groupByExpressions)
        {
            shared_ptr<FieldId> fieldId = this->columnReferences[groupByExpression];
            this->groupingFields.insert(fieldId->hashCode());
        }
    }

    void analyze(Expression *expression) {
        shared_ptr<Visitor> visitor = make_shared<Visitor>(shared_from_this());

        auto re = (Visitor::ProcessResult*)visitor->Visit(expression,NULL);
        bool result = re->get();

        delete re;
        if (!result) {
            this->exceptionCollector->recordError("Expression must be an aggregate expression or appear in GROUP BY clause");
        }
    }

};



#endif //FRONTEND_AGGREGATIONANALYZER_HPP
