//
// Created by zxk on 10/30/24.
//

#ifndef FRONTEND_SQLTOROWEXPRESSIONTRANSLATOR_HPP
#define FRONTEND_SQLTOROWEXPRESSIONTRANSLATOR_HPP


#include "spdlog/spdlog.h"
#include "../../Analyzer/FunctionAndTypeResolver.hpp"
#include "../../AstNodes/Expression/Expression.h"
#include "../../Planner/Expression/RowExpressionNodeAllocator.hpp"
class SqlToRowExpressionTranslator
{
    shared_ptr<FunctionAndTypeResolver> functionAndTypeResolver;
    shared_ptr<FunctionMetadataManager> functionMetadataManager;
    map<Expression *, Type*> Types;
    shared_ptr<RowExpressionNodeAllocator> rowExpressionNodeAllocator;

public:

    SqlToRowExpressionTranslator()
    {
        this->rowExpressionNodeAllocator = make_shared<RowExpressionNodeAllocator>();
    }

    void releaseRowExpressionNodes()
    {
        this->rowExpressionNodeAllocator->release_all();
    }

    shared_ptr<RowExpression> translate(Expression *expression, shared_ptr<FunctionAndTypeResolver> functionAndTypeResolver,
    map<Expression *, Type*> types)
    {

        auto result = (RowExpression*)(make_shared<Visitor>(functionAndTypeResolver,types,rowExpressionNodeAllocator)->Visit(expression,NULL));
        return shared_ptr<RowExpression>(result);
    }

    class Visitor : public DefaultAstExpressionVisitor{

        shared_ptr<FunctionAndTypeResolver> functionAndTypeResolver;
        shared_ptr<FunctionMetadataManager> functionMetadataManager;
        map<Expression *, Type*> types;
        shared_ptr<RowExpressionNodeAllocator> nodeAllocator;
    public:
        Visitor(shared_ptr<FunctionAndTypeResolver> functionAndTypeResolver,
                map<Expression *, Type*> types,shared_ptr<RowExpressionNodeAllocator> rowExpressionNodeAllocator)
        {

            this->functionAndTypeResolver = functionAndTypeResolver;
            this->functionMetadataManager = functionAndTypeResolver->getFunctionMetadataManager();
            this->types = types;
            this->nodeAllocator = rowExpressionNodeAllocator;
        }

        void *VisitNotExpression(NotExpression *node, void *context) override {
            RowExpression *expression = (RowExpression *) Visit(node->getExpression(), context);
            vector<Type *> types = {expression->getType().get()};
            list<shared_ptr<RowExpression>> arguments;
            arguments.push_back(shared_ptr<RowExpression>(expression));

            auto functionHandle = functionAndTypeResolver->resolveOperator(node->getOp(), types);
            auto fmeta = this->functionMetadataManager->getFunctionMetadata(functionHandle);

            return nodeAllocator->new_CallExpression(node->getLocation(), node->getOp(), functionHandle,
                                      make_shared<Type>(fmeta->getReturnType()->getBaseType()), arguments);
        }

        void *VisitFieldReference(FieldReference *node, void *context) override {
            return nodeAllocator->new_InputReferenceExpression(node->getLocation(), node->getFieldIndex(),
                                                this->types[node]->getType());
        }

        void *VisitSymbolReference(SymbolReference *node, void *context) override {
            return nodeAllocator->new_VariableReferenceExpression(node->getLocation(), node->getName(), this->types[node]->getType());
        }

        void *VisitCast(Cast *node, void *context) override {

            auto value = shared_ptr<RowExpression>((RowExpression *) Visit(node->getExpression(), context));

            list<shared_ptr<RowExpression>> arguments;
            arguments.push_back(value);

            auto functionHandle = this->functionAndTypeResolver->resolveOperator("CAST", {value->getType().get()});
            auto fmeta = this->functionMetadataManager->getFunctionMetadata(functionHandle);

            if(functionHandle == NULL || fmeta == NULL) {
                return NULL;
            }

            return nodeAllocator->new_CallExpression(node->getLocation(), "CAST", functionHandle,
                                      make_shared<Type>(fmeta->getReturnType()->getBaseType()), arguments);

        }

        void *VisitArithmeticBinaryExpression(ArithmeticBinaryExpression *node, void *context) override {
            list<shared_ptr<RowExpression>> arguments;

            for (auto argument: node->getChildren()) {
                arguments.push_back(shared_ptr<RowExpression>((RowExpression *) Visit(argument, context)));
            }
            vector<Type *> types;
            for (auto argument: arguments) {
                types.push_back(argument->getType().get());
            }

            auto functionHandle = this->functionAndTypeResolver->resolveOperator(node->getOPName(node->getOperator()),
                                                                                 types);
            auto fmeta = this->functionMetadataManager->getFunctionMetadata(functionHandle);

            if(functionHandle == NULL || fmeta == NULL) {
                return NULL;
            }

            return nodeAllocator->new_CallExpression(node->getLocation(), node->getOPName(node->getOperator()), functionHandle,
                                      make_shared<Type>(fmeta->getReturnType()->getBaseType()), arguments);

        }

        void *VisitLogicalBinaryExpression(LogicalBinaryExpression *node, void *context) override {
            list<shared_ptr<RowExpression>> arguments;

            for (auto argument: node->getChildren()) {
                arguments.push_back(shared_ptr<RowExpression>((RowExpression *) Visit(argument, context)));
            }
            vector<Type *> types;
            for (auto argument: arguments) {
                types.push_back(argument->getType().get());
            }

            auto functionHandle = this->functionAndTypeResolver->resolveOperator(node->getOp(), types);
            auto fmeta = this->functionMetadataManager->getFunctionMetadata(functionHandle);


            if(functionHandle == NULL || fmeta == NULL) {
                return NULL;
            }

            return nodeAllocator->new_CallExpression(node->getLocation(), node->getOp(), functionHandle,
                                      make_shared<Type>(fmeta->getReturnType()->getBaseType()), arguments);

        }

        void *VisitComparisionExpression(ComparisionExpression *node, void *context) override {
            list<shared_ptr<RowExpression>> arguments;

            for (auto argument: node->getChildren()) {
                arguments.push_back(shared_ptr<RowExpression>((RowExpression *) Visit(argument, context)));
            }
            vector<Type *> types;
            for (auto argument: arguments) {
                types.push_back(argument->getType().get());
            }

            auto functionHandle = this->functionAndTypeResolver->resolveOperator(node->getOPName(node->getOperator()),
                                                                                 types);
            auto fmeta = this->functionMetadataManager->getFunctionMetadata(functionHandle);

            if(functionHandle == NULL || fmeta == NULL) {
                return NULL;
            }

            return nodeAllocator->new_CallExpression(node->getLocation(), node->getOPName(node->getOperator()), functionHandle,
                                      make_shared<Type>(fmeta->getReturnType()->getBaseType()), arguments);

        }


        void *VisitFunctionCall(FunctionCall *node, void *context) override {
            list<shared_ptr<RowExpression>> arguments;

            for (auto argument: node->getChildren()) {
                arguments.push_back(shared_ptr<RowExpression>((RowExpression *) Visit(argument, context)));
            }
            vector<Type *> types;
            for (auto argument: arguments) {
                types.push_back(argument->getType().get());
            }

            auto functionHandle = this->functionAndTypeResolver->resolveFunction(node->getFuncName(), types);
            auto fmeta = this->functionMetadataManager->getFunctionMetadata(functionHandle);

            if(functionHandle == NULL || fmeta == NULL) {
                return NULL;
            }

            return nodeAllocator->new_CallExpression(node->getLocation(), node->getFuncName(), functionHandle,
                                      make_shared<Type>(fmeta->getReturnType()->getBaseType()), arguments);

        }

        void *VisitIdentifier(Identifier *node, void *context) override {
            return nodeAllocator->new_VariableReferenceExpression(node->getLocation(), node->getValue(), this->types[node]->getType());
        }

        void *VisitExpression(Expression *node, void *context) override {
            spdlog::error("SqlToRowExpressionTranslator VisitExpression : shouldn't be here!");
            return NULL;
        }

        void *VisitDoubleLiteral(DoubleLiteral *node, void *context) override {
            return nodeAllocator->new_ConstantExpression(node->getLocation(), to_string(node->getValue()),"double");
        }

        void *VisitStringLiteral(StringLiteral *node, void *context) override {
            return nodeAllocator->new_ConstantExpression(node->getLocation(), node->getValue(), "int64");
        }

        void *VisitDayTimeIntervalLiteral(DayTimeIntervalLiteral *node, void *context) override {
            return nodeAllocator->new_ConstantExpression(node->getLocation(), to_string(node->getValue()), "int64");
        }

        void *VisitInt64Literal(Int64Literal *node, void *context) override {
            return nodeAllocator->new_ConstantExpression(node->getLocation(), to_string(node->getValue()), "int64");
        }

        void *VisitInt32Literal(Int32Literal *node, void *context) override {
            return nodeAllocator->new_ConstantExpression(node->getLocation(), to_string(node->getValue()), "int32");
        }

        void *VisitDate32Literal(Date32Literal *node, void *context) override {
            return nodeAllocator->new_ConstantExpression(node->getLocation(), node->getValue(), "int64");
        }
    };


};





#endif //FRONTEND_SQLTOROWEXPRESSIONTRANSLATOR_HPP
