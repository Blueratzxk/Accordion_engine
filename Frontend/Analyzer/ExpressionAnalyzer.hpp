//
// Created by zxk on 10/3/24.
//

#ifndef FRONTEND_EXPRESSIONANALYZER_HPP
#define FRONTEND_EXPRESSIONANALYZER_HPP



#include "../AstNodes/DefaultAstNodeVisitor.hpp"

#include "Scope.hpp"
#include "Analysis.hpp"

#include "FunctionCallMeta.hpp"
#include "spdlog/spdlog.h"
#include "ExpressionAnalysis.hpp"
#include "FunctionMetaData.hpp"
#include "FunctionAndTypeResolver.hpp"
#include "ExceptionCollector.hpp"
#include "../Planner/Expression/TypeProvider.hpp"
#include "TypeAllocator.hpp"
#include "FieldId.hpp"
class ExpressionAnalyzer: public enable_shared_from_this<ExpressionAnalyzer> {

    shared_ptr<Analysis> analysis;
    shared_ptr<FunctionAndTypeResolver> functionAndTypeResolver;
    shared_ptr<ExceptionCollector> exceptionCollector;
    shared_ptr<AstNodeAllocator> astAllocator;
    shared_ptr<TypeAllocator> typeAllocator;

    class Visitor : public DefaultAstNodeVisitor {

        list<Node *> stack;
        shared_ptr<ExpressionAnalyzer> expressionAnalyzer = NULL;
        Scope *scope;

        map<Expression *, Type *> Types;
        map<Expression *, Type *> Coercions;
        set<Expression*> operators;
        map<Expression *,shared_ptr<FieldId>> columnReferences;

        map<Expression *, string> functionCalls;
        map<string, shared_ptr<FunctionHandle>> functionHandles;

        shared_ptr<FunctionAndTypeResolver> functionAndTypeResolver;

        shared_ptr<TypeProvider> typeProvider = NULL;
        shared_ptr<TypeAllocator> typeAllocator;


    public:

        Visitor(Scope *scope, shared_ptr<ExpressionAnalyzer> expressionAnalyzer,shared_ptr<FunctionAndTypeResolver> functionAndTypeResolver,shared_ptr<TypeAllocator> typeAllocator) {
            this->scope = scope;
            this->expressionAnalyzer = expressionAnalyzer;
            this->functionAndTypeResolver = functionAndTypeResolver;
            this->typeAllocator = typeAllocator;
        }

        Visitor(Scope *scope, shared_ptr<TypeProvider> typeProvider, shared_ptr<ExpressionAnalyzer> expressionAnalyzer,shared_ptr<FunctionAndTypeResolver> functionAndTypeResolver,shared_ptr<TypeAllocator> typeAllocator) {
            this->scope = scope;
            this->expressionAnalyzer = expressionAnalyzer;
            this->functionAndTypeResolver = functionAndTypeResolver;
            this->typeProvider = typeProvider;
            this->typeAllocator = typeAllocator;
        }

        Node *getPreviousNode() {
            return stack.back();
        }

        void *Visit(Node *node, void *context) override {

            stack.push_back(node);
            auto result = node->accept(this, context);
            stack.pop_back();
            return result;
        }

        void *VisitIdentifier(Identifier *node, void *context) override {

            auto scope = this->scope;

            auto re = scope->tryResolveField(node);

            if (re == NULL) {

                this->expressionAnalyzer->exceptionCollector->recordError("ExpressionAnalyzer: Cannot find the expression identifier " + node->getValue() + "!");
                return setType(node,NULL);
            }
            else
            {
              //  return setType(node,typeAllocator->new_Type(re->getType()));

                return handleResolvedField(node,re);
            }
        }


        shared_ptr<FieldId> makeFieldIdFrom(shared_ptr<Scope::ResolvedField> field)
        {
            Scope *sourceScope = field->getScope();
            shared_ptr<RelationType> relationType = sourceScope->getRelationType();
            return make_shared<FieldId>(sourceScope->getRelationId(), relationType->indexOf(field->getField()));
        }

     Type* handleResolvedField(Expression *node, shared_ptr<Scope::ResolvedField> resolvedField)
        {
            return handleResolvedField(node, makeFieldIdFrom(resolvedField), resolvedField->getField());
        }

     Type* handleResolvedField(Expression *node, shared_ptr<FieldId> fieldId, shared_ptr<Field> field)
        {

            shared_ptr<FieldId> previous = columnReferences[node] = fieldId;
            return setType(node, this->typeAllocator->new_Type(field->getType()));
        }

        void *VisitFunctionCall(FunctionCall *node, void *context) override {

            vector<Type *> types;
            for (auto paras: node->getChildren()) {
                Type *result = (Type *) Visit(paras, context);
                if(result == NULL)
                    return NULL;
                types.push_back(result);
            }
            string argumentsString;
            for(int i = 0 ;i < types.size() ; i++)
                argumentsString+=(types[i]->getType()+",");
            if(!argumentsString.empty())
                argumentsString.pop_back();



            auto func = this->functionAndTypeResolver->resolveFunction(node->getFuncName(),types);

            if(func == NULL) {
                return setType(node, NULL);
                this->expressionAnalyzer->exceptionCollector->recordError("Cannot find the function "+node->getFuncName()+argumentsString);
            }
            auto type = this->functionAndTypeResolver->getFunctionMetadata(func);

            if(type == NULL) {
                this->expressionAnalyzer->exceptionCollector->recordError("Cannot find the function "+node->getFuncName()+argumentsString);
                return setType(node, NULL);
            }
            this->functionCalls[node] = type->getFunctionKind();
            this->functionHandles[type->getName()] = func;

            return setType(node,typeAllocator->new_Type(type->getReturnType()->getBaseType()));
        }

        void *VisitInt32Literal(Int32Literal *node, void *context) override {
            return setType(node,typeAllocator->new_Type("bigint"));
        }

        void *VisitInt64Literal(Int64Literal *node, void *context) override {
            return setType(node,typeAllocator->new_Type("bigint"));
        }

        void *VisitDayTimeIntervalLiteral(DayTimeIntervalLiteral *node, void *context) override {
            return setType(node,typeAllocator->new_Type("bigint"));
        }

        void *VisitStringLiteral(StringLiteral *node, void *context) override {
            return setType(node,typeAllocator->new_Type("string"));
        }

        void *VisitDoubleLiteral(DoubleLiteral *node, void *context) override {
            return setType(node,typeAllocator->new_Type("double"));
        }


        Type *setType(Expression *expression, Type *type) {
           this->Types[expression] = type;
            return type;
        }

        void * VisitComparisionExpression(ComparisionExpression *node, void *context) override
        {
            auto returnType = getOperator(context, node, node->getOPName(node->getOperator()),{node->getLeft(),node->getRight()});
            this->operators.insert(node);
            return returnType;
        }

        void * VisitCast(Cast *node, void *context) override
        {
            auto returnType = getOperator(context, node, "CAST",{node->getExpression()});
            this->operators.insert(node);
            return returnType;
        }
        void * VisitArithmeticBinaryExpression(ArithmeticBinaryExpression *node, void *context) override
        {
            auto returnType = getOperator(context, node, node->getOPName(node->getOperator()),{node->getLeft(),node->getRight()});
            this->operators.insert(node);
            return returnType;
        }

        void * VisitLogicalBinaryExpression(LogicalBinaryExpression *node, void *context) override
        {
            auto returnType = getOperator(context, node, node->getOp(),{node->getLeft(),node->getRight()});
            this->operators.insert(node);
            return returnType;
        }

        void * VisitSymbolReference(SymbolReference *node, void *context) override
        {
            shared_ptr<Type> type = typeProvider->get(node);
            return setType(node, type.get());
        }

        shared_ptr<ExpressionAnalysis> getExpressionAnalysis() {

            //   for(auto et : this->Types)
            //   {
            //       this->expressionAnalyzer->analysis->setType(et.first,et.second);
            //   }


            return make_shared<ExpressionAnalysis>(this->Types, this->functionCalls, this->functionHandles,this->Coercions,
                                                   this->typeAllocator,this->operators,this->columnReferences);
        }


        void* getOperator(void* context, Expression *node, string operatorType, vector<Expression*> arguments)
        {
            vector<Type*> argumentTypes;
            string argumentString;

            for (Expression *expression : arguments) {
                auto argumentType = (Type *)Visit(expression, context);
                if(argumentType == NULL)
                    return NULL;


                argumentTypes.push_back(argumentType);

                argumentString+= (argumentType->getType()+",");
            }
            argumentString.pop_back();

            shared_ptr<FunctionMetadata> operatorMetadata = NULL;
            auto matchedHandle = functionAndTypeResolver->resolveOperator(operatorType, argumentTypes);
            if(matchedHandle == NULL) {
                this->expressionAnalyzer->exceptionCollector->recordError("Cannot find the operator "+operatorType+"("+argumentString+")");

                return setType(node, NULL);
            }

            operatorMetadata = this->functionAndTypeResolver->getFunctionMetadata(matchedHandle);
            if(operatorMetadata == NULL) {
                this->expressionAnalyzer->exceptionCollector->recordError("Cannot find the operatorMetadata "+operatorType+"("+argumentString+")");
                return setType(node, NULL);
            }


            Type *type = this->typeAllocator->new_Type(operatorMetadata->getReturnType()->getBaseType());
            this->functionCalls[node] = operatorMetadata->getFunctionKind();

            //record arguments coercions
            auto operatorArgumentTypes = operatorMetadata->getArgumentTypes();
            for(int i = 0 ; i < operatorMetadata->getArgumentTypes().size() ; i++)
            {
                auto argumentType = argumentTypes[i]->getType();
                auto operatorArgumentType = operatorArgumentTypes[i]->getBaseType();
                if(argumentType != operatorArgumentType)
                {
                    this->Coercions[arguments[i]] = this->typeAllocator->new_Type(operatorMetadata->getArgumentTypes()[i]->getBaseType());
                }
            }


            return setType(node, type);
        }

    };


public:
    ExpressionAnalyzer(shared_ptr<Analysis> analysis, shared_ptr<FunctionAndTypeResolver> functionAndTypeResolver, shared_ptr<ExceptionCollector> exceptionCollector,shared_ptr<AstNodeAllocator> astNodeAllocator) {

        this->analysis = analysis;
        this->functionAndTypeResolver = functionAndTypeResolver;
        this->exceptionCollector = exceptionCollector;
        this->astAllocator = astNodeAllocator;
        this->typeAllocator = make_shared<TypeAllocator>();
        this->analysis->addTypeAllocator(this->typeAllocator);
    }

    shared_ptr<ExpressionAnalysis>  analyzeExpression(shared_ptr<TypeProvider> typeProvider, Expression *expression)
    {
        Scope *outScope = new Scope("VoidScope",NULL,true);

        shared_ptr<Visitor> exAnalyzer = make_shared<Visitor>(outScope, typeProvider,shared_from_this(), functionAndTypeResolver,typeAllocator);
        exAnalyzer->Visit(expression, NULL);
        auto expressionAnalysisResult = exAnalyzer->getExpressionAnalysis();

        delete outScope;

        return expressionAnalysisResult;
    }


    shared_ptr<ExpressionAnalysis>  analyzeExpression(Scope *scope, Expression *expression)
    {
        shared_ptr<Visitor> exAnalyzer = make_shared<Visitor>(scope, shared_from_this(), functionAndTypeResolver,typeAllocator);

        exAnalyzer->Visit(expression, NULL);
        auto expressionAnalysisResult = exAnalyzer->getExpressionAnalysis();
        return expressionAnalysisResult;
    }



};






#endif //FRONTEND_EXPRESSIONANALYZER_HPP
