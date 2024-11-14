//
// Created by zxk on 10/3/24.
//

#ifndef FRONTEND_TRANSLATIONMAP_HPP
#define FRONTEND_TRANSLATIONMAP_HPP

#include "RelationPlan.hpp"
#include "../Analyzer/Analysis.hpp"
#include "../AstNodes/Expression/Expression.h"
#include "../AstCleaner.hpp"
#include "Expression/ExpressionTreeRewriter.hpp"
class TranslationMap {
    shared_ptr<RelationPlan> rewriteBase;
    shared_ptr<Analysis> analysis;
    shared_ptr<AstNodeCleaner> cleaner;
    vector<shared_ptr<VariableReferenceExpression>> fieldVariables;
    map<Expression*, shared_ptr<VariableReferenceExpression>> expressionToVariables;
    map<Expression*, Expression*> expressionToExpressions;
    shared_ptr<AstNodeAllocator> astNodeAllocator;

public:
    TranslationMap(shared_ptr<RelationPlan> rewriteBase, shared_ptr<Analysis> analysis,shared_ptr<AstNodeAllocator> astNodeAllocator,
                   vector<shared_ptr<VariableReferenceExpression>> fieldVariables) {

        this->rewriteBase = rewriteBase;
        this->analysis = analysis;
        this->fieldVariables = fieldVariables;
        this->cleaner = make_shared<AstNodeCleaner>();
        this->astNodeAllocator = astNodeAllocator;
    }

    shared_ptr<RelationPlan> getRelationPlan() {
        return rewriteBase;
    }

    shared_ptr<AstNodeAllocator> getAstNodeAllocator()
    {
        return this->astNodeAllocator;
    }
    shared_ptr<Analysis> getAnalysis() {
        return analysis;
    }


    void setFieldMappings(vector<shared_ptr<VariableReferenceExpression>> variables) {

        if(this->fieldVariables.size() == 0)
            this->fieldVariables = vector<shared_ptr<VariableReferenceExpression>>(variables.size());

        for (int i = 0; i < variables.size(); i++) {
            this->fieldVariables[i] = variables[i];
        }
    }

    void copyMappingsFrom(shared_ptr<TranslationMap> other) {

        for (auto item: other->expressionToVariables)
            expressionToVariables[item.first] = item.second;

        for (auto item: other->expressionToExpressions)
            expressionToExpressions[item.first] = item.second;

        if(other->fieldVariables.size() > 0 && this->fieldVariables.size() == 0)
            this->fieldVariables = vector<shared_ptr<VariableReferenceExpression>>(other->fieldVariables.size());
        for (int i = 0; i < other->fieldVariables.size(); i++) {
            this->fieldVariables[i] = other->fieldVariables[i];
        }

    }

    void putExpressionMappingsFrom(shared_ptr<TranslationMap> other) {
        for (auto item: other->expressionToVariables)
            expressionToVariables[item.first] = item.second;

        for (auto item: other->expressionToExpressions)
            expressionToExpressions[item.first] = item.second;
    }

    void put(Expression *expression, shared_ptr<VariableReferenceExpression> variable) {
        if (expression->getExpressionId() == "FieldReference") {
            int fieldIndex = ((FieldReference *) (expression))->getFieldIndex();
            fieldVariables[fieldIndex] = variable;

            auto symbol = this->astNodeAllocator->new_SymbolReference(expression->getLocation(),
                                              rewriteBase->getVariable(fieldIndex)->getName());
            expressionToVariables[symbol] = variable;
            return;
        }

        Expression *translated = translateNamesToSymbols(expression);
        expressionToVariables[translated] = variable;

        //rewriteBase->getScope()->tryResolveField(expression)
        //       .filter(ResolvedField::isLocal)
        //     .ifPresent(field -> fieldVariables[field.getHierarchyFieldIndex()] = variable);
    }


    class translateRewriter : public ExpressionTreeRewriter::ExpressionRewriter {


        TranslationMap *translationMap;
    public:
        translateRewriter(void *user) : ExpressionTreeRewriter::ExpressionRewriter(user)
        {
            translationMap = ((TranslationMap *)this->user);
        }

        Expression * rewriteExpression(Expression *node, void *context, shared_ptr<ExpressionTreeRewriter> treeRewriter) override
        {
            if(translationMap->containsExpression(node) && ! translationMap->isConstant(node))
            {
                return translationMap->astNodeAllocator->new_SymbolReference(node->getLocation(),(translationMap->getVariableByExpression(node))->getName());
            }

            Expression *rewrittenExpression = static_cast<Expression *>(treeRewriter->defaultRewrite(node, context));
            return rewrittenExpression;
        }
    };


    Expression *rewrite(Expression *expression) {
        // first, translate names from sql-land references to plan symbols
        Expression *mapped = translateNamesToSymbols(expression);

        auto rewriter = make_shared<translateRewriter>(this);
        auto treeRewriter = make_shared<ExpressionTreeRewriter>(astNodeAllocator);
        auto result = static_cast<Expression *>(treeRewriter->rewriteWith(rewriter, mapped, NULL));


        return result;

    }


    class translateNameToSymbolRewriter : public ExpressionTreeRewriter::ExpressionRewriter {

        TranslationMap *translationMap;
    public:
        translateNameToSymbolRewriter(void *user) : ExpressionTreeRewriter::ExpressionRewriter(user)
        {
            translationMap = ((TranslationMap *)this->user);
        }

        Expression * rewriteExpression(Expression *node, void *context, shared_ptr<ExpressionTreeRewriter> treeRewriter) override
        {
            Expression *rewrittenExpression = static_cast<Expression *>(treeRewriter->defaultRewrite(node, context));

            return coerceIfNecessary(node,rewrittenExpression);
        }

        Expression * rewriteIdentifier(Identifier *node, void *context, shared_ptr<ExpressionTreeRewriter> treeRewriter) override
        {
            auto relationPlan = ((TranslationMap *)this->user)->getRelationPlan();
            auto re = ((TranslationMap *)this->user)->getVariable(relationPlan,node);

            auto rewritten = translationMap->astNodeAllocator->new_SymbolReference(re->getSourceLocation(),re->getName());

            auto result = coerceIfNecessary(node,rewritten);
            return result;
        }

        Expression * rewriteFieldReference(FieldReference *node, void *context, shared_ptr<ExpressionTreeRewriter> treeRewriter) override
        {
            auto re = ((TranslationMap *)this->user)->getRelationPlan()->getVariable(node->getFieldIndex());
            auto rewritten = translationMap->astNodeAllocator->new_SymbolReference(re->getSourceLocation(),re->getName());

            return coerceIfNecessary(node,rewritten);

        }

        Expression* coerceIfNecessary(Expression *original, Expression *rewritten) {
            Type *coercion = translationMap->analysis->getCoercion(original);
            Expression *result = rewritten;
            if (coercion != NULL) {
                result = translationMap->astNodeAllocator->new_Cast(
                        original->getLocation(),
                        rewritten,
                        coercion->getType());
            }
            return result;
        }
    };


    Expression *translateNamesToSymbols(Expression *expression) {
        auto rewriter = make_shared<translateNameToSymbolRewriter>(this);
        auto treeRewriter = make_shared<ExpressionTreeRewriter>(astNodeAllocator);


        return static_cast<Expression *>(treeRewriter->rewriteWith(rewriter, expression, NULL));
    }


    bool containsSymbol(Expression *expression) {
        if (expression->getExpressionId() == "FieldReference") {
            int field = ((FieldReference *) expression)->getFieldIndex();
            return fieldVariables[field] != NULL;
        }

        Expression *translated = translateNamesToSymbols(expression);
        auto result = containsExpression(translated);


        return result;
    }

    shared_ptr<VariableReferenceExpression> get(Expression *expression) {
        if (expression->getExpressionId() == "FieldReference") {
            int field = ((FieldReference *) expression)->getFieldIndex();
            return fieldVariables[field];
        }

        Expression *translated = translateNamesToSymbols(expression);
        if (!containsExpression(translated)) {
            return get(expressionToExpressions[translated]);
        }

        auto var = getVariableByExpression(translated);


        return var;
    }

    shared_ptr<VariableReferenceExpression> getVariableByExpression(Expression *translated)
    {
        for(auto item : expressionToVariables)
        {
            if(translated->equals(item.first))
                return item.second;
        }
        spdlog::error("Cannot get variable? The execution shouldn't be here.");
        return NULL;
    }

    bool containsExpression(Expression *expression)
    {
        string expressionType = expression->getExpressionId();
        for(auto item : expressionToVariables)
        {
           if(expression->equals(item.first))
               return true;
        }
        return false;
    }

    void put(Expression *expression, Expression *rewritten) {
        expressionToExpressions[translateNamesToSymbols(expression)] = rewritten;
    }



    shared_ptr<VariableReferenceExpression> getVariable(shared_ptr<RelationPlan> plan, Expression *expression)
    {
        auto re = plan->getScope()->tryResolveField(expression);

        if(re != NULL)
            return plan->getFieldMappings()[re->getHierarchyFieldIndex()];

        return make_shared<VariableReferenceExpression>("0","NULL","NULL");
    }

    bool isConstant(Expression *expression)
    {
        Expression *tempExpression = expression;
        while (tempExpression->getExpressionId() == "Cast") {
            tempExpression = ((Cast*) tempExpression)->getExpression();
        }

        if (tempExpression->getExpressionId()  == "Literal") {
            return true;
        }
        // Everything else is considered non-const
        return false;
    }

    ~TranslationMap() {


    }


};



#endif //FRONTEND_TRANSLATIONMAP_HPP
