//
// Created by zxk on 10/22/24.
//

#ifndef FRONTEND_PLANBUILDER_HPP
#define FRONTEND_PLANBUILDER_HPP

#include "TranslationMap.hpp"

#include "Assignments.hpp"
#include <memory>


#include "Expression/TranslateExpressions.hpp"
#include "PlanNodeIdAllocator.hpp"
#include "../PlanNode/PlanNodeAllocator.hpp"
class PlanBuilder {
    PlanNode *root;

    shared_ptr<TranslationMap> translationMap;
public:
    PlanBuilder(shared_ptr<TranslationMap> translationMap, PlanNode *root) {
        this->root = root;
        this->translationMap = translationMap;

    }


    shared_ptr<TranslationMap> copyTranslations() {
        vector<shared_ptr<VariableReferenceExpression>> fieldvariables;
        shared_ptr<TranslationMap> translations = make_shared<TranslationMap>(getRelationPlan(), getAnalysis(),getAstNodeAllocator(),
                                                                              fieldvariables);
        translations->copyMappingsFrom(getTranslations());
        return translations;
    }

    shared_ptr<Analysis> getAnalysis() {
        return translationMap->getAnalysis();
    }
    shared_ptr<AstNodeAllocator> getAstNodeAllocator()
    {
        return translationMap->getAstNodeAllocator();
    }
    shared_ptr<PlanBuilder> withNewRoot(PlanNode *root) {

        return make_shared<PlanBuilder>(translationMap, root);
    }

    shared_ptr<RelationPlan> getRelationPlan() {
        return translationMap->getRelationPlan();
    }

    PlanNode *getRoot() {
        return root;
    }

    bool canTranslate(Expression *expression) {
        return translationMap->containsSymbol(expression);
    }

    shared_ptr<VariableReferenceExpression> translate(Expression *expression) {
        return translationMap->get(expression);
    }

    shared_ptr<VariableReferenceExpression> translateToVariable(Expression *expression) {
        return translationMap->get(expression);
    }

    Expression *rewrite(Expression *expression) {
        return translationMap->rewrite(expression);
    }

    shared_ptr<TranslationMap> getTranslations() {
        return translationMap;
    }


    shared_ptr<PlanBuilder> appendProjections(
            vector<Expression*> expressions,
            shared_ptr<PlanNodeAllocator> planNodeAllocator,
            shared_ptr<VariableAllocator> variableAllocator,
            shared_ptr<PlanNodeIdAllocator> idAllocator,
            shared_ptr<AstNodeAllocator> astAllocator,
            shared_ptr<ExceptionCollector> exceptionCollector,
            shared_ptr<FunctionAndTypeResolver> functionAndTypeResolver,
            shared_ptr<Analysis> analysis,
            shared_ptr<SqlToRowExpressionTranslator> sqlToRowExpressionTranslator)
    {
        shared_ptr<TranslationMap> translations = copyTranslations();

        shared_ptr<AssignmentsBuilder> projections = make_shared<AssignmentsBuilder>();

        auto outputVariables = getRoot()->getOutputVariables();
        // add an identity projection for underlying plan
        for (shared_ptr<VariableReferenceExpression> variable : outputVariables) {
            projections->put(variable,variable);
        }

        map<shared_ptr<VariableReferenceExpression>, Expression*> newTranslations;
        for (Expression *expression : expressions) {
            shared_ptr<VariableReferenceExpression> variable = variableAllocator->newVariable(expression->getLocation(),variableAllocator->getNameHint(expression),
                                                                                              make_shared<Type>(analysis->getTypeWithCoercions(expression)->getType()),"");
            projections->put(variable, TranslateExpressions::rowExpression(translations->rewrite(expression),functionAndTypeResolver,exceptionCollector,astAllocator, variableAllocator,sqlToRowExpressionTranslator));
            newTranslations[variable] = expression;
        }
        // Now append the new translations into the TranslationMap
        for (auto item : newTranslations) {
            translations->put(item.second, item.first);
        }


        return make_shared<PlanBuilder>(translations, planNodeAllocator->new_ProjectNode(idAllocator->getNextId(),getRoot(),projections->build()));
    }

};


#endif //FRONTEND_PLANBUILDER_HPP
