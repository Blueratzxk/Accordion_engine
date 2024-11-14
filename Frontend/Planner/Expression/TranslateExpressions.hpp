//
// Created by zxk on 11/1/24.
//

#ifndef FRONTEND_TRANSLATEEXPRESSIONS_HPP
#define FRONTEND_TRANSLATEEXPRESSIONS_HPP


#include "../../Analyzer/FunctionAndTypeResolver.hpp"

#include "../VariableAllocator.hpp"
#include "../../Analyzer/Analysis.hpp"
#include "SqlToRowExpressionTranslator.hpp"
#include "../../PlanNode/NodeVisitor.hpp"
#include "../../Analyzer/ExpressionAnalyzer.hpp"
class AstNodeAllocator;
class TranslateExpressions
{

public:

    static shared_ptr<RowExpression>rowExpression(Expression *expression,shared_ptr<FunctionAndTypeResolver> functionAndTypeResolver,shared_ptr<ExceptionCollector> exceptionCollector,
                                           shared_ptr<AstNodeAllocator> astAllocator,shared_ptr<VariableAllocator> variableAllocator,
                                           shared_ptr<SqlToRowExpressionTranslator> sqlToRowExpressionTranslator) {

        shared_ptr<Analysis> analysis = make_shared<Analysis>("");
        shared_ptr<ExpressionAnalyzer> expressionAnalyzer = make_shared<ExpressionAnalyzer>(analysis,
                                                                                            functionAndTypeResolver,
                                                                                            exceptionCollector,
                                                                                            astAllocator);
        auto result = expressionAnalyzer->analyzeExpression(variableAllocator->getTypes(), expression);

        auto symbolTypes = result->getTypes();
        auto re = sqlToRowExpressionTranslator->translate(expression, functionAndTypeResolver, symbolTypes);

        return re;
    }

};



#endif //FRONTEND_TRANSLATEEXPRESSIONS_HPP
