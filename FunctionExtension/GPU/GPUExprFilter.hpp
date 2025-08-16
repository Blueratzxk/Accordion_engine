//
// Created by zxk on 8/16/25.
//

#ifndef OLVP_GPUEXPRFILTER_HPP
#define OLVP_GPUEXPRFILTER_HPP


#include "GPUFilterExprEvaluator.hpp"
class GPUExprFilter
{
    std::shared_ptr<AstNodePtr> Expr;
    std::shared_ptr<arrow::Schema> input_schema;

public:
    GPUExprFilter(std::shared_ptr<arrow::Schema> input_schemaIn,std::shared_ptr<AstNodePtr> ExprAstTree)
    {
        this->Expr = ExprAstTree;
        this->input_schema = input_schemaIn;

    }

    shared_ptr<DataPage> evaluate(shared_ptr<DataPage> page)
    {
        shared_ptr<AstTreeEvaluator> astTreeEvaluator = make_shared<AstTreeEvaluator>(this->input_schema,page);
        auto re = astTreeEvaluator->Visit(Expr->get(),NULL);
        int elementCount = 0;
        auto final = astTreeEvaluator->filter(re,elementCount);

        return make_shared<DataPage>(final,elementCount,DataPage::GPU);
    }



};


#endif //OLVP_GPUEXPRFILTER_HPP
