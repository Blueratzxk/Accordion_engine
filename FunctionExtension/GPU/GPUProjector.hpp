//
// Created by zxk on 8/18/25.
//

#ifndef OLVP_GPUPROJECTOR_HPP
#define OLVP_GPUPROJECTOR_HPP



#include "GPUProjectEvaluator.hpp"
#include "../../Frontend/AstNodes/AstNodePtr.hpp"
#include "../../Frontend/AstNodes/ExprOutput.hpp"


class GPUProjector
{
    vector<std::shared_ptr<AstNodePtr>> Exprs;

    vector<ExprOutput> outputs;
    std::shared_ptr<arrow::Schema> input_schema;
    GPUFunctions gpuFunctions;

public:
    GPUProjector(std::shared_ptr<arrow::Schema> input_schemaIn,vector<shared_ptr<AstNodePtr>> ExprAstTrees,vector<ExprOutput> outputsIn) {

        this->Exprs = ExprAstTrees;
        this->input_schema = input_schemaIn;
        this->outputs = outputsIn;
    }
    GPUProjector()
    {
    }

    vector<void*> evaluate(shared_ptr<DataPage> page)
    {
        shared_ptr<GPUProjectEvaluator> astTreeEvaluator = make_shared<GPUProjectEvaluator>(this->input_schema,page);

        vector<void*> resultColumns;

        for(int i = 0 ; i < this->Exprs.size() ; i++) {
            auto re = astTreeEvaluator->Visit(Exprs[i]->get(), NULL);
            auto *result = (ProjectEvaResult *) re;
            resultColumns.push_back(result->getColumn());
            delete result;
        }

        return resultColumns;
    }


};


#endif //OLVP_GPUPROJECTOR_HPP
