//
// Created by zxk on 5/21/23.
//

#ifndef OLVP_ARROWFUNCTIONOPTIONSSUPPLIER_HPP
#define OLVP_ARROWFUNCTIONOPTIONSSUPPLIER_HPP

#include "arrow/compute/function.h"
#include "arrow/compute/api_aggregate.h"
#include <string>
#include "spdlog/spdlog.h"
using namespace std;
class ArrowFunctionOptionsSupplier
{

public:
    ArrowFunctionOptionsSupplier()
    {

    }
    static std::shared_ptr<arrow::compute::FunctionOptions> getOptions(string functionName)
    {
        if(functionName == "hash_count" || functionName == "hash_count_distinct" || functionName == "hash_distinct" || functionName == "hash_count")
        {
            return std::make_shared<arrow::compute::CountOptions>();
        }
        else if(functionName == "count" || functionName == "count_distinct")
        {
            return std::make_shared<arrow::compute::CountOptions>();
        }
        else if(functionName == "hash_all" || functionName == "hash_any" || functionName == "hash_approximate_median" ||
        functionName == "hash_max" || functionName == "hash_min" || functionName == "hash_mean" ||
        functionName == "hash_min_max" || functionName == "hash_sum")
        {
            return std::make_shared<arrow::compute::ScalarAggregateOptions>();
        }
        else if(functionName == "all" || functionName == "any" || functionName == "approximate_median" ||
                functionName == "max" || functionName == "min" || functionName == "mean" ||
                functionName == "min_max" || functionName == "sum" )
        {
            return std::make_shared<arrow::compute::ScalarAggregateOptions>();
        }
        else
        {
            spdlog::critical("FunctionOptionsSupplier cannot resolve the functionName "+ functionName +"!!");
            return NULL;
        }
    }


};


#endif //OLVP_ARROWFUNCTIONOPTIONSSUPPLIER_HPP
