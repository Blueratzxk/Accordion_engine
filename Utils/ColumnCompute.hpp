//
// Created by zxk on 4/18/25.
//

#ifndef OLVP_COLUMNCOMPUTE_HPP
#define OLVP_COLUMNCOMPUTE_HPP

#include "arrow/api.h"
#include "arrow/compute/function.h"
#include "arrow/compute/cast.h"
#include "spdlog/spdlog.h"
#include "TypeUtils.hpp"
class ColumnComputeUtils
{

public:
    static bool ComputeInAndToRecordBatch(std::shared_ptr<arrow::RecordBatch> base,std::shared_ptr<arrow::RecordBatch> &newObj,std::string operation,std::string keyLeft,std::string keyRight,std::string keyOutput)
    {
        int indexLeft = -1;
        int indexRight = -1;

        auto fieldNames = base->schema()->field_names();
        for(int i = 0 ; i < fieldNames.size() ; i++)
        {
            if(fieldNames[i] == keyLeft)
                indexLeft = i;
            if(fieldNames[i] == keyRight)
                indexRight = i;
        }
        if(indexLeft == -1 || indexRight == -1)
            return false;

        if(operation == "divide")
        {
            auto result = divide(base->column(indexLeft),base->column(indexRight));
            auto newRecordBatch = base->AddColumn(0,keyOutput,result).ValueOrDie();
            newObj = newRecordBatch;
            return true;
        }
        else
        {
            spdlog::info("Unsupported compute function "+operation+" in recordBatch!");
        }
        return false;

    }

    static std::shared_ptr<arrow::Array> divide(std::shared_ptr<arrow::Array> left,std::shared_ptr<arrow::Array> right)
    {
        if(left->type() != arrow::float64())
            left = arrow::compute::Cast(left,arrow::compute::CastOptions::Safe(arrow::float64())).ValueOrDie().make_array();
        if(right->type() != arrow::float64())
            right = arrow::compute::Cast(right,arrow::compute::CastOptions::Safe(arrow::float64())).ValueOrDie().make_array();
        auto result = arrow::compute::CallFunction("divide",{left,right});

        if(result.ok())
        {
            std::shared_ptr<arrow::Array> array = result->make_array();
            return array;
        }
        else
            spdlog::critical("Column compute error!");
        return NULL;
    }


};



#endif //OLVP_COLUMNCOMPUTE_HPP
