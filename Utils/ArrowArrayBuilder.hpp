//
// Created by zxk on 5/20/23.
//

#ifndef OLVP_ARROWARRAYBUILDER_HPP
#define OLVP_ARROWARRAYBUILDER_HPP

#include "arrow/builder.h"
#include "spdlog/spdlog.h"
class ArrowArrayBuilder
{
    std::shared_ptr<arrow::ArrayBuilder> builder = NULL;

public:
    ArrowArrayBuilder(std::shared_ptr<arrow::DataType> type)
    {
        if(type->Equals(arrow::Int32Type()))
        {
            this->builder = std::make_shared<arrow::Int32Builder>();
        }
        else if(type->Equals(arrow::Int64Type()))
        {
            this->builder = std::make_shared<arrow::Int64Builder>();
        }
        else if(type->Equals(arrow::DoubleType()))
        {
            this->builder = std::make_shared<arrow::DoubleBuilder>();
        }
        else if(type->Equals(arrow::utf8()))
        {
            this->builder = std::make_shared<arrow::StringBuilder>();
        }
        else if(type->Equals(arrow::FloatType()))
        {
            this->builder = std::make_shared<arrow::FloatBuilder>();
        }
        else if(type->Equals(arrow::Date32Type()))
        {
            this->builder = std::make_shared<arrow::Date32Builder>();
        }
        else
        {
            spdlog::critical("ArrowArrayBuilder cannot resolve the type "+type->ToString());
        }

    }
    void Reset()
    {
        this->builder->Reset();
    }
    bool addScalar(std::shared_ptr<arrow::Scalar> scalar)
    {
        arrow::Status status = this->builder->AppendScalar(*scalar);
        if(!status.ok())
            return false;
        return true;
    }

    int getBytesSize()
    {
        return this->builder->length()*this->builder->type()->byte_width();
    }

    arrow::Result<std::shared_ptr<arrow::Array>> buildFinish()
    {
        arrow::Result<std::shared_ptr<arrow::Array>> result = this->builder->Finish();
        return result;
    }

};



#endif //OLVP_ARROWARRAYBUILDER_HPP
