//
// Created by zxk on 5/22/23.
//

#ifndef OLVP_TYPEUTILS_HPP
#define OLVP_TYPEUTILS_HPP

#include "arrow/util/hashing.h"
#include "arrow/api.h"
#include "arrow/array.h"
#include "arrow/chunked_array.h"
#include "arrow/scalar.h"
#include "ArrowArrayBuilder.hpp"
class TypeUtils
{

public:
    TypeUtils(){}


    static uint64_t hashPosition(std::shared_ptr<arrow::DataType> type,std::shared_ptr<arrow::ChunkedArray> array,int position)
    {

        if(type->Equals(arrow::Int32Type()))
        {
            std::shared_ptr<arrow::Scalar> value = array->GetScalar(position).ValueOrDie();
            std::shared_ptr<arrow::Int32Scalar> true_value = dynamic_pointer_cast<arrow::Int32Scalar>(value);

            int dd = true_value->value ;
            uint64_t hash = arrow::internal::ScalarHelper<int32_t ,0>::ComputeHash(true_value->value);

            return hash;
        }
        else if(type->Equals(arrow::Int64Type()))
        {
            std::shared_ptr<arrow::Scalar> value = array->GetScalar(position).ValueOrDie();
            std::shared_ptr<arrow::Int64Scalar> true_value = dynamic_pointer_cast<arrow::Int64Scalar>(value);
            uint64_t hash = arrow::internal::ScalarHelper<int64_t ,0>::ComputeHash(true_value->value);
            return hash;
        }
        else if(type->Equals(arrow::DoubleType()))
        {
            std::shared_ptr<arrow::Scalar> value = array->GetScalar(position).ValueOrDie();
            std::shared_ptr<arrow::DoubleScalar> true_value = dynamic_pointer_cast<arrow::DoubleScalar>(value);
            uint64_t hash = arrow::internal::ScalarHelper<double ,0>::ComputeHash(true_value->value);
            return hash;
        }
        else if(type->Equals(arrow::FloatType()))
        {
            std::shared_ptr<arrow::Scalar> value = array->GetScalar(position).ValueOrDie();
            std::shared_ptr<arrow::FloatScalar> true_value = dynamic_pointer_cast<arrow::FloatScalar>(value);
            uint64_t hash = arrow::internal::ScalarHelper<float ,0>::ComputeHash(true_value->value);
            return hash;
        }
        else if(type->Equals(arrow::utf8()))
        {
            std::shared_ptr<arrow::Scalar> value = array->GetScalar(position).ValueOrDie();
            std::shared_ptr<arrow::StringScalar> true_value = dynamic_pointer_cast<arrow::StringScalar>(value);
            uint64_t hash = arrow::internal::ScalarHelper<string ,0>::ComputeHash(string((char*)true_value->data()));
            return hash;
        }
        else if(type->Equals(arrow::date32()))
        {
            std::shared_ptr<arrow::Scalar> value = array->GetScalar(position).ValueOrDie();
            std::shared_ptr<arrow::Date32Scalar> true_value = dynamic_pointer_cast<arrow::Date32Scalar>(value);
            uint64_t hash = arrow::internal::ScalarHelper<int32_t,0>::ComputeHash(true_value->value);
            return hash;
        }


        else
        {
            spdlog::critical("TypeUtils cannot find the scalar type " + type->ToString() +"!");
        }
        return -1;


    }

    static uint64_t hashPosition(std::shared_ptr<arrow::DataType> type,std::shared_ptr<arrow::Array> array,int position)
    {
        if(type->Equals(arrow::Int32Type()))
        {
            std::shared_ptr<arrow::Int32Array> true_array = dynamic_pointer_cast<arrow::Int32Array>(array);
            uint64_t hash = arrow::internal::ScalarHelper<int32_t,0>::ComputeHash(true_array->Value(position));
            return hash;
        }
        else if(type->Equals(arrow::Int64Type()))
        {
            std::shared_ptr<arrow::Int64Array> true_array = dynamic_pointer_cast<arrow::Int64Array>(array);
            uint64_t hash = arrow::internal::ScalarHelper<int64_t ,0>::ComputeHash(true_array->Value(position));
            return hash;
        }
        else if(type->Equals(arrow::DoubleType()))
        {
            std::shared_ptr<arrow::DoubleArray> true_array = dynamic_pointer_cast<arrow::DoubleArray>(array);
            uint64_t hash = arrow::internal::ScalarHelper<double ,0>::ComputeHash(true_array->Value(position));
            return hash;
        }
        else if(type->Equals(arrow::FloatType()))
        {
            std::shared_ptr<arrow::FloatArray> true_array = dynamic_pointer_cast<arrow::FloatArray>(array);
            uint64_t hash = arrow::internal::ScalarHelper<float ,0>::ComputeHash(true_array->Value(position));
            return hash;
        }
        else if(type->Equals(arrow::utf8()))
        {
            std::shared_ptr<arrow::StringArray> true_array = dynamic_pointer_cast<arrow::StringArray>(array);
            uint64_t hash = arrow::internal::ScalarHelper<string,0>::ComputeHash(true_array->GetString(position));
            return hash;
        }
        else if(type->Equals(arrow::date32()))
        {
            std::shared_ptr<arrow::Date32Array> true_array = dynamic_pointer_cast<arrow::Date32Array>(array);
            uint64_t hash = arrow::internal::ScalarHelper<int32_t ,0>::ComputeHash(true_array->Value(position));
            return hash;
        }

        else
        {
            spdlog::critical("TypeUtils cannot find the scalar type " + type->ToString() +"!");
        }
        return -1;
    }

    static bool positionEqualsPosition(std::shared_ptr<arrow::DataType> type,std::shared_ptr<arrow::ChunkedArray> array,
                                           int leftPosition,int rightPosition) {



        if(type->Equals(arrow::Int32Type()))
        {
            std::shared_ptr<arrow::Scalar> lvalue = array->GetScalar(leftPosition).ValueOrDie();
            std::shared_ptr<arrow::Int32Scalar> ltrue_value = dynamic_pointer_cast<arrow::Int32Scalar>(lvalue);
            std::shared_ptr<arrow::Scalar> rvalue = array->GetScalar(rightPosition).ValueOrDie();
            std::shared_ptr<arrow::Int32Scalar> rtrue_value = dynamic_pointer_cast<arrow::Int32Scalar>(rvalue);

            return ltrue_value->Equals(*rtrue_value);

        }
        else if(type->Equals(arrow::Int64Type()))
        {
            std::shared_ptr<arrow::Scalar> lvalue = array->GetScalar(leftPosition).ValueOrDie();
            std::shared_ptr<arrow::Int64Scalar> ltrue_value = dynamic_pointer_cast<arrow::Int64Scalar>(lvalue);
            std::shared_ptr<arrow::Scalar> rvalue = array->GetScalar(rightPosition).ValueOrDie();
            std::shared_ptr<arrow::Int64Scalar> rtrue_value = dynamic_pointer_cast<arrow::Int64Scalar>(rvalue);

            return ltrue_value->Equals(*rtrue_value);
        }
        else if(type->Equals(arrow::DoubleType()))
        {
            std::shared_ptr<arrow::Scalar> lvalue = array->GetScalar(leftPosition).ValueOrDie();
            std::shared_ptr<arrow::DoubleScalar> ltrue_value = dynamic_pointer_cast<arrow::DoubleScalar>(lvalue);
            std::shared_ptr<arrow::Scalar> rvalue = array->GetScalar(rightPosition).ValueOrDie();
            std::shared_ptr<arrow::DoubleScalar> rtrue_value = dynamic_pointer_cast<arrow::DoubleScalar>(rvalue);

            return ltrue_value->Equals(*rtrue_value);
        }
        else if(type->Equals(arrow::FloatType()))
        {
            std::shared_ptr<arrow::Scalar> lvalue = array->GetScalar(leftPosition).ValueOrDie();
            std::shared_ptr<arrow::FloatScalar> ltrue_value = dynamic_pointer_cast<arrow::FloatScalar>(lvalue);
            std::shared_ptr<arrow::Scalar> rvalue = array->GetScalar(rightPosition).ValueOrDie();
            std::shared_ptr<arrow::FloatScalar> rtrue_value = dynamic_pointer_cast<arrow::FloatScalar>(rvalue);

            return ltrue_value->Equals(*rtrue_value);
        }
        else if(type->Equals(arrow::utf8()))
        {
            std::shared_ptr<arrow::Scalar> lvalue = array->GetScalar(leftPosition).ValueOrDie();
            std::shared_ptr<arrow::StringScalar> ltrue_value = dynamic_pointer_cast<arrow::StringScalar>(lvalue);
            std::shared_ptr<arrow::Scalar> rvalue = array->GetScalar(rightPosition).ValueOrDie();
            std::shared_ptr<arrow::StringScalar> rtrue_value = dynamic_pointer_cast<arrow::StringScalar>(rvalue);

            return ltrue_value->Equals(*rtrue_value);
        }

        else if(type->Equals(arrow::date32()))
        {
            std::shared_ptr<arrow::Scalar> lvalue = array->GetScalar(leftPosition).ValueOrDie();
            std::shared_ptr<arrow::Date32Scalar> ltrue_value = dynamic_pointer_cast<arrow::Date32Scalar>(lvalue);
            std::shared_ptr<arrow::Scalar> rvalue = array->GetScalar(rightPosition).ValueOrDie();
            std::shared_ptr<arrow::Date32Scalar> rtrue_value = dynamic_pointer_cast<arrow::Date32Scalar>(rvalue);

            return ltrue_value->Equals(*rtrue_value);
        }

        else
        {
            spdlog::critical("TypeUtils cannot find the scalar type " + type->ToString() +"!");
        }
        return -1;


    }

    static bool isPositionNull(std::shared_ptr<arrow::DataType> type,std::shared_ptr<arrow::ChunkedArray> array,
                                       int position) {

        return array->GetScalar(position).ValueOrDie()->Equals(arrow::NullScalar());
    }


    static void appendTo(std::shared_ptr<arrow::DataType> type,std::shared_ptr<arrow::ChunkedArray> array,
                         int position,std::shared_ptr<ArrowArrayBuilder> arrayBuilder)
    {
       arrayBuilder->addScalar(array->GetScalar(position).ValueOrDie());
    }


    static bool equalTo(std::shared_ptr<arrow::ChunkedArray> leftArray,int leftPosition,std::shared_ptr<arrow::Array> rightArray,int rightPosition)
    {
        return leftArray->GetScalar(leftPosition).ValueOrDie()->Equals(*rightArray->GetScalar(rightPosition).ValueOrDie());
    }


};

#endif //OLVP_TYPEUTILS_HPP
