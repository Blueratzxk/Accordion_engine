//
// Created by zxk on 5/17/23.
//

#ifndef OLVP_ARROWRECORDBATCHVIEWER_HPP
#define OLVP_ARROWRECORDBATCHVIEWER_HPP

#include <iostream>
#include "arrow/record_batch.h"
#include "arrow/array.h"
using namespace std;



/*
arrow::NullType
arrow::BooleanType
arrow::Int8Type
arrow::Int16Type
arrow::Int32Type
arrow::Int64Type
arrow::UInt8Type
arrow::UInt16Type
arrow::UInt32Type
arrow::UInt64Type
arrow::HalfFloatType
arrow::FloatType
arrow::DoubleType
arrow::StringType
arrow::BinaryType
arrow::LargeStringType
arrow::LargeBinaryType
arrow::FixedSizeBinaryType
arrow::Date32Type
arrow::Date64Type
arrow::Time32Type
arrow::Time64Type
arrow::TimestampType
arrow::DayTimeIntervalType
arrow::MonthDayNanoIntervalType
arrow::MonthIntervalType
arrow::DurationType
arrow::Decimal128Type
arrow::Decimal256Type
arrow::ListType
arrow::LargeListType
arrow::MapType
arrow::FixedSizeListType
arrow::StructType
arrow::SparseUnionType
arrow::DenseUnionType
arrow::DictionaryType
arrow::RunEndEncodedType
arrow::ExtensionType

 */




class ArrowRecordBatchViewer
{

public:


    static void PrintBatchRows(std::shared_ptr<arrow::RecordBatch> batch)
    {
        int numRows = batch->num_rows();
        std::vector<std::shared_ptr<arrow::Array>> columns = batch->columns();

        for(int j = 0 ; j < columns.size() ; j++)
        {
            cout << batch->column_name(j) <<"|";
        }
        cout << endl;
        for(int j = 0 ; j < columns.size() ; j++)
        {
            cout << columns[j]->type()->ToString() << "|";
        }
        cout << endl;
        for(int i = 0 ; i < numRows ; i++)
        {

            for(int j = 0 ; j < columns.size() ; j++)
            {
                if(columns[j]->type()->Equals(arrow::Int64Type()))
                {
                    cout << static_pointer_cast<arrow::Int64Array>(columns[j])->Value(i) << "|";
                }
                else if(columns[j]->type()->Equals(arrow::StringType()))
                {
                    cout << static_pointer_cast<arrow::StringArray>(columns[j])->GetString(i) << "|";
                }
                else if(columns[j]->type()->Equals(arrow::Int32Type()))
                {
                    cout << static_pointer_cast<arrow::Int32Array>(columns[j])->Value(i) << "|";
                }
                else if(columns[j]->type()->Equals(arrow::FloatType()))
                {
                    cout << static_pointer_cast<arrow::FloatArray>(columns[j])->Value(i) << "|";
                }
                else if(columns[j]->type()->Equals(arrow::DoubleType()))
                {
                    cout << static_pointer_cast<arrow::DoubleArray>(columns[j])->Value(i) << "|";
                }
                else if(columns[j]->type()->Equals(arrow::Date32Type()))
                {
                    cout << static_pointer_cast<arrow::Date32Array>(columns[j])->Value(i)<<"("<<static_pointer_cast<arrow::Date32Array>(columns[j])->GetScalar(i).ValueOrDie()->ToString()<<")"<< "|";
                }
            }
            cout << endl;

        }

    }




    static string BatchRowsToString(std::shared_ptr<arrow::RecordBatch> batch)
    {
        string result;
        int numRows = batch->num_rows();
        std::vector<std::shared_ptr<arrow::Array>> columns = batch->columns();

        for(int j = 0 ; j < columns.size() ; j++)
        {
            result.append(batch->column_name(j)).append("|");
        }
        result.append("\n");
        for(int j = 0 ; j < columns.size() ; j++)
        {
            result.append(columns[j]->type()->ToString()).append("|");
        }
        result.append("\n");
        for(int i = 0 ; i < numRows ; i++)
        {

            for(int j = 0 ; j < columns.size() ; j++)
            {
                if(columns[j]->type()->Equals(arrow::Int64Type()))
                {
                    result.append(to_string(static_pointer_cast<arrow::Int64Array>(columns[j])->Value(i))).append("|");
                }
                else if(columns[j]->type()->Equals(arrow::StringType()))
                {
                    result.append(static_pointer_cast<arrow::StringArray>(columns[j])->GetString(i)).append("|");
                }
                else if(columns[j]->type()->Equals(arrow::Int32Type()))
                {
                   result.append(to_string(static_pointer_cast<arrow::Int32Array>(columns[j])->Value(i))).append ("|");
                }
                else if(columns[j]->type()->Equals(arrow::FloatType()))
                {
                    result.append(to_string(static_pointer_cast<arrow::FloatArray>(columns[j])->Value(i))).append("|");
                }
                else if(columns[j]->type()->Equals(arrow::DoubleType()))
                {
                    result.append(to_string(static_pointer_cast<arrow::DoubleArray>(columns[j])->Value(i))).append("|");
                }
                else if(columns[j]->type()->Equals(arrow::Date32Type()))
                {
                    result.append(static_pointer_cast<arrow::Date32Array>(columns[j])->GetScalar(i).ValueOrDie()->ToString()).append("|");
                }
            }
            result.append("\n");

        }

        return result;

    }


};


#endif //OLVP_ARROWRECORDBATCHVIEWER_HPP
