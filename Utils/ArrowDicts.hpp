//
// Created by zxk on 5/19/23.
//

#ifndef OLVP_ARROWDICTS_HPP
#define OLVP_ARROWDICTS_HPP


#include "gandiva/tree_expr_builder.h"
#include <string>
#include "spdlog/spdlog.h"
using namespace std;
class Typer
{

public:

    Typer(){

    }

    static gandiva::DataTypePtr getType(string type)
    {
        if(type == "int32")
            return arrow::int32();
        else if(type == "int16")
            return arrow::int16();
        else if(type == "int64")
            return arrow::int64();
        else if(type == "float")
            return arrow::float32();
        else if(type == "double")
            return arrow::float64();
        else if(type == "halfFloat")
            return arrow::float16();
        else if(type == "string")
            return arrow::utf8();
        else if(type == "bool")
            return arrow::boolean();
        else if(type == "date32")
            return arrow::date32();
        else if(type == "day_time_interval")
            return arrow::day_time_interval();
        else if(type == "date64")
            return arrow::date64();
        else
        {
            spdlog::warn("Typer cannot find the type "+type);
            return NULL;
        }

    }



    static std::shared_ptr<arrow::Array> make_arrow_column(const arrow::DataType& type, const std::vector<std::string>& data) {
        std::shared_ptr<arrow::Array> array;


        switch (type.id()) {
            case arrow::Type::INT32: {
                arrow::Int32Builder builder;
                for (const auto& s : data) {
                    int32_t val = std::stoi(s);
                    builder.Append(val).ok();
                }
                builder.Finish(&array).ok();
                break;
            }
            case arrow::Type::INT64: {
                arrow::Int64Builder builder;
                for (const auto& s : data) {
                    int64_t val = std::stoll(s);
                    builder.Append(val).ok();
                }
                builder.Finish(&array).ok();
                break;
            }
            case arrow::Type::FLOAT: {
                arrow::FloatBuilder builder;
                for (const auto& s : data) {
                    float val = std::stof(s);
                    builder.Append(val).ok();
                }
                builder.Finish(&array).ok();
                break;
            }
            case arrow::Type::DOUBLE: {
                arrow::DoubleBuilder builder;
                for (const auto& s : data) {
                    double val = std::stod(s);
                    builder.Append(val).ok();
                }
                builder.Finish(&array).ok();
                break;
            }
            case arrow::Type::STRING: {
                arrow::StringBuilder builder;
                for (const auto& s : data) {
                    builder.Append(s).ok();
                }
                builder.Finish(&array).ok();
                break;
            }
            default:
                throw std::runtime_error("Make_arrow_column unsupported Arrow data type");
        }


        return array;
    }




};


#endif //OLVP_ARROWDICTS_HPP
