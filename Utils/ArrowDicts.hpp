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




};


#endif //OLVP_ARROWDICTS_HPP
