//
// Created by zxk on 10/18/24.
//

#ifndef FRONTEND_TYPESIGNATURE_HPP
#define FRONTEND_TYPESIGNATURE_HPP

#include "StandardTypes.h"
#include <string>
using namespace std;
class TypeSignature
{
    string baseType;
    string type;
public:
    TypeSignature(string baseType,string type)
    {
        this->type = type;
        this->baseType = change(baseType);
    }

    TypeSignature(string baseType)
    {
        this->type = "";
        this->baseType = change(baseType);
    }

    string getBaseType()
    {
        return this->baseType;
    }

    string change(string type)
    {
       return type;
    }

    static shared_ptr<TypeSignature> parseTypeSignature(string type)
    {
        int leftBracket = type.find('(');
        string typeString = type.substr(0,leftBracket);
        
        if(StandardTypes::isStandardType(typeString)) {
            if(typeString == "decimal")
                typeString = "double";
            return make_shared<TypeSignature>(typeString, "");
        }


        if(typeString == "T")
            return make_shared<TypeSignature>("T","");

        if(typeString == "int32")
        {
            return make_shared<TypeSignature>(StandardTypes::BIGINT,"int32");
        }
        else if(typeString == "int64")
        {
            return make_shared<TypeSignature>(StandardTypes::BIGINT,"int64");
        }
        else if(typeString == "double")
        {
            return make_shared<TypeSignature>(StandardTypes::DOUBLE,"double");
        }
        else if(typeString == "date32")
        {
            return make_shared<TypeSignature>(StandardTypes::DATE,"date32");
        }
        else if(typeString == "bool")
        {
            return make_shared<TypeSignature>(StandardTypes::BOOLEAN,"bool");
        }
        else if(typeString == "string")
        {
            return make_shared<TypeSignature>(StandardTypes::VARCHAR,"string");
        }
        else {
            spdlog::error("Cannot convert this type "+typeString+" to typeSignature!");
            return NULL;
        }
    }
};


#endif //FRONTEND_TYPESIGNATURE_HPP
