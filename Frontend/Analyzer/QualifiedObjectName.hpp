//
// Created by zxk on 10/4/24.
//

#ifndef FRONTEND_QUALIFIEDOBJECTNAME_HPP
#define FRONTEND_QUALIFIEDOBJECTNAME_HPP
#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include "spdlog/spdlog.h"
using namespace std;

class QualifiedObjectName
{

    string catalogName;
    string schemaName;
    string objectName;

public:
    static void Stringsplit(std::string str, const char split,std::vector<std::string>& res)
    {
        std::istringstream iss(str);	// 输入流
        std::string token;			// 接收缓冲区
        while (getline(iss, token, split))	// 以split为分隔符
        {
            res.push_back(token);
        }
    }
    static shared_ptr<QualifiedObjectName> valueOf(string name)
    {


        vector<string> parts;
        Stringsplit(name,'.',parts);

        if (parts.size() != 3) {
            spdlog::error("QualifiedObjectName should have exactly 3 parts");
        }

        return make_shared<QualifiedObjectName>(parts[0], parts[1], parts[2]);
    }




    QualifiedObjectName(string catalogName, string schemaName, string objectName)
    {

        this->catalogName = catalogName;
        this->schemaName = schemaName;
        this->objectName = objectName;
    }




    string getCatalogName()
    {
        return catalogName;
    }


    string getSchemaName()
    {
        return schemaName;
    }


    string getObjectName()
    {
        return objectName;
    }



};



#endif //FRONTEND_QUALIFIEDOBJECTNAME_HPP
