//
// Created by zxk on 10/1/24.
//

#ifndef FRONTEND_SQLPARSER_HPP
#define FRONTEND_SQLPARSER_HPP

#include <iostream>
#include <string>
#include "pg_query.h"
#include <memory>
using namespace std;

class SqlParser
{
    string SQL;
    PgQueryParseResult result;

public:
    SqlParser(string sql)
    {
        this->SQL = sql;

    }

    void parse()
    {
        result = pg_query_parse(this->SQL.c_str());
    }

    char * getParseResultJson()
    {
        char *SQLJson = result.parse_tree;
        return SQLJson;
    }

    void parserExit()
    {
        pg_query_exit();
    }

    ~SqlParser(){
         pg_query_free_parse_result(result);

    }



};

#endif //FRONTEND_SQLPARSER_HPP
