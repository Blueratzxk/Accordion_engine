//
// Created by zxk on 10/24/24.
//

#ifndef FRONTEND_TABLEHANDLE_HPP
#define FRONTEND_TABLEHANDLE_HPP


class TableHandle
{

    string catalogName;
    string schemaName;
    string tableName;

public:
    TableHandle(string catalogName,string schemaName,string tableName){
        this->catalogName = catalogName;
        this->schemaName = schemaName;
        this->tableName = tableName;
    }

    string getCatalogName(){return this->catalogName;}
    string getSchemaName(){return this->schemaName;}
    string getTableName(){return this->tableName;}

};


#endif //FRONTEND_TABLEHANDLE_HPP
