//
// Created by zxk on 5/15/23.
//

#ifndef OLVP_CONNECTORPAGESOURCE_HPP
#define OLVP_CONNECTORPAGESOURCE_HPP



#include <arrow/record_batch.h>

class ConnectorPageSource
{
public:

    virtual std::shared_ptr<arrow::RecordBatch> getNextBatch() = 0;
    virtual bool loadTable(std::string path) {return false;}
    virtual size_t getFileBytesSize(){return 0;}
    virtual size_t getBytesRead(){return 0;}

    virtual string getCatalogName(){return "NULL";}

    virtual string getSchemaName(){return "NULL";}

    virtual string getTableName(){ return "NULL";}

};


#endif //OLVP_CONNECTORPAGESOURCE_HPP
