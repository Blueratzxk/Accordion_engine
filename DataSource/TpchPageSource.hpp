//
// Created by zxk on 5/15/23.
//

#ifndef OLVP_TPCHPAGESOURCE_HPP
#define OLVP_TPCHPAGESOURCE_HPP


//#include "../common.h"
#include "SchemaManager.hpp"
#include "../Connector/ConnectorPageSource.hpp"


#include "CSVTableReader.hpp"
#include "CSVCacheableTableReader.hpp"
#include "CSVOnceTableReader.hpp"
using namespace std;

class TpchPageSource: public ConnectorPageSource
{

    CatalogsMetaManager smm;
    shared_ptr<ConnectorPageSource> splitGen = NULL;


    string catalogName;
    string schemaName;
    string tableName;
    shared_ptr<Session> session;
    string defaultScanSize = "-1";

public:
    TpchPageSource(shared_ptr<Session> session): ConnectorPageSource("TpchPageSource")
    {
        this->session = session;
    }

    TpchPageSource(shared_ptr<Session> session,string defaultScanSize): ConnectorPageSource("TpchPageSource")
    {
        this->session = session;
        this->defaultScanSize = defaultScanSize;

    }

    string getCatalogName()
    {
        return this->catalogName;
    }

    string getSchemaName()
    {
        return this->schemaName;
    }

    string getTableName()
    {
        return this->tableName;
    }

    bool load(string CatalogName,string SchemaName,string TableName)
    {

        TableInfo tinfo = smm.getTable(CatalogName,SchemaName,TableName);

        this->catalogName = CatalogName;
        this->schemaName = SchemaName;
        this->tableName = TableName;

        if(TableInfo::isEmpty(tinfo)) {
            spdlog::warn("Cannot find schema or table definition!");
            return false;
        }

        string tablePath = tinfo.getFilePath();
        string fileType = tinfo.getFileType();


        string size = session->getRuntimeConfigs().findTableScanSizeConfig(this->catalogName+"_"+this->schemaName+"_"+this->tableName);
        if(size != "NULL")
            this->defaultScanSize = size;


        if(fileType.compare("CSV") == 0)
        {
            shared_ptr<CSVTableReader> gen(new CSVTableReader(this->session,defaultScanSize));
            this->splitGen = gen;
        }
        else
        {
            spdlog::critical("unknown data file type!");
            return false;
        }

        bool loadStatus = this->splitGen->loadTable(tablePath);

        if(!loadStatus)
            return false;

        return true;

    }

    bool load(string CatalogName,string SchemaName,string TableName,string partitionFile)
    {
        this->catalogName = CatalogName;
        this->schemaName = SchemaName;
        this->tableName = TableName;

        string size = session->getRuntimeConfigs().findTableScanSizeConfig(this->catalogName+"_"+this->schemaName+"_"+this->tableName);

        if(size != "NULL")
            this->defaultScanSize = size;


        shared_ptr<CSVTableReader> gen(new CSVTableReader(this->session,defaultScanSize));

        this->splitGen = gen;



        bool loadStatus = this->splitGen->loadTable(partitionFile);

        if(!loadStatus)
            return false;

        return true;

    }

    size_t getFileBytesSize(){return this->splitGen->getFileBytesSize();}
    size_t getBytesRead(){return this->splitGen->getBytesRead();}


    std::shared_ptr<arrow::RecordBatch> getNextBatch()
    {
        return this->splitGen->getNextBatch();

    }


};

#endif //OLVP_TPCHPAGESOURCE_HPP
