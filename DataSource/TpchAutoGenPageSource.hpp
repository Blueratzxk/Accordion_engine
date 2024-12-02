//
// Created by zxk on 11/30/24.
//

#ifndef OLVP_TPCHAUTOGENPAGESOURCE_HPP
#define OLVP_TPCHAUTOGENPAGESOURCE_HPP



#include "SchemaManager.hpp"
#include "../Connector/ConnectorPageSource.hpp"
#include "tpch/TpchTableGen.hpp"


using namespace std;

#include "../Session/Session.hpp"

class TpchAutoGenPageSource: public ConnectorPageSource
{

    CatalogsMetaManager smm;
    shared_ptr<ConnectorPageSource> splitGen = NULL;


    string catalogName;
    string schemaName;
    string tableName;
    shared_ptr<Session> session;
    string defaultScanSize = "-1";
    int scaleFactor = -1;

public:
    TpchAutoGenPageSource(shared_ptr<Session> session) : ConnectorPageSource("TpchAutoGenPageSource")
    {
        this->session = session;
    }

    TpchAutoGenPageSource(shared_ptr<Session> session,string defaultScanSize): ConnectorPageSource("TpchAutoGenPageSource")
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
    int getScaleFactor(){return this->scaleFactor;}

    bool setSourceConfig(string CatalogName,string SchemaName,string TableName,int SplitTupleCount,int SplitOffset,int scanBatchSize,int scaleFactor)
    {
        this->catalogName = CatalogName;
        this->schemaName = SchemaName;
        this->tableName = TableName;


        this->scaleFactor = scaleFactor;
        shared_ptr<TpchTableGen> gen = make_shared<TpchTableGen>(SplitTupleCount,SplitOffset,scanBatchSize,scaleFactor,TableName);

        this->splitGen = gen;

        return true;

    }

    size_t getFileBytesSize(){return this->splitGen->getFileBytesSize();}
    size_t getBytesRead(){return this->splitGen->getBytesRead();}


    std::shared_ptr<arrow::RecordBatch> getNextBatch()
    {
        return this->splitGen->getNextBatch();

    }


};


#endif //OLVP_TPCHAUTOGENPAGESOURCE_HPP
