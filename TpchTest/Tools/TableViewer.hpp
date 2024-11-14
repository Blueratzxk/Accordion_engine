//
// Created by zxk on 6/17/23.
//

#ifndef OLVP_TABLEVIEWER_HPP
#define OLVP_TABLEVIEWER_HPP

#include "../../DataSource/TpchPageSource.hpp"
#include "../../Utils/ArrowRecordBatchViewer.hpp"
class TableViewer
{

    string catalogName;
    string schemaName;
    string tableName;
    shared_ptr<Session> session = make_shared<Session>("query",-1);
    std::shared_ptr<TpchPageSource> tpch = std::make_shared<TpchPageSource>(session);
public:
    TableViewer(string catalogName,string schemaName,string tableName){
        this->catalogName = catalogName;
        this->schemaName = schemaName;
        this->tableName = tableName;
    }

    void viewSchema()
    {
        if(!tpch->load(catalogName,schemaName,tableName)) {
           spdlog::critical("Tpch TableViewer Cannot load this table!");
           return;
        }

        shared_ptr<arrow::RecordBatch> batch = tpch->getNextBatch();
        cout << (batch->schema()->ToString()) << endl;
        cout << "---------------------------------------------"<<endl;

    }

    void viewData()
    {
        ArrowRecordBatchViewer viewer;
        if(!tpch->load(catalogName,schemaName,tableName)) {
            spdlog::critical("Tpch TableViewer Cannot load this table!");
            return;
        }

        shared_ptr<arrow::RecordBatch> batch = tpch->getNextBatch();
        viewer.PrintBatchRows(batch);
    }




};


#endif //OLVP_TABLEVIEWER_HPP
