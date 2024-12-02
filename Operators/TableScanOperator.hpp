//
// Created by zxk on 5/16/23.
//

#ifndef OLVP_TABLESCANOPERATOR_HPP
#define OLVP_TABLESCANOPERATOR_HPP

#include "Operator.hpp"
#include "../Split/Split.hpp"
#include "../Connector/PageSourceManager.hpp"
#include "../Connector/SystemPageSourceProvider.hpp"

#include <unistd.h>
using namespace std;

class TableScanOperator:public Operator {
    int elementsCount;

    std::shared_ptr<DataPage> out;
    bool finished;


    string name = "TableScanOperator";

    atomic<bool> forceShutDown = false;
    mutex forceShutDownLock;

    int count = 0;


    std::shared_ptr<DataPage> inputPage = NULL;


    std::shared_ptr<PageSourceManager> PSM;
    std::shared_ptr<ConnectorPageSource> connectorPageSource = NULL;

    mutex addSplitLock;

    shared_ptr<DriverContext> driverContext;


    shared_ptr<TableScanRecord> tableScanRecord;


    string tableScanId = "NULL";
    bool endPageSend = false;
    int counter = 0;

    int tupleLen = -1;

public:
    string getOperatorId() { return this->name; }

    TableScanOperator(shared_ptr<DriverContext> driverContext,std::shared_ptr<PageSourceManager> pageSourceProvider) {

        this->PSM = pageSourceProvider;

        this->elementsCount = 0;
        this->driverContext = driverContext;
        this->tableScanRecord = NULL;
        this->finished = false;


    }

    TableScanOperator(string id,shared_ptr<DriverContext> driverContext,std::shared_ptr<PageSourceManager> pageSourceProvider) {

        this->PSM = pageSourceProvider;

        this->elementsCount = 0;
        this->driverContext = driverContext;
        this->tableScanRecord = NULL;
        this->finished = false;
        this->tableScanId = id;


    }

    void addInput(std::shared_ptr<DataPage> input) override {

    }


    void checkShutDownAndSet() {
        bool isShutDown;

        isShutDown = this->forceShutDown;

        if (isShutDown == true) {
            this->elementsCount = 0;
            this->finished = true;

        }
    }

    std::shared_ptr<DataPage> getOutput() override {

        std::shared_ptr<DataPage> page = NULL;

 //       while(this->driverContext->getOutputBuffer()->isFull())
//            ;
        //usleep(1000000);

  //      if(this->driverContext->getOutputBuffer()->isEmpty())
 //       {
 //           this->driverContext->getOutputBuffer()->changeBufferSize();
 //       }


        if(this->finished)
        {
            spdlog::info("TableScan Quit! Total process " + to_string(counter) + " small pages\n");
            return NULL;
        }


        addSplitLock.lock();
        if (this->connectorPageSource != NULL) {
        //    addSplitLock.unlock();
            std::shared_ptr<arrow::RecordBatch> batch = this->connectorPageSource->getNextBatch();

            if (batch == NULL) {

                this->finished = true;
                spdlog::info("TableScan Quit! Total process " + to_string(counter) + " pages\n");
                addSplitLock.unlock();
                spdlog::debug("TableScan return endPage\n");
                return DataPage::getEndPage();

            }

            if(this->tupleLen == -1)
            {
                shared_ptr<arrow::Schema> sche = batch->schema();
                for(int i = 0 ; i < sche->num_fields() ; i++)
                    this->tupleLen += sche->field(i)->type()->byte_width();
            }

            this->elementsCount += batch->num_rows();

            if(this->tableScanRecord == NULL)
            {
                if(this->connectorPageSource->getConnectorPageSourceId() == "TpchAutoGenPageSource") {
                    auto pageSource = dynamic_pointer_cast<TpchAutoGenPageSource>(this->connectorPageSource);
                    this->tableScanRecord = make_shared<TableScanRecord>(this->tableScanId,
                                                                         this->connectorPageSource->getCatalogName(),
                                                                         this->connectorPageSource->getSchemaName(),
                                                                         this->connectorPageSource->getTableName(),
                                                                         0,
                                                                         this->connectorPageSource->getFileBytesSize(),
                                                                         true, pageSource->getScaleFactor());


                }
                else
                    this->tableScanRecord = make_shared<TableScanRecord>(this->tableScanId,
                                                                         this->connectorPageSource->getCatalogName(),
                                                                         this->connectorPageSource->getSchemaName(),
                                                                         this->connectorPageSource->getTableName(),
                                                                         0,
                                                                         this->connectorPageSource->getFileBytesSize());


                this->driverContext->recordTableScanInfo(this->tableScanRecord);
            }

            size_t bytesRead = this->connectorPageSource->getBytesRead();
            this->tableScanRecord->updateScanedBytes(bytesRead);

            int remainingTuples = (this->tableScanRecord->getTotalTableBytes() - bytesRead)/this->tupleLen;
            this->driverContext->setRemainingTableTupleCount(remainingTuples);

            count++;
            page = std::make_shared<DataPage>(batch);

        }
        addSplitLock.unlock();
        this->checkShutDownAndSet();
        counter++;
        return page;

    }


    bool needsInput() override {
        return false;
    }

    void forceShutDownPipeline()
    {

        cout << std::this_thread::get_id() <<"forced to close £¡" << endl;
        this->forceShutDown = true;

    }
    void addSplits(shared_ptr<Session> session,Split split)
    {

        spdlog::debug("Source add splits!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        addSplitLock.lock();
        this->connectorPageSource = PSM->createPageSource(session,split);
        addSplitLock.unlock();

    }

    bool isFinished()
    {
        return this->finished;
    }

};




#endif //OLVP_TABLESCANOPERATOR_HPP
