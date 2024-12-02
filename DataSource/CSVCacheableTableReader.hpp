//
// Created by zxk on 6/21/23.
//

#ifndef OLVP_CSVCACHEABLETABLEREADER_HPP
#define OLVP_CSVCACHEABLETABLEREADER_HPP



//#include "../common.h"
#include "../Connector/ConnectorPageSource.hpp"
#include "../Utils/FileCommon.hpp"



#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
//#include "../Session/Session.hpp"
#include "../Utils/RW_BlockQueue.hpp"

class Session;

using namespace std;
class CSVCacheableTableReader:public ConnectorPageSource
{
    string tableFilePath;
    std::shared_ptr<arrow::RecordBatch> batch;
    std::shared_ptr<arrow::csv::StreamingReader> reader;
    size_t fileBytes;
    shared_ptr<Session> session;

    shared_ptr<RW_BlockQueue<shared_ptr<arrow::RecordBatch>>> blocks;

    atomic<size_t> bytesRead = 0;

public:

    CSVCacheableTableReader(shared_ptr<Session> session) : ConnectorPageSource("CSVCacheableTableReader")
    {
        this->session = session;
    }

    static void asyncReader(CSVCacheableTableReader *csvCacheableTableReader)
    {
        while(true) {
            shared_ptr<arrow::RecordBatch> batch;
            arrow::Status status = csvCacheableTableReader->reader->ReadNext(&batch);
            if (!status.ok()) {
                spdlog::critical("Get Next Batch Failed!");
                return;
            }
            csvCacheableTableReader->blocks->push_back(batch);


            if(batch == NULL)
                break;
        }
    }


    bool loadTable(string path)
    {
        this->tableFilePath = path;
        arrow::io::IOContext io_context = arrow::io::default_io_context();
        std::shared_ptr<arrow::io::InputStream> input = arrow::io::ReadableFile::Open(tableFilePath).ValueOrDie();
        this->fileBytes = FileCommon::getFileSize(tableFilePath.c_str());

        this->blocks = make_shared<RW_BlockQueue<shared_ptr<arrow::RecordBatch>>>(100);



        if(!input)
        {
            spdlog::critical("CSVTableReader load table ERROR");
            return false;
        }

        auto read_options = arrow::csv::ReadOptions::Defaults();
        read_options.use_threads = true;
        auto parse_options = arrow::csv::ParseOptions::Defaults();
        auto convert_options = arrow::csv::ConvertOptions::Defaults();
        parse_options.delimiter = '|';
        read_options.block_size = atoi(this->session->getExecutionConfig()->getTableScanBatchSize().c_str());


        auto maybe_reader =
                arrow::csv::StreamingReader::Make(io_context,
                                                  input,
                                                  read_options,
                                                  parse_options,
                                                  convert_options);
        if (!maybe_reader.ok()) {
            // Handle StreamingReader instantiation error...
            spdlog::critical("read faild! Handle StreamingReader instantiation error...");
            return false;
        }
        this->reader = *maybe_reader;

        thread(asyncReader,this).detach();

        return true;

    }

    size_t getFileBytesSize()
    {
        return this->fileBytes;
    }
    size_t getBytesRead()
    {
        return this->bytesRead;
    }

    std::shared_ptr<arrow::RecordBatch> getNextBatch()
    {


        if (!this->blocks->pop(this->batch)) {
            spdlog::critical("Get Next Batch Failed!");
            return NULL;
        }

        if (batch == NULL) {
            // Handle end of file
            return NULL;
        }
        spdlog::debug("Batch Read OK!");

        arrow::Result<shared_ptr<arrow::RecordBatch>> result = batch->RemoveColumn(batch->num_columns() - 1);
        this->batch = result.ValueOrDie();

        int tupleSize = 0;
        int batchSize = 0;
        for(int i = 0 ; i < this->batch->schema()->num_fields() ; i++)
        {
            tupleSize += this->batch->schema()->field(i)->type()->byte_width();
        }
        batchSize = tupleSize * this->batch->num_rows();

        this->bytesRead += batchSize;



        return this->batch;


    }




};



#endif //OLVP_CSVCACHEABLETABLEREADER_HPP
