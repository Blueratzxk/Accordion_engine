//
// Created by zxk on 5/15/23.
//


#ifndef OLVP_CSVTABLEREADER_HPP
#define OLVP_CSVTABLEREADER_HPP

//#include "../common.h"
#include "../Connector/ConnectorPageSource.hpp"
#include "../Utils/FileCommon.hpp"



#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include "../Session/Session.hpp"
//#include "../Session/RuntimeConfigParser.hpp"


using namespace std;
class CSVTableReader:public ConnectorPageSource
{
    string tableFilePath;
    std::shared_ptr<arrow::RecordBatch> batch;
    std::shared_ptr<arrow::csv::StreamingReader> reader;
    size_t fileBytes;
    shared_ptr<Session> session;
    int rowCount = 0;


    std::shared_ptr<arrow::io::InputStream> input;
    arrow::io::IOContext io_context;

    string scanBlockSize = "-1";
public:

    CSVTableReader(shared_ptr<Session> session) : ConnectorPageSource("CSVTableReader")
    {
        this->session = session;
    }

    CSVTableReader(shared_ptr<Session> session,string blockSize) : ConnectorPageSource("CSVTableReader")
    {
        this->session = session;
        this->scanBlockSize = blockSize;
    }

    bool loadTable(string path)
    {
        this->tableFilePath = path;
        io_context = arrow::io::default_io_context();
        input = arrow::io::ReadableFile::Open(tableFilePath).ValueOrDie();
        this->fileBytes = FileCommon::getFileSize(tableFilePath.c_str());


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



        int tempSize = atoi(this->scanBlockSize.c_str());
        if(tempSize > 0)
            read_options.block_size = tempSize;
        else
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

        return true;

    }


    size_t getFileBytesSize()
    {
        return this->fileBytes;
    }
    size_t getBytesRead()
    {
        return this->reader->bytes_read();
    }

    std::shared_ptr<arrow::RecordBatch> getNextBatch()
    {

        arrow::Status status = this->reader->ReadNext(&batch);


        if (!status.ok()) {
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
        this->rowCount+=this->batch->num_rows();

        spdlog::debug("Rows has been read:"+ to_string(this->rowCount));



        return this->batch;


    }


    vector<std::shared_ptr<arrow::RecordBatch>> getNextBatchs(int batchCount)
    {
        vector<std::shared_ptr<arrow::RecordBatch>> batches;
        for(int i = 0 ; i < batchCount ; i++) {
            arrow::Status status = this->reader->ReadNext(&batch);


            if (!status.ok()) {
                spdlog::critical("Get Next Batch Failed!");
                return {};
            }

            if (batch == NULL) {
                // Handle end of file
                return batches;
            }
            spdlog::debug("Batch Read OK!");

            arrow::Result<shared_ptr<arrow::RecordBatch>> result = batch->RemoveColumn(batch->num_columns() - 1);
            this->batch = result.ValueOrDie();

            batches.push_back(this->batch);
        }

    }




};

#endif //OLVP_CSVTABLEREADER_HPP
