//
// Created by zxk on 6/21/23.
//

#ifndef OLVP_CSVONCETABLEREADER_HPP
#define OLVP_CSVONCETABLEREADER_HPP



//#include "../common.h"
#include "../Connector/ConnectorPageSource.hpp"
#include "../Utils/FileCommon.hpp"



#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
//#include "../Session/Session.hpp"
class Session;


using namespace std;
class CSVOnceTableReader:public ConnectorPageSource
{
    string tableFilePath;
    std::shared_ptr<arrow::RecordBatch> batch;
    std::shared_ptr<arrow::csv::TableReader> reader;
    size_t fileBytes;
    shared_ptr<Session> session;

    shared_ptr<arrow::Table> table;
    size_t bytes_read = 0;

    int nextSlice = 0;
    int batchSize;
    int numTuplesPerBatch = 0;
    int allSliceNum = 0;
    int tupleLength = 0;
    int allTupleNums = 0;

public:

    CSVOnceTableReader(shared_ptr<Session> session)
    {
        this->session = session;
    }

    bool loadTable(string path)
    {
        this->tableFilePath = path;
        arrow::io::IOContext io_context = arrow::io::default_io_context();
        std::shared_ptr<arrow::io::InputStream> input = arrow::io::ReadableFile::Open(tableFilePath).ValueOrDie();
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


        this->batchSize = atoi(this->session->getExecutionConfig()->getTableScanBatchSize().c_str());


        auto maybe_reader =
                arrow::csv::TableReader::Make(io_context,
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

        arrow::Result<shared_ptr<arrow::Table>> result = this->reader->Read();

        if(!result.ok())
        {
            spdlog::critical("read table faild!"+result.status().ToString());
            return false;
        }

        this->table = result.ValueOrDie();

        int tupleLength = 0;
        for(int i = 0 ; i < this->table->schema()->num_fields(); i++)
        {
            tupleLength+= this->table->schema()->field(i)->type()->byte_width();
        }
        this->numTuplesPerBatch = this->batchSize / tupleLength;
        this->allTupleNums = this->table->num_rows();
        this->allSliceNum = (this->allTupleNums - 1)/this->numTuplesPerBatch +1;
        this->tupleLength = tupleLength;
        this->fileBytes = this->allTupleNums;

        return true;

    }

    size_t getFileBytesSize()
    {
        return this->fileBytes;
    }
    size_t getBytesRead()
    {
        return this->bytes_read;
    }

    std::shared_ptr<arrow::RecordBatch> getNextBatch()
    {
        if(this->nextSlice >= this->allSliceNum)
            return NULL;

        if(!(this->nextSlice == (this->allSliceNum - 1)))
            this->batch = this->table->Slice(this->nextSlice * this->numTuplesPerBatch,this->numTuplesPerBatch)->CombineChunksToBatch().ValueOrDie();
        else
            this->batch = this->table->Slice(this->nextSlice * this->numTuplesPerBatch,this->table->num_rows() - this->nextSlice * this->numTuplesPerBatch)->CombineChunksToBatch().ValueOrDie();
        this->nextSlice++;


        this->bytes_read+= this->batch->num_rows();



        if (batch == NULL) {
            // Handle end of file
            return NULL;
        }
        spdlog::debug("Batch Read OK!");

        arrow::Result<shared_ptr<arrow::RecordBatch>> result = batch->RemoveColumn(batch->num_columns() - 1);
        this->batch = result.ValueOrDie();

        return this->batch;


    }





};




#endif //OLVP_CSVONCETABLEREADER_HPP
