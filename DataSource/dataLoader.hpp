//
// Created by zxk on 5/15/23.
//

#ifndef OLVP_DATALOADER_HPP
#define OLVP_DATALOADER_HPP

#include "arrow/csv/api.h"
#include <arrow/compute/api.h>
#include "arrow/pretty_print.h"
#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/json/api.h>
#include <arrow/io/api.h>
#include <arrow/table.h>
#include <arrow/pretty_print.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <memory>
#include <iostream>

using namespace std;
class dataLoader
{
public:
    dataLoader(){}

    void getBatch()
    {

        arrow::io::IOContext io_context = arrow::io::default_io_context();
        std::shared_ptr<arrow::io::InputStream> input = arrow::io::ReadableFile::Open("/home/zxk/Desktop/dataSet/supplier.tbl").ValueOrDie();


        auto read_options = arrow::csv::ReadOptions::Defaults();
        auto parse_options = arrow::csv::ParseOptions::Defaults();
        auto convert_options = arrow::csv::ConvertOptions::Defaults();
        parse_options.delimiter = '|';
        read_options.block_size = 1024;

        // Instantiate StreamingReader from input stream and options
        auto maybe_reader =
                arrow::csv::StreamingReader::Make(io_context,
                                                  input,
                                                  read_options,
                                                  parse_options,
                                                  convert_options);
        if (!maybe_reader.ok()) {
            // Handle StreamingReader instantiation error...
            std::cout << "read faild!"<<std::endl;
        }
        std::shared_ptr<arrow::csv::StreamingReader> reader = *maybe_reader;

        // Set aside a RecordBatch pointer for re-use while streaming
        std::shared_ptr<arrow::RecordBatch> batch;

        while (true) {
            // Attempt to read the first RecordBatch
            arrow::Status status = reader->ReadNext(&batch);


            if (!status.ok()) {
                // Handle read error

            }

            if (batch == NULL) {
                // Handle end of file
                break;
            }
            std::cout << "read OK!" <<std::endl;
            // Do something with the batch
            arrow::Result<shared_ptr<arrow::RecordBatch>> result = batch->RemoveColumn(batch->num_columns() - 1);
            batch = result.ValueOrDie();


            std::vector<std::string> names = batch->schema()->field_names();

            for(int i = 0 ; i < batch->schema()->num_fields() ; i++)
            {

                cout << batch->schema()->field(i)->type()->ToString() << endl;
            }

         }


    }




};


#endif //OLVP_DATALOADER_HPP
