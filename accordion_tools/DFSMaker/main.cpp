#include <sys/stat.h>
#include <iostream>

#include <fstream>

#include <arrow/csv/api.h>
#include <arrow/csv/writer.h>
#include <arrow/ipc/writer.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <sstream>
#include "spdlog/spdlog.h"
#include "nlohmann/json.hpp"

using namespace std;


class FileCommon
{
public:

    static size_t getFileSize(const char *fileName) {

        if (fileName == NULL) {
            return 0;
        }
        // 这是一个存储文件(夹)信息的结构体，其中有文件大小和创建时间、访问时间、修改时间等
        struct stat statbuf;
        // 提供文件名字符串，获得文件属性结构体
        stat(fileName, &statbuf);
        // 获取文件大小
        size_t filesize = statbuf.st_size;
        return filesize;
    }


};



class CSVTableReader
{
    string tableFilePath;
    std::shared_ptr<arrow::RecordBatch> batch;
    std::shared_ptr<arrow::csv::StreamingReader> reader;
    size_t fileBytes;

    int rowCount = 0;

    int partitionNumber = 0;
    int pc = 0;

    std::shared_ptr<arrow::io::InputStream> input;
    arrow::io::IOContext io_context;
public:

    CSVTableReader()
    {

    }

    bool loadTable(string path,int partitionCount)
    {
        this->tableFilePath = path;
        io_context = arrow::io::default_io_context();
        input = arrow::io::ReadableFile::Open(tableFilePath).ValueOrDie();
        this->fileBytes = FileCommon::getFileSize(tableFilePath.c_str());

        this->pc = partitionCount;

        if(!input)
        {
            spdlog::critical(arrow::io::ReadableFile::Open(tableFilePath).status().ToString());
            return false;
        }

        auto read_options = arrow::csv::ReadOptions::Defaults();
        read_options.use_threads = true;
        auto parse_options = arrow::csv::ParseOptions::Defaults();
        auto convert_options = arrow::csv::ConvertOptions::Defaults();
        parse_options.delimiter = '|';
        read_options.block_size = this->fileBytes/(partitionCount);


        auto maybe_reader =
                arrow::csv::StreamingReader::Make(io_context,
                                                  input,
                                                  read_options,
                                                  parse_options,
                                                  convert_options);
        if (!maybe_reader.ok()) {
            spdlog::critical(maybe_reader.status().ToString());
            return false;
        }
        this->reader = *maybe_reader;

        return true;

    }

    void writePartition()
    {

        std::shared_ptr<arrow::io::OutputStream> output = arrow::io::FileOutputStream::Open(this->tableFilePath+"_"+
                                                                                            to_string(this->partitionNumber)).ValueOrDie();
        auto write_options = arrow::csv::WriteOptions::Defaults();
        write_options.delimiter = '|';
        write_options.quoting_style = arrow::csv::QuotingStyle::None;

        auto maybe_writer = arrow::csv::MakeCSVWriter(output, this->batch->schema(), write_options);
        if (!maybe_writer.ok()) {
            // Handle writer instantiation error...
        }
        std::shared_ptr<arrow::ipc::RecordBatchWriter> writer = *maybe_writer;

        // Write batches...

        if (!writer->WriteRecordBatch(*batch).ok()) {
            spdlog::debug("Write Error!");
        }

        if (!writer->Close().ok()) {
            // Handle close error...
        }
        if (!output->Close().ok()) {
            // Handle file close error...
        }
    }

    void writePartitions(vector<std::shared_ptr<arrow::RecordBatch>> batches)
    {

        std::shared_ptr<arrow::io::OutputStream> output = arrow::io::FileOutputStream::Open(this->tableFilePath+"_"+
                                                                                            to_string(this->partitionNumber)).ValueOrDie();
        auto write_options = arrow::csv::WriteOptions::Defaults();
        write_options.delimiter = '|';
        write_options.quoting_style = arrow::csv::QuotingStyle::None;

        auto maybe_writer = arrow::csv::MakeCSVWriter(output, this->batch->schema(), write_options);
        if (!maybe_writer.ok()) {
            // Handle writer instantiation error...
        }
        std::shared_ptr<arrow::ipc::RecordBatchWriter> writer = *maybe_writer;

        // Write batches...

        for(int i = 0 ; i < batches.size() ; i++) {
            if (!writer->WriteRecordBatch(*batches[i]).ok()) {
                spdlog::debug("Write Error!");
            }
        }

        if (!writer->Close().ok()) {
            // Handle close error...
        }
        if (!output->Close().ok()) {
            // Handle file close error...
        }
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

            return NULL;
        }

        if (batch == NULL) {
            // Handle end of file
            return NULL;
        }
        spdlog::debug("Batch Read OK!");




        if(this->partitionNumber == this->pc - 1)
        {
            std::shared_ptr<arrow::RecordBatch> lastBatch;
            arrow::Status status = this->reader->ReadNext(&lastBatch);

            if (!status.ok()) {

                return NULL;
            }

            if(lastBatch != NULL)
            {
                writePartitions({this->batch,lastBatch});
                return lastBatch;
            }
            else
            {
                writePartitions({this->batch});
                return this->batch;
            }

        }
        else
            writePartition();

        this->partitionNumber++;

        arrow::Result<shared_ptr<arrow::RecordBatch>> result = batch->RemoveColumn(batch->num_columns() - 1);
        this->batch = result.ValueOrDie();
        this->rowCount+=this->batch->num_rows();

        spdlog::debug("Rows has been read:"+ to_string(this->rowCount));



        return this->batch;


    }




};


class TableInfosAnalyzer
{

    shared_ptr<std::ifstream> jfile;


    map<string,shared_ptr<vector<pair<string,string>>>> table2Splits;

public:
    TableInfosAnalyzer(string filename){


        jfile = make_shared<ifstream>(filename);

        if(!jfile->is_open())
        {
            spdlog::error("Cannot opern file DataFileDicts!");
            exit(0);
        }

    }
    TableInfosAnalyzer(){

    }


    void Stringsplit(std::string str, const char split,std::vector<std::string>& res)
    {
        std::istringstream iss(str);	// 输入流
        std::string token;			// 接收缓冲区
        while (getline(iss, token, split))	// 以split为分隔符
        {
            res.push_back(token);
        }
    }

    void analyze()
    {
        nlohmann::json j;
        *jfile >> j;

        nlohmann::json  js = nlohmann::json::array();
        js = j["tpch_test"]["tables"];

        for(auto jj : js)
        {
            string tableName = jj["tableName"];

            nlohmann::json splits = nlohmann::json::array();
            splits = jj["distributedFilePaths"];

            map<string,string> splitInfos;

            if(table2Splits.find(tableName) == table2Splits.end())
            {
                this->table2Splits[tableName] = make_shared<vector<pair<string,string>>>();
            }

            for(auto split : splits)
            {
                this->table2Splits[tableName]->push_back({split["netAddr"],split["fileAddr"]});
            }

        }
    }

    void generatePartitions() {

        for (auto table : this->table2Splits)
        {

            if(table.second->size() <= 1)
                continue;

            string originFileName = table.first;
            originFileName += ".tbl";

            CSVTableReader reader;


            int i = 0 ;
            if(reader.loadTable(originFileName,table.second->size()))
            {
                while (reader.getNextBatch() != NULL){
                    spdlog::info("Generate "+originFileName+"_"+ to_string(i));
                    i++;
                }

            }
            else
            {
                spdlog::critical("load Table ERROR!");
            }
            spdlog::info("Table "+originFileName+" partitions generated!");
        }
    }

    void customSplit(string str, char separator,vector<string> &strings) {
        int startIndex = 0, endIndex = 0;
        for (int i = 0; i <= str.size(); i++) {

            // If we reached the end of the word or the end of the input.
            if (str[i] == separator || i == str.size()) {
                endIndex = i;
                string temp;
                temp.append(str, startIndex, endIndex - startIndex);
                strings.push_back(temp);
                startIndex = endIndex + 1;
            }
        }
    }

    void sendAllPartitions(string prefix,string u,string p)
    {
        ofstream of("makeDFS.sh");
        of<<"#!/bin/bash"<<endl;
        for (auto table : this->table2Splits)
        {
            string originFileName = table.first;
            originFileName += ".tbl";


            for(int i = 0 ; i < table.second->size();i++)
            {
                string splitName;
                if(table.second->size() == 1)
                    splitName = originFileName;
                else
                    splitName = originFileName+"_"+to_string(i);
                string user = u;

                string destAddr = (*table.second)[i].first;
                vector<string> strings;
                customSplit(destAddr,':',strings);

                string destIP = strings[0];
                string destPath = (*table.second)[i].second;

                if(prefix != "")
                {
                    destPath = prefix+destPath;
                }

                string passwd = p;

                of << "bash scpFile.sh "+splitName+" "+user+" "+destIP+" "+destPath+" "+passwd << endl;
            }



        }

    }


    //table, node nums,partition nums per node
    std::map<string,pair<int,int>> tableMaps ={
            {"nation",{1,1}},
            {"supplier",{1,1}},
            {"region",{1,1}},
            {"part",{1,1}},
            {"partsupp",{4,1}},
            {"customer",{4,1}},
            {"orders",{4,1}},
            {"lineitem",{4,2}}

    };


    nlohmann::json genPartitionsForTable(nlohmann::json &table,vector<string> slaves)
    {
        string tableName = table["tableName"];

        int nodesNumForTable = tableMaps[tableName].first;
        int partitionNumForTable = tableMaps[tableName].second;

        nlohmann::json paths = nlohmann::json::array();

        int tableNumber = 0;

        if(nodesNumForTable == 1 && partitionNumForTable == 1)
        {
            nlohmann::json info;
            info["netAddr"] = slaves[0]+":9080";
            info["fileAddr"] = "data/"+tableName+".tbl";
            paths.push_back(info);
        }
        else {
            for (int i = 0; i < nodesNumForTable; i++) {
                for (int j = 0; j < partitionNumForTable; j++) {
                    nlohmann::json info;
                    info["netAddr"] = slaves[i] + ":9080";
                    info["fileAddr"] = "data/" + tableName + ".tbl_" + to_string(tableNumber);
                    tableNumber++;
                    paths.push_back(info);
                }

            }
        }

        table["distributedFilePaths"] = paths;

        return table;

    }





    void assignNodesforDataDictFile(string dataDictPath,string slavesPath,string tablePmapPath)
    {

        ifstream tablemap(tablePmapPath);
        if(tablemap.is_open())
        {
            string tmap;

            while(getline(tablemap,tmap))
            { vector<string> res;
                Stringsplit(tmap,',',res);

                if(tableMaps.count(res[0]) > 0)
                {
                    auto p = make_pair<int,int>(atoi(res[1].c_str()),atoi(res[2].c_str()));

                    cout << p.first <<"-"<<p.second << endl;
                    tableMaps[res[0]] = p;
                }

            }

        }
        else
        {
            cout << "No map file!"<<endl;
        }

        nlohmann::json datadict;
        ifstream jfile(dataDictPath);
        jfile >> datadict;


        ifstream slaves(slavesPath);
        vector<string> slaveIps;

        string read;
        while(getline(slaves,read))
        {
            slaveIps.push_back(read);
        }



        auto tableArrayNew = nlohmann::json::array();
        for(auto &item : datadict.items())
        {
            for(int i = 0 ; i <  (item.value())["tables"].size() ;i++) {
                genPartitionsForTable((item.value())["tables"][i],slaveIps);
            }
        }


        ofstream output("DataFileDicts.out");
        output << datadict.dump(1);

    }



    void makeDFSFile(string prefix,string u,string p)
    {
        ofstream of("makeDFS.sh");
        of<<"#!/bin/bash"<<endl;
        for (auto table : this->table2Splits)
        {
            string originFileName = table.first;
            originFileName += ".tbl";


            for(int i = 0 ; i < table.second->size();i++)
            {
                string splitName;
                if(table.second->size() == 1)
                    splitName = originFileName;
                else
                    splitName = originFileName+"_"+to_string(i);
                string user = u;

                string destAddr = (*table.second)[i].first;
                vector<string> strings;
                customSplit(destAddr,':',strings);

                string destIP = strings[0];
                string destPath = (*table.second)[i].second;

                if(prefix != "")
                {
                    destPath = prefix+destPath;
                }

                string passwd = p;

                of << "bash scpFile.sh "+splitName+" "+user+" "+destIP+" "+destPath+" "+passwd << endl;
            }



        }

    }




};


int main(int argc, char* argv[]) {



    string fileName = string(argv[1]);
    string user = string(argv[2]);
    string passwd = string(argv[3]);

    string prefix = "";
    if(argc == 5)
        prefix = string(argv[4]);

    if(fileName.size() == 0)
    {
        spdlog::error("need file name!");
        exit(0);
    }

    TableInfosAnalyzer tableInfosAnalyzer(fileName);
    tableInfosAnalyzer.analyze();
    tableInfosAnalyzer.generatePartitions();
    tableInfosAnalyzer.sendAllPartitions(prefix,user,passwd);




    return 0;
}
