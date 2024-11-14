//
// Created by zxk on 6/4/23.
//

#ifndef OLVP_ARROWRPCCLIENT_HPP
#define OLVP_ARROWRPCCLIENT_HPP


#include <arrow/api.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include <arrow/flight/server.h>
#include <arrow/flight/api.h>
#include <arrow/flight/client.h>
#include "nlohmann/json.hpp"

#include "../../Execution/Task/Id/TaskId.hpp"
#include "PageTransformer.hpp"
#include "DataPageRPCBuffer.hpp"
class ArrowRPCClient
{
    string clientBufferIp;
    string clientBufferPort;
    string path;
    string taskId;
    string bufferId = "";
    string note;

    arrow::flight::Location location;
    std::unique_ptr<arrow::flight::FlightClient> client = NULL;

    mutex notelock;

public:
    ArrowRPCClient(string ip,string port){
        this->clientBufferIp = ip;
        this->clientBufferPort = port;
        this->note = "";
    }



    arrow::Status connect()
    {
        if(this->client != NULL)
            return arrow::Status::OK();




        // callOptions
        ARROW_ASSIGN_OR_RAISE(this->location,arrow::flight::Location::ForGrpcTcp(this->clientBufferIp,atoi(this->clientBufferPort.c_str())));
        ARROW_ASSIGN_OR_RAISE(this->client, arrow::flight::FlightClient::Connect(location));



    //    cout << "Connected to " << location.ToString() << std::endl;
        return arrow::Status::OK();
    }
    arrow::Status close()
    {
        arrow::Status result = this->client->Close();
        this->client = NULL;
        return result;
    }


    void setNote(string newNote)
    {
        notelock.lock();
        this->note = newNote;
        notelock.unlock();
    }
    void removeNote()
    {
        notelock.lock();
        this->note = "";
        notelock.unlock();
    }
    string getNote()
    {
        string n;
        notelock.lock();
        n = this->note;
        notelock.unlock();
        return n;
    }


    void setBufferTarget(string taskIdInput,string bufferIdInput,string token)
    {
        this->taskId = taskIdInput;
        this->bufferId = bufferIdInput;
    }

    arrow::Status getOnceBatches(DataPageRPCBuffer &buffer,int dataSize,int *tagIn)
    {
        std::unique_ptr<arrow::flight::FlightStreamReader> stream;
        arrow::flight::Ticket ticket;

        if(bufferId.compare("") == 0) {
            cout << "taskId or BufferId empty!" << endl;
            return arrow::Status::Cancelled("ParamterError");
        }

        string noteSend = this->getNote();

        nlohmann::json json;
        json["taskId"] = this->taskId;
        json["bufferId"] = this->bufferId;
        json["pageNums"] = to_string(dataSize);

        if(note != "")
        {
            json["note"] = noteSend;
            spdlog::debug("Task "+ this->taskId + "set note "+noteSend + "!");
            this->removeNote();
        }

        ticket.ticket = json.dump();

        arrow::Status  status = this->connect();


        auto clientOptions = arrow::flight::FlightClientOptions::Defaults();
        auto callOptions = arrow::flight::FlightCallOptions();


        ARROW_ASSIGN_OR_RAISE(stream, this->client->DoGet(callOptions,ticket));
        arrow::Status statusClose = this->close();


        vector<std::shared_ptr<arrow::RecordBatch>> batches;
        ARROW_ASSIGN_OR_RAISE(batches, stream->ToRecordBatches());

        ArrowTableToDataPage a2d;
        int tag = 0;
        vector<shared_ptr<DataPage>> pages = a2d.ToPages(batches,&tag);
        *tagIn = tag;

        if(tag == 2)
        {
            spdlog::debug("Get end page from "+this->taskId+"_"+this->bufferId+"\n");
        }

        buffer.enqueuePages(pages);
        return arrow::Status::OK();
    }

    arrow::Status getOnceBatches(vector<shared_ptr<DataPage>> &pagesReturn,int dataSize,int *tagIn)
    {
        std::unique_ptr<arrow::flight::FlightStreamReader> stream;
        arrow::flight::Ticket ticket;

        if(bufferId.compare("") == 0) {
            cout << "taskId or BufferId empty!" << endl;
            return arrow::Status::Cancelled("ParamterError");
        }

        string noteSend = this->getNote();

        nlohmann::json json;
        json["taskId"] = this->taskId;
        json["bufferId"] = this->bufferId;
        json["pageNums"] = to_string(dataSize);

        if(note != "")
        {
            json["note"] = noteSend;
            spdlog::debug("Task "+ this->taskId + "set note "+noteSend + "!");
            this->removeNote();
        }

        ticket.ticket = json.dump();

        arrow::Status  status = this->connect();


        auto clientOptions = arrow::flight::FlightClientOptions::Defaults();
        auto callOptions = arrow::flight::FlightCallOptions();


        ARROW_ASSIGN_OR_RAISE(stream, this->client->DoGet(callOptions,ticket));
        arrow::Status statusClose = this->close();


        vector<std::shared_ptr<arrow::RecordBatch>> batches;
        ARROW_ASSIGN_OR_RAISE(batches, stream->ToRecordBatches());

        ArrowTableToDataPage a2d;
        int tag = 0;
        vector<shared_ptr<DataPage>> pages = a2d.ToPages(batches,&tag);
        *tagIn = tag;

        if(tag == 2)
        {
            spdlog::debug("Get end page from "+this->taskId+"_"+this->bufferId+"\n");
        }

        pagesReturn = pages;
        return arrow::Status::OK();
    }

    arrow::Status getAllBatchesWithCircle(DataPageRPCBuffer &buffer)
    {

        std::unique_ptr<arrow::flight::FlightStreamReader> stream;
        arrow::flight::Ticket ticket;

        if( bufferId.compare("") == 0)
            return arrow::Status::Cancelled("ParamterError");


        nlohmann::json json;
        json["taskId"] = this->taskId;
        json["bufferId"] = this->bufferId;
        json["pageNums"] = 10000;
        ticket.ticket = json.dump();



        bool end = false;

        do {

            arrow::Status  status = this->connect();

            ARROW_ASSIGN_OR_RAISE(stream, client->DoGet(ticket));
            arrow::Status  statusClose = this->close();
            vector<std::shared_ptr<arrow::RecordBatch>> batches;
            ARROW_ASSIGN_OR_RAISE(batches, stream->ToRecordBatches());

            ArrowTableToDataPage a2d;
            int tag;
            vector<shared_ptr<DataPage>> results = a2d.ToPages(batches,&tag);

            //cout << this->taskId << "get page! "<<results.size()<<endl;
            if(results.size() > 0)
            {
                cout << this->taskId << "get page! "<<results.size()<<"###"<< results[0]->getElementsCount() << "@@@@@@@@@@@"<<endl;
            }

            buffer.enqueuePages(results);


            for (int i = 0; i < results.size(); i++) {
                if(results[i]->isEndPage())
                    end = true;
            }

        }while(!end);



        return arrow::Status::OK();
    }



};


#endif //OLVP_ARROWRPCCLIENT_HPP
