//
// Created by zxk on 4/20/25.
//

#ifndef OLVP_INTERTASKARROWRPCCLIENT_HPP
#define OLVP_INTERTASKARROWRPCCLIENT_HPP



#include <arrow/api.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include <arrow/flight/server.h>
#include <arrow/flight/api.h>
#include <arrow/flight/client.h>
#include "nlohmann/json.hpp"

#include "../../Execution/Task/Id/TaskId.hpp"
#include "PageTransformer.hpp"

#include "InterTaskDataPageRPCBuffer.hpp"
class InterTaskArrowRPCClient
{
    string clientBufferIp;
    string clientBufferPort;
    string path;
    string taskId;
    string bufferId = "";
    string interTaskComponentId = "";
    string note;

    arrow::flight::Location location;
    std::unique_ptr<arrow::flight::FlightClient> client = NULL;

    mutex notelock;

public:
    InterTaskArrowRPCClient(string ip,string port){
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

    void setInterTaskCacheTarget(string taskIdInput,string componentId,string bufferId)
    {
        this->taskId = taskIdInput;
        this->interTaskComponentId = componentId;
        this->bufferId = bufferId;
    }

    arrow::Status getOnceBatches(InterTaskDataPageRPCBuffer &buffer,int dataSize,int *tagIn)
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


    string generateTicket(int dataSize)
    {
        string noteSend = this->getNote();

        nlohmann::json json;

        if(this->interTaskComponentId == "") {

            json["ticketType"] = "normal";
            json["taskId"] = this->taskId;
            json["bufferId"] = this->bufferId;
            json["pageNums"] = to_string(dataSize);

            if (note != "") {
                json["note"] = noteSend;
                spdlog::debug("Task " + this->taskId + "set note " + noteSend + "!");
                this->removeNote();
            }
        }
        else
        {
            json["ticketType"] = "interTask";
            json["taskId"] = this->taskId;
            json["componentId"] = this->interTaskComponentId;
            json["bufferId"] = this->bufferId;
            json["pageNums"] = to_string(dataSize);
        }

        return json.dump();
    }

    arrow::Status getOnceBatches(vector<shared_ptr<DataPage>> &pagesReturn,int dataSize,int *tagIn)
    {
        std::unique_ptr<arrow::flight::FlightStreamReader> stream;
        arrow::flight::Ticket ticket;

        if(bufferId.compare("") == 0) {
            cout << "taskId or BufferId empty!" << endl;
            return arrow::Status::Cancelled("ParamterError");
        }



        ticket.ticket = generateTicket(dataSize);

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


};


#endif //OLVP_INTERTASKARROWRPCCLIENT_HPP
