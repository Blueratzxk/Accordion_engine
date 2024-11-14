//
// Created by zxk on 6/3/23.
//

#ifndef OLVP_ARROWRPCSERVER_HPP
#define OLVP_ARROWRPCSERVER_HPP


#include <arrow/io/api.h>

#include <arrow/api.h>
#include <arrow/result.h>
#include <arrow/status.h>



#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/type_fwd.h>

#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <arrow/flight/server.h>
#include <arrow/flight/api.h>
#include <arrow/flight/client.h>

#include "../../Config/WebConfig.hpp"
#include "../TaskServerInferface.hpp"
#include "PageTransformer.hpp"



class pageTransferService : public arrow::flight::FlightServerBase {
public:
    const arrow::flight::ActionType kActionDropDataset{"drop_dataset", "Delete a dataset."};

    pageTransferService() {}

    arrow::Status ListFlights(
            const arrow::flight::ServerCallContext &, const arrow::flight::Criteria *,
            std::unique_ptr<arrow::flight::FlightListing> *listings) override {

        return arrow::Status::OK();
    }

    arrow::Status GetFlightInfo(const arrow::flight::ServerCallContext &,
                                const arrow::flight::FlightDescriptor &descriptor,
                                std::unique_ptr<arrow::flight::FlightInfo> *info) override {


        cout << "GET INFO!" << endl;

        return arrow::Status::OK();
    }

    arrow::Status DoPut(const arrow::flight::ServerCallContext &,
                        std::unique_ptr<arrow::flight::FlightMessageReader> reader,
                        std::unique_ptr<arrow::flight::FlightMetadataWriter> writer) override {

        cout << "!!@@!#@!#@!#@#" << endl;
        ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> table, reader->ToTable());
        return arrow::Status::OK();
    }

    arrow::Status DoGet(const arrow::flight::ServerCallContext &,
                        const arrow::flight::Ticket &request,
                        std::unique_ptr<arrow::flight::FlightDataStream> *stream) override {


        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

        string taskId, bufferId,note;
        getTaskBufferInfo(request.ticket, taskId, bufferId,note);
        int pageNums = getPageNums(request.ticket);

        if(note != "")
        {
            TaskServerInterFace::triggerTaskBufferNoteEvent(taskId,bufferId,note);
        }

        DataPageToArrowTable d2a;
        if(pageNums > 0)
        {
            vector<shared_ptr<DataPage>> pages;
            pages = TaskServerInterFace::getTaskResults(taskId,bufferId,pageNums);


            batches = d2a.ToBatches(pages);
        }

        spdlog::debug("Get page from task "+taskId+"---->"+bufferId+".Got " +to_string(batches[0]->num_columns())+" Pages");


        //  cout << "$$" << d2a.getSchema()->num_fields() << endl;

        ARROW_ASSIGN_OR_RAISE(auto owning_reader, arrow::RecordBatchReader::Make(
                std::move(batches), d2a.getSchema()));
        *stream = std::unique_ptr<arrow::flight::FlightDataStream>(
                new arrow::flight::RecordBatchStream(owning_reader));



        return arrow::Status::OK();
    }

    arrow::Status ListActions(const arrow::flight::ServerCallContext &,
                              std::vector<arrow::flight::ActionType> *actions) override {
        *actions = {kActionDropDataset};
        return arrow::Status::OK();
    }

    arrow::Status DoAction(const arrow::flight::ServerCallContext &,
                           const arrow::flight::Action &action,
                           std::unique_ptr<arrow::flight::ResultStream> *result) override {


        *result = std::unique_ptr<arrow::flight::ResultStream>(
                new arrow::flight::SimpleResultStream({}));
        return arrow::Status::NotImplemented("Unknown action type: ", action.type);
    }

private:

    void getTaskBufferInfo(string ticket, string &taskId, string &bufferId,string &note) {
        nlohmann::json info = nlohmann::json::parse(ticket);
        taskId = info["taskId"];
        bufferId = info["bufferId"];
        if(info.contains("note"))
            note = info["note"];
        else
            note = "";



    }
    int getPageNums(string ticket) {
        nlohmann::json info = nlohmann::json::parse(ticket);
        if(info.find("pageNums")!= info.end())
        {
            string pageNums = info["pageNums"];

            return atoi(pageNums.c_str());
        }
        else
            return -1;

    }


};  // end ParquetStorageService

Status StartArrowRPCServer(shared_ptr<arrow::flight::FlightServerBase> *serv) {

    WebConfig config;

    arrow::flight::Location server_location;
    ARROW_ASSIGN_OR_RAISE(server_location,
                          arrow::flight::Location::ForGrpcTcp(config.getRPCServerIp(),
                                                              atoi(config.getRPCServerPort().c_str())));

    arrow::flight::FlightServerOptions options(server_location);
    auto server = std::shared_ptr<arrow::flight::FlightServerBase>(
            new pageTransferService());
    ARROW_RETURN_NOT_OK(server->Init(options));

    thread process([server] {



        string info;
        info += "Arrow RPC Running.";
        info += " Listening on port " + to_string(server->port()) + ".";


        spdlog::info(info);
        ARROW_RETURN_NOT_OK(server->Serve());


        return arrow::Status::OK();
    });
    process.detach();

    (*serv) = server;

    return arrow::Status::OK();

}



#endif //OLVP_ARROWRPCSERVER_HPP
