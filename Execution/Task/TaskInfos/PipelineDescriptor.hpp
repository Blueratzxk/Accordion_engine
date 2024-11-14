//
// Created by zxk on 6/9/23.
//

#ifndef OLVP_PIPELINEDESCRIPTOR_HPP
#define OLVP_PIPELINEDESCRIPTOR_HPP

#include <string>
#include <vector>
#include <memory>
#include "nlohmann/json.hpp"
using namespace std;
class PipelineDescriptor
{
    string pipelineId;
    string pipelineType;
    int activePipelineDriverNumber;
    vector<string> pipelineTemplate;
    shared_ptr<TableScanRecord> tableScanRecord = NULL;
    bool scalable = false;

public:
    PipelineDescriptor(string pipelineId,string pipelineType,int activePipelineDriverNumber,vector<string> pipelineTemplate,bool isScalable)
    {
        this->pipelineId = pipelineId;
        this->pipelineType = pipelineType;
        this->activePipelineDriverNumber = activePipelineDriverNumber;
        this->pipelineTemplate = pipelineTemplate;
        this->scalable = isScalable;
    }

    PipelineDescriptor(string pipelineId,string pipelineType,int activePipelineDriverNumber,vector<string> pipelineTemplate,shared_ptr<TableScanRecord> tableScanRecord,bool isScalable)
    {
        this->pipelineId = pipelineId;
        this->pipelineType = pipelineType;
        this->activePipelineDriverNumber = activePipelineDriverNumber;
        this->pipelineTemplate = pipelineTemplate;
        this->tableScanRecord = tableScanRecord;
        this->scalable = isScalable;
    }

    string getPipelineId(){
        return this->pipelineId;
    }
    string getPipelineType(){
        return this->pipelineType;
    }
    bool isScalable()
    {
        return this->scalable;
    }
    int getActivePipelineDriverNumber(){
        return this->activePipelineDriverNumber;
    }
    vector<string> getPipelineTemplate(){
        return this->pipelineTemplate;
    }

    shared_ptr<TableScanRecord> getTableScanRecord()
    {
        return this->tableScanRecord;
    }


    string ToString()
    {
        string result;
        result.append("{");
        result.append("\"pipelineId\":");
        result.append("\""+this->pipelineId+"\"");
        result.append(",");
        result.append("\"pipelineType\":");
        result.append("\""+this->pipelineType+"\"");
        result.append(",");
        result.append("\"isScalable\":");
        result.append("\""+ to_string(this->scalable)+"\"");
        result.append(",");
        result.append("\"activePipelineDriverNumber\":");
        result.append("\""+ to_string(this->activePipelineDriverNumber)+"\"");
        result.append(",");
        result.append("\"pipelineFormat\":");

        string pipelineFormat;
        for(auto pipe : this->pipelineTemplate)
        {
            pipelineFormat += pipe;
            pipelineFormat += ",";
        }
        pipelineFormat.pop_back();
        result.append("\""+pipelineFormat+"\"");

        if(this->tableScanRecord != NULL) {
            result.append(",");
            result.append("\"tableScanRecord\":");
            result.append(this->tableScanRecord->ToString());
        }



        result.append("}");

        return result;
    }



    static string Serialize(PipelineDescriptor pipelineDescriptor)
    {
        nlohmann::json json;

        json["pipelineId"] = pipelineDescriptor.pipelineId;
        json["pipelineType"] = pipelineDescriptor.pipelineType;
        json["activePipelineDriverNumber"] = pipelineDescriptor.activePipelineDriverNumber;
        json["pipelineTemplate"] = pipelineDescriptor.pipelineTemplate;
        json["scalable"] = pipelineDescriptor.scalable;
        if(pipelineDescriptor.tableScanRecord != NULL)
        {
            json["tableScanRecord"] = TableScanRecord::Serialize(pipelineDescriptor.tableScanRecord);
        }


        string result = json.dump();
        return result;
    }

    static shared_ptr<PipelineDescriptor> Deserialize(string pipelineDescriptor)
    {
        nlohmann::json json = nlohmann::json::parse(pipelineDescriptor);

        if(json.contains("tableScanRecord"))
        {
            return make_shared<PipelineDescriptor>(json["pipelineId"],json["pipelineType"],json["activePipelineDriverNumber"],json["pipelineTemplate"],
                                                   TableScanRecord::Deserialize(json["tableScanRecord"]),json["scalable"]);
        }
        else
            return make_shared<PipelineDescriptor>(json["pipelineId"],json["pipelineType"],json["activePipelineDriverNumber"],json["pipelineTemplate"],json["scalable"]);
    }


};



#endif //OLVP_PIPELINEDESCRIPTOR_HPP
