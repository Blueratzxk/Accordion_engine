//
// Created by zxk on 4/20/25.
//

#ifndef OLVP_INTERTASKMISSIONDESCRIPTOR_HPP
#define OLVP_INTERTASKMISSIONDESCRIPTOR_HPP


class InterTaskSourceDescriptor
{
    string source_ip;
    string source_port;
    string sourceId;
    string bufferId;

    string destinationIdOnNewTask;
public:
    InterTaskSourceDescriptor(){}
    InterTaskSourceDescriptor(string source_ip,string source_port,string sourceId,string bufferId,string destinationIdOnNewTask){
        this->source_ip = source_ip;
        this->source_port = source_port;
        this->sourceId = sourceId;
        this->bufferId = bufferId;
        this->destinationIdOnNewTask = destinationIdOnNewTask;
    }

    string getInterSource_ip(){return this->source_ip;}
    string getInterSource_port(){return this->source_port;}
    string getInterSourceId(){return this->sourceId;}
    string getBufferId(){return this->bufferId;}
    string getDestinationIdOnNewTask(){return this->destinationIdOnNewTask;}

    static string Serialize(InterTaskSourceDescriptor interTaskSourceDescriptor)
    {
        nlohmann::json json;

        json["source_ip"] = interTaskSourceDescriptor.source_ip;
        json["source_port"] = interTaskSourceDescriptor.source_port;
        json["sourceId"] = interTaskSourceDescriptor.sourceId;
        json["bufferId"] = interTaskSourceDescriptor.bufferId;
        json["destinationIdOnNewTask"] = interTaskSourceDescriptor.destinationIdOnNewTask;


        string result = json.dump();

        return result;
    }

    static InterTaskSourceDescriptor Deserialize(string interTaskSourceDescriptor)
    {
        nlohmann::json json = nlohmann::json::parse(interTaskSourceDescriptor);
        return InterTaskSourceDescriptor(json["source_ip"],json["source_port"],json["sourceId"],json["bufferId"],json["destinationIdOnNewTask"]);
    }

};

class InterTaskMissionDescriptor
{

public:
    enum MissionType
    {
        OPERATOR_MIGRATION,
        INTERTASK_EXCHANGE_SERVICE
    };

private:

    set<string> sourceTypes;
    MissionType missionType;

    InterTaskSourceDescriptor interTaskSourceDescriptor;
public:
    InterTaskMissionDescriptor(MissionType missionType,set<string> sourceTypes,InterTaskSourceDescriptor interTaskSourceDescriptor){
        this->missionType = missionType;
        this->sourceTypes = sourceTypes;
        this->interTaskSourceDescriptor = interTaskSourceDescriptor;
    }

    InterTaskMissionDescriptor(MissionType missionType,set<string> sourceTypes){
        this->missionType = missionType;
        this->sourceTypes = sourceTypes;
    }
    InterTaskMissionDescriptor(MissionType missionType,InterTaskSourceDescriptor interTaskSourceDescriptor){
        this->missionType = missionType;
        this->interTaskSourceDescriptor = interTaskSourceDescriptor;
    }



    MissionType getMissionType(){return this->missionType;}
    set<string> getSourceTypes(){return this->sourceTypes;}
    InterTaskSourceDescriptor getInterTaskSourceDescriptor(){return this->interTaskSourceDescriptor;}

    static string Serialize(InterTaskMissionDescriptor interTaskMissionDescriptor) {

        nlohmann::json json;

        json["missionType"] = interTaskMissionDescriptor.missionType;
        json["sourceTypes"] = interTaskMissionDescriptor.sourceTypes;
        json["interTaskSourceDescriptor"] = InterTaskSourceDescriptor::Serialize(interTaskMissionDescriptor.interTaskSourceDescriptor);

        string result = json.dump();
        return result;

    }
    static shared_ptr<InterTaskMissionDescriptor> Deserialize(string interTaskMissionDescriptor)
    {
        nlohmann::json json = nlohmann::json::parse(interTaskMissionDescriptor);

        return make_shared<InterTaskMissionDescriptor>(json["missionType"],json["sourceTypes"],InterTaskSourceDescriptor::Deserialize(json["interTaskSourceDescriptor"]));
    }


};



#endif //OLVP_INTERTASKMISSIONDESCRIPTOR_HPP
