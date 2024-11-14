//
// Created by zxk on 5/31/23.
//

#ifndef OLVP_PARTITIONINGHANDLESERIALIZER_HPP
#define OLVP_PARTITIONINGHANDLESERIALIZER_HPP



#include "SystemPartitioningHandle.h"
#include "nlohmann/json.hpp"

class PartitioningHandleSerializer
{
public:
    PartitioningHandleSerializer()
    {

    }

    static string Serialize(shared_ptr<PartitioningHandle> handle)
    {
        if(handle == NULL)
            return "NULL";
        else
            return handle->Serialize(*handle);
    }

    static shared_ptr<PartitioningHandle> Deserialize(string handle)
    {
        if(handle == "NULL")
            return NULL;

        nlohmann::json han = nlohmann::json::parse(handle);
        string handleId = han["handleId"];
        string connectorId = han["connectorId"];
        string con = han["connectorPartitioningHandle"];
        if(handleId.compare("SystemPartitioningHandle") == 0)
        {
            SystemPartitioningHandle sys;
            return make_shared<PartitioningHandle>(connectorId,sys.Deserialize(con));
        }
        else {
            cout << "deserialize cannot find partitioning Handle!" << endl;
            exit(0);
        }
    }



};


#endif //OLVP_PARTITIONINGHANDLESERIALIZER_HPP
