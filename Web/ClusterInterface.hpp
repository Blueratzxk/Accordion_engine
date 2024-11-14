//
// Created by zxk on 11/26/23.
//

#ifndef OLVP_CLUSTERINTERFACE_HPP
#define OLVP_CLUSTERINTERFACE_HPP


#include "../System/ClusterServer.h"

#include <string>
using namespace std;

class ClusterInterFace
{

public:
    ClusterInterFace(){
    }

    static void reportHeartbeat(string heartbeat)
    {
        ClusterServer::resolveHeartbeat(heartbeat);
    }



};




#endif //OLVP_CLUSTERINTERFACE_HPP
