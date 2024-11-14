//
// Created by zxk on 5/16/23.
//

#ifndef OLVP_CONNECTORSPLIT_HPP
#define OLVP_CONNECTORSPLIT_HPP

#include "../common.h"
using namespace std;

class ConnectorSplit
{

    std::string ConnectorSplitId;
    enum NodeSelectionStrategy
    {
        HARD_AFFINITY,
        SOFT_AFFINITY,
        NO_PREFERENCE
    };

public:

    ConnectorSplit(string Id){this->ConnectorSplitId = Id;}
    std::string getId(){return this->ConnectorSplitId;}
    virtual list<string> getPreferredNodes(){ list<string> l; return l;}
    virtual string getInfo(){return "info";}
    virtual NodeSelectionStrategy getNodeSelectionStrategy() {return NO_PREFERENCE;}


};
#endif //OLVP_CONNECTORSPLIT_HPP
