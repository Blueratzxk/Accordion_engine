//
// Created by zxk on 5/16/23.
//

#ifndef OLVP_OPERATOR_HPP
#define OLVP_OPERATOR_HPP

#include "../common.h"
#include "../Page/DataPage.hpp"

using namespace std;

class Operator {
public:
    virtual string getOperatorId() =0;
    virtual void addInput(std::shared_ptr<DataPage> input) {}
    virtual std::shared_ptr<DataPage> getOutput() { return NULL; }
    virtual bool externalEvent(){return false;}
    virtual void fulfillExternalEventWithPages(vector<std::shared_ptr<DataPage>> pages){}
    virtual bool isFinished() { return true; }
    virtual bool needsInput() { return true; }
    virtual int extensionToken() {return 0;}
    virtual int isExtension() {return false;}
    virtual shared_ptr<DataPage> uploadToExtension(shared_ptr<DataPage> page) {return page;};
    virtual shared_ptr<DataPage> downloadToCPU(shared_ptr<DataPage> page){return page;};

};



#endif //OLVP_OPERATOR_HPP
