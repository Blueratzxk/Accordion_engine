//
// Created by zxk on 11/9/24.
//

#ifndef ACCORDION_FRONTEND_H
#define ACCORDION_FRONTEND_H

#include <string>
#include <list>
class PlanNode;

class FrontEnd
{
    std::list<std::string> feedbacks;
public:
    FrontEnd(){

    }
    std::list<std::string> getFeedbacks();
    PlanNode* gogogo(std::string sql);
    PlanNode* go(std::string sql);
};



#endif //ACCORDION_FRONTEND_H
