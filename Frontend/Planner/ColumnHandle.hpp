//
// Created by zxk on 10/24/24.
//

#ifndef FRONTEND_COLUMNHANDLE_HPP
#define FRONTEND_COLUMNHANDLE_HPP

class ColumnHandle
{

    string handleId;
public:
    ColumnHandle(string handleId){
        this->handleId = handleId;
    }
    string getHandleId()
    {
        return this->handleId;
    }
};


#endif //FRONTEND_COLUMNHANDLE_HPP
