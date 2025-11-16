//
// Created by zxk on 11/15/25.
//

#ifndef OLVP_TASKRESULTPROVIDER_HPP
#define OLVP_TASKRESULTPROVIDER_HPP


class TaskResultProvider
{
    shared_ptr<TaskResultFetcher> fetcher = NULL;

    vector<shared_ptr<DataPage>> resultSet;

    mutex resultProduceAndGetLock;
    bool produceAndGetEndPage = false;

    unsigned int cursor = 0;
    unsigned int getDataCursor = 0;

    bool firstPageGot = false;

public:
    TaskResultProvider(){}

    void setTaskResultFetcher(shared_ptr<TaskResultFetcher> resultFetcher)
    {
        this->fetcher = resultFetcher;
    }

    bool hasFetcher()
    {
        return this->fetcher != nullptr;
    }

    list<shared_ptr<DataPage>> headPage()
    {
        return {this->resultSet.front()};
    }

    list<shared_ptr<DataPage>> nextPage() {

        if (this->fetcher == NULL)
            return {};

        if(this->resultSet.empty())
            ;
        else if(cursor == 0 && this->resultSet.size() == 1 && !firstPageGot) {
            auto re = resultSet[cursor];
            firstPageGot = true;
            return {re};
        }
        else if(cursor == this->resultSet.size() - 1)
            ;
        else if (cursor < (this->resultSet.size() - 1)) {

            cursor++;
            auto re = resultSet[cursor];


            return {re};
        }

        resultProduceAndGetLock.lock();

        if (this->produceAndGetEndPage) {
            resultProduceAndGetLock.unlock();
            if(this->resultSet.empty())
                return {};
            else
                return {resultSet.back()};
        }

        shared_ptr<DataPage> result;
        do {
            this->fetcher->schedule();
            result = this->fetcher->pollPage();
        } while (result == NULL || result->isEmptyPage());


        if (result->isEndPage()) {
            this->produceAndGetEndPage = true;
        } else
            this->resultSet.push_back(result);

        resultProduceAndGetLock.unlock();

        if (cursor == this->resultSet.size() -1)
            return {resultSet[cursor]};
        else {
            cursor++;
            auto re = resultSet[cursor];
            return {re};
        }
    }

    list<shared_ptr<DataPage>> getResultByCursor(unsigned int index){

        if(this->fetcher == NULL)
            return {};
        if(this->resultSet.empty())
            return {};

        if(index >= this->resultSet.size() - 1) {
            cursor = resultSet.size() - 1;
            return {resultSet[cursor]};
        }
        cursor = index;

        return {resultSet[cursor]};
    }

    int getCursor()
    {
        return this->cursor;
    }

    int getResultSize()
    {
        return this->resultSet.size() - 1;
    }

    void fetchAllPages()
    {
        shared_ptr<DataPage> result;
        for(;;){
            this->fetcher->schedule();
            result = this->fetcher->pollPage();

            if(result == NULL || result->isEmptyPage())
                ;
            else if(result->isEndPage())
                break;
            else
                this->resultSet.push_back(result);
        }
        this->produceAndGetEndPage = true;
    }

    void finish()
    {
        this->resultProduceAndGetLock.lock();
        this->produceAndGetEndPage = true;
        this->resultProduceAndGetLock.unlock();
    }

};


#endif //OLVP_TASKRESULTPROVIDER_HPP
