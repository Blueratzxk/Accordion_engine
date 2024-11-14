//
// Created by zxk on 5/27/23.
//

#ifndef OLVP_BLOCKQUEUE_HPP
#define OLVP_BLOCKQUEUE_HPP
#include<mutex>
#include<list>
#include<condition_variable>
using namespace std;

template <typename T>
class BlockQueue
{
public:
    BlockQueue(){}

    ~BlockQueue(){}

    void Put(const T& x)
    {
        unique_lock<mutex> guard(m_mutex);
        m_queue.push_back(x);
        m_cond.notify_all();
    }

    void Put(T&&x)
    {
        unique_lock<mutex> guard(m_mutex);
        m_queue.push_back(x);
        m_cond.notify_all();
    }

    T Take()
    {
        unique_lock<mutex> guard(m_mutex);
        while (m_queue.size() == 0)
            m_cond.wait(guard);
        T front(m_queue.front());
        m_queue.pop_front();
        return front;
    }

    size_t Size()
    {
        unique_lock<mutex> guard(m_mutex);
        return m_queue.size();
    }

private:
    mutable mutex m_mutex;
    condition_variable m_cond;
    list<T>     m_queue;
};

#endif //OLVP_BLOCKQUEUE_HPP
