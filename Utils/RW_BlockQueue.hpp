//
// Created by zxk on 6/21/23.
//

#ifndef OLVP_RW_WaitBLOCKQUEUE_HPP
#define OLVP_RW_WaitBLOCKQUEUE_HPP

#include <mutex>
#include <deque>
#include <condition_variable>
#include <assert.h>
#include <chrono>
template<class T>
class RW_BlockQueue {
public:
    explicit RW_BlockQueue(size_t maxCapacity = 1024);

    ~RW_BlockQueue();

    void close();

    void flush();

    void clear();

    T front();

    T back();

    size_t get_size();

    size_t get_capacity();

    void push_back(const T& item);

    void push_front(const T& item);

    bool empty();

    bool full();

    bool pop(T& item);

    bool pop(T& item, int timeout);

private:
    std::deque<T> deq;

    size_t capacity;

    std::mutex mtx;

    bool isClose;

    std::condition_variable Consumer;

    std::condition_variable Producer;
};


template<class T>
RW_BlockQueue<T>::RW_BlockQueue(size_t maxCapacity):capacity(maxCapacity)
{
    assert(maxCapacity > 0);
    isClose = false;
}

template<class T>
RW_BlockQueue<T>::~RW_BlockQueue()
{
    close();
};

template<class T>
void RW_BlockQueue<T>::close()
{
    {
        std::lock_guard<std::mutex> locker(mtx);
        deq.clear();
        isClose= true;
    }
    Producer.notify_all();
    Consumer.notify_all();
};

template<class T>
void RW_BlockQueue<T>::flush()
{
    Consumer.notify_one();
}

template<class T>
void RW_BlockQueue<T>::clear()
{
    std::lock_guard<std::mutex> lock(mtx);
    deq.clear();
}

template<class T>
T RW_BlockQueue<T>::front()
{
    std::lock_guard<std::mutex> lock(mtx);
    return deq.front();
}

template<class T>
T RW_BlockQueue<T>::back()
{
    std::lock_guard<std::mutex> lock(mtx);
    return deq.back();
}

template<class T>
size_t RW_BlockQueue<T>::get_size()
{
    std::lock_guard<std::mutex> lock(mtx);
    return deq.size();
}

template<class T>
size_t RW_BlockQueue<T>::get_capacity()
{
    std::lock_guard<std::mutex> lock(mtx);
    return capacity;
}

template<class T>
void RW_BlockQueue<T>::push_back(const T& item)
{
    std::unique_lock<std::mutex> lock(mtx);
    while (deq.size() >= capacity)
    {
        Producer.wait(lock);
    }
    deq.push_back(item);
    Consumer.notify_one();
}

template<class T>
void RW_BlockQueue<T>::push_front(const T& item)
{
    std::unique_lock<std::mutex> lock(mtx);
    while (deq.size() >= capacity)
    {
        Producer.wait(lock);
    }
    deq.push_front(item);
    Consumer.notify_one();
}

template<class T>
bool RW_BlockQueue<T>::empty()
{
    std::lock_guard<std::mutex> lock(mtx);
    return deq.empty();
}

template<class T>
bool RW_BlockQueue<T>::full()
{
    std::lock_guard<std::mutex> lock(mtx);
    return deq.size()>=capacity;
}

template<class T>
bool RW_BlockQueue<T>::pop(T& item)
{
    std::unique_lock<std::mutex> lock(mtx);
    while (deq.empty())
    {
        Consumer.wait(lock);
        if (isClose)
            return false;
    }
    item = deq.front();
    deq.pop_front();
    Producer.notify_one();
    return true;
}

template<class T>
bool RW_BlockQueue<T>::pop(T& item,int timeout)
{
    std::unique_lock<std::mutex> lock(mtx);
    while (deq.empty())
    {
        if (Consumer.wait_for(lock, std::chrono::microseconds(timeout)) == std::cv_status::timeout)
            return false;
        if (isClose)
            return false;
    }
    item = deq.front();
    deq.pop_front();
    Producer.notify_one();
    return true;
}


#endif //OLVP_RW_WaitBLOCKQUEUE_HPP
