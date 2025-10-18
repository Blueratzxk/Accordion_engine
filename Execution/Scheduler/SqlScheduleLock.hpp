//
// Created by zxk on 10/17/25.
//

#ifndef OLVP_SQLSCHEDULELOCK_HPP
#define OLVP_SQLSCHEDULELOCK_HPP

#include <mutex>
#include <condition_variable>
#include <iostream>
#include <thread>
#include <chrono>
#include <vector>

class SqlScheduleLock
{

private:
    std::mutex m;
    std::condition_variable cv;
    int tuningRequests = 0;
    bool sidewayScheduling = false;

public:
    bool tune_lock() {
        std::unique_lock<std::mutex> lk(m);

        if (sidewayScheduling) {
            spdlog::info("Sideway task running! Tuning request rejected!");
            return false;
        }
        tuningRequests++;
        return true;
    }

    void tune_unlock() {
        std::unique_lock<std::mutex> lk(m);
        tuningRequests--;
        if (tuningRequests == 0) {
            cv.notify_all();
        }
    }

    void sideway_lock() {
        std::unique_lock<std::mutex> lk(m);
        cv.wait(lk, [this] { return !sidewayScheduling && tuningRequests == 0; });
        sidewayScheduling = true;
    }

    void sideway_unlock() {
        std::unique_lock<std::mutex> lk(m);
        sidewayScheduling = false;
        cv.notify_all();
    }

};



#endif //OLVP_SQLSCHEDULELOCK_HPP
