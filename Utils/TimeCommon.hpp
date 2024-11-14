//
// Created by zxk on 6/17/23.
//

#ifndef OLVP_TIMECOMMON_HPP
#define OLVP_TIMECOMMON_HPP


#include <iomanip>
#include <sstream>


using namespace std;

class TimeCommon
{

public:
    static bool getDate32(string dateString,int32_t *out)
    {
        //"2017-06-08 09:00:05"
        std::tm tm = {};
        std::stringstream ss(dateString);
        ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");

        if(ss.fail()){
            *out = -1;
            spdlog::critical("Date string format error! The value is set to -1!");
            return false;
        }
        time_t timeStamp = std::mktime(&tm);
        *out = timeStamp/3600/24;
        return true;
    }

    static string getCurrentTimeString()
    {
        auto t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        std::stringstream ss;
        ss << std::put_time(std::localtime(&t), "%Y-%m-%d-%H-%M-%S-");
        std::string str_time = ss.str();
        return str_time;
    }

    static long long getCurrentTimeStamp()
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    }



};

#endif //OLVP_TIMECOMMON_HPP
