//
// Created by zxk on 7/8/24.
//

#ifndef OLVP_NETINFOCOLLECTOR_HPP
#define OLVP_NETINFOCOLLECTOR_HPP


#include "spdlog/spdlog.h"
#include <fstream>
#include <sstream>
#include <string>
#include <unistd.h>

using namespace std;


class NetInfoCollector {

    std::string interface;

    unsigned long long oldReceivedBytes;
    unsigned long long oldTransmittedBytes;
    unsigned long long newReceivedBytes;
    unsigned long long newTransmittedBytes;
    double receivedRate;
    double transmittedRate;
    int sampleGap = 1;
    WebConfig webConfig;

public:
    NetInfoCollector(string interface){
        this->interface = interface;
    }

    unsigned long long getValue(std::string const &line, int fieldIndex) {
        std::istringstream iss(line);
        std::string token;
        unsigned long long value;
        for (int i = 0; i <= fieldIndex; ++i) {
            iss >> token;
        }
        value = std::stoull(token);
        return value;
    }

     int getNICSpeed() {


         auto speedFromFile = webConfig.getNIC_Speed();
         if(speedFromFile != "")
             return atoi(speedFromFile.c_str());

        string nicSpeedFilePath;
        nicSpeedFilePath.append("/sys/class/net/");
        nicSpeedFilePath.append(this->interface);
        nicSpeedFilePath.append("/speed");

        std::ifstream netDev(nicSpeedFilePath);
        if (!netDev) {
            spdlog::error("cannot open "+nicSpeedFilePath+" !");
            return 1000;
        }

         int value;
        std::string line;
        std::getline(netDev, line);

        value = atol(line.c_str());
        netDev.close();

        return value;
    }


    unsigned long long getReceivedBytes(std::string const &interface) {
        std::ifstream netDev("/proc/net/dev");
        if (!netDev) {
            spdlog::error("cannot open /proc/net/dev!");
            return 0;
        }
        std::string line;
        while (std::getline(netDev, line)) {
            if (line.find(interface + ":") != std::string::npos) {
                netDev.close();
                return getValue(line, 1);
            }
        }
        netDev.close();

        return 0;
    }


    unsigned long long getTransmittedBytes(std::string const &interface) {
        std::ifstream netDev("/proc/net/dev");
        if (!netDev) {
            spdlog::error("cannot open /proc/net/dev!");
            return 0;
        }
        std::string line;
        while (std::getline(netDev, line)) {
            if (line.find(interface + ":") != std::string::npos) {
                netDev.close();
                return getValue(line, 9);
            }
        }
        netDev.close();


        return 0;
    }
    void sampleAlpha()
    {
        oldReceivedBytes = getReceivedBytes(interface);
        oldTransmittedBytes = getTransmittedBytes(interface);
    }

    void sampleBeta()
    {
        newReceivedBytes = getReceivedBytes(interface);
        newTransmittedBytes = getTransmittedBytes(interface);
    }

    void computeRate(double sampleGap)
    {
        receivedRate = (newReceivedBytes - oldReceivedBytes) / 1024.0/ sampleGap;
        transmittedRate = (newTransmittedBytes - oldTransmittedBytes) / 1024.0/ sampleGap;
    }

    void collect()
    {

        sampleAlpha();
        sleep(sampleGap);
        sampleBeta();
        computeRate(sampleGap);
        // std::cout << "接收速率： " << receivedRate << " KB/s" << std::endl;
        //  std::cout << "发送速率： " << transmittedRate << " KB/s" << std::endl;
    }

    double getReceivedRate()
    {
        return this->receivedRate;
    }

    double getTransmittedRate()
    {
        return this->transmittedRate;
    }


};



#endif //OLVP_NETINFOCOLLECTOR_HPP
