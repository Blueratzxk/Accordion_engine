#include <sys/stat.h>
#include <iostream>

#include <fstream>

#include <arrow/csv/api.h>
#include <arrow/csv/writer.h>
#include <arrow/csv/type_fwd.h>
#include <arrow/ipc/writer.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <sstream>
#include "spdlog/spdlog.h"
#include "nlohmann/json.hpp"

using namespace std;



class httpConfigReader
{
    nlohmann::json j;

    string filePath;
    string localIp;

public:
    httpConfigReader(string filePath,string localIp)
    {
        this->filePath = filePath;
        this->localIp = localIp;
    }

    void read()
    {

        std::ifstream(filePath) >> j;
    }



    void write()
    {

        std::ofstream(filePath) << j.dump(3);
    }


    void update()
    {

        for(auto &x : j.items())
        {
            if(x.key() == "local") {
                x.value()["Restful_Web_Server_IP"] = localIp;
                x.value() ["Arrow_RPC_Server_IP"] = localIp;
            }
        }

    }


};

int main(int argc, char* argv[]) {
    string filepath = string(argv[1]);
    string localIP = string(argv[2]);
    httpConfigReader reader(filepath,localIP);
    reader.read();
    reader.update();
    reader.write();
    return 0;
}
