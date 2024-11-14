//
// Created by zxk on 11/27/23.
//

#ifndef OLVP_WEBCOMMON_HPP
#define OLVP_WEBCOMMON_HPP

#include <cstdlib>
#include <cstdio>
#include <string.h>
class WebCommon
{
public:
    WebCommon(){}

    static bool isPortUsed(int port) {

        char cmd[64] = {0};
        sprintf(cmd, "lsof -i:%d", port);
        FILE *fp = nullptr;
        if ((fp = popen(cmd, "r")) == nullptr) {
            return false;
        }
        bool isOpen = false;
        char buf[1024] = {0};
        while (fgets(buf, sizeof buf, fp)) {
            if (strstr(buf, "LISTEN")) {
                isOpen = true;
                break;
            }
        }
        pclose(fp);
        return isOpen;
    }


};


#endif //OLVP_WEBCOMMON_HPP
