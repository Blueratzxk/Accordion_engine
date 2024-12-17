//
// Created by zxk on 7/19/24.
//

#ifndef OLVP_STRINGUTILS_HPP
#define OLVP_STRINGUTILS_HPP

#include <string>
#include <vector>
#include <sstream>


class StringUtils
{
public:
    static void Stringsplit(std::string str, const char split,std::vector<std::string>& res)
    {
        std::istringstream iss(str);	// 输入流
        std::string token;			// 接收缓冲区
        while (getline(iss, token, split))	// 以split为分隔符
        {
            res.push_back(token);
        }
    }



};

#endif //OLVP_STRINGUTILS_HPP
