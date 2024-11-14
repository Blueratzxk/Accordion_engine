//
// Created by zxk on 6/10/23.
//

#ifndef OLVP_FILECOMMON_HPP
#define OLVP_FILECOMMON_HPP
#include <sys/stat.h>
#include <iostream>

class FileCommon
{
public:

    static size_t getFileSize(const char *fileName) {

        if (fileName == NULL) {
            return 0;
        }
        // 这是一个存储文件(夹)信息的结构体，其中有文件大小和创建时间、访问时间、修改时间等
        struct stat statbuf;
        // 提供文件名字符串，获得文件属性结构体
        stat(fileName, &statbuf);
        // 获取文件大小
        size_t filesize = statbuf.st_size;
        return filesize;
    }


};


#endif //OLVP_FILECOMMON_HPP
