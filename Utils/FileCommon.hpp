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
        // ����һ���洢�ļ�(��)��Ϣ�Ľṹ�壬�������ļ���С�ʹ���ʱ�䡢����ʱ�䡢�޸�ʱ���
        struct stat statbuf;
        // �ṩ�ļ����ַ���������ļ����Խṹ��
        stat(fileName, &statbuf);
        // ��ȡ�ļ���С
        size_t filesize = statbuf.st_size;
        return filesize;
    }


};


#endif //OLVP_FILECOMMON_HPP
