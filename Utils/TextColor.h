//
// Created by zxk on 7/8/24.
//

#ifndef OLVP_TEXTCOLOR_H
#define OLVP_TEXTCOLOR_H

#include "spdlog/spdlog.h"
#include <string>


using namespace std;



#define TEXT_NONE_COLOR      "\033[0m"
#define TEXT_BLACK           "\033[0;30m"
#define TEXT_DARK_GRAY       "\033[1;30m"
#define TEXT_RED             "\033[0;31m"
#define TEXT_LIGHT_RED       "\033[1;31m"
#define TEXT_GREEN           "\033[0;32m"
#define TEXT_LIGHT_GREEN     "\033[1;32m"
#define TEXT_BROWN           "\033[0;33m"
#define TEXT_YELLOW          "\033[1;33m"
#define TEXT_BLUE            "\033[0;34m"
#define TEXT_LIGHT_BLUE      "\033[1;34m"
#define TEXT_PURPLE          "\033[0;35m"
#define TEXT_LIGHT_PURPLE    "\033[1;35m"
#define TEXT_CYAN            "\033[0;36m"
#define TEXT_LIGHT_CYAN      "\033[1;36m"
#define TEXT_LIGHT_GRAY      "\033[0;37m"
#define TEXT_WHITE           "\033[1;37m"


class TextColor
{
public:
    TextColor(){}

    static inline string RED(string text)
    {
        return TEXT_RED+text+TEXT_NONE_COLOR;
    }
    static inline string BLACK(string text)
    {
        return TEXT_BLACK+text+TEXT_NONE_COLOR;
    }
    static inline string DARK_GRAY(string text)
    {
        return TEXT_DARK_GRAY+text+TEXT_NONE_COLOR;
    }
    static inline string LIGHT_GRAY(string text)
    {
        return TEXT_LIGHT_GRAY+text+TEXT_NONE_COLOR;
    }
    static inline string LIGHT_RED(string text)
    {
        return TEXT_LIGHT_RED+text+TEXT_NONE_COLOR;
    }
    static inline string GREEN(string text)
    {
        return TEXT_GREEN+text+TEXT_NONE_COLOR;
    }
    static inline string LIGHT_GREEN(string text)
    {
        return TEXT_LIGHT_GREEN+text+TEXT_NONE_COLOR;
    }

    static inline string BROWN(string text)
    {
        return TEXT_BROWN+text+TEXT_NONE_COLOR;
    }
    static inline string YELLOW(string text)
    {
        return TEXT_YELLOW+text+TEXT_NONE_COLOR;
    }

    static inline string BLUE(string text)
    {
        return TEXT_BLUE+text+TEXT_NONE_COLOR;
    }

    static inline string LIGHT_BLUE(string text)
    {
        return TEXT_LIGHT_BLUE+text+TEXT_NONE_COLOR;
    }
    static inline string PURPLE(string text)
    {
        return TEXT_PURPLE+text+TEXT_NONE_COLOR;
    }
    static inline string LIGHT_PURPLE(string text)
    {
        return TEXT_LIGHT_PURPLE+text+TEXT_NONE_COLOR;
    }
    static inline string LIGHT_CYAN(string text)
    {
        return TEXT_LIGHT_CYAN+text+TEXT_NONE_COLOR;
    }
    static inline string CYAN(string text)
    {
        return TEXT_CYAN+text+TEXT_NONE_COLOR;
    }

    static inline string WHITE(string text)
    {
        return TEXT_WHITE+text+TEXT_NONE_COLOR;
    }

    static inline string numberTextTrim(string text,int charNumber,string suppChar)
    {
        string result;
        result = text;
        if(result.length() > charNumber)
            return result.substr(0,charNumber);
        else
        {
            int suppNumber = charNumber-result.length();
            for(int i = 0 ; i < suppNumber ; i++)
                result.append(suppChar);
            return result;
        }

    }


};


#endif //OLVP_TEXTCOLOR_H
