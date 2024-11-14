//
// Created by zxk on 11/17/23.
//

#ifndef OLVP_SIMPLELEXER_HPP
#define OLVP_SIMPLELEXER_HPP



#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <fstream>
#include <sstream>

#include "SimpleToken.hpp"
#include "spdlog/spdlog.h"
using namespace std;


class SimpleLexer {

    vector<string> KEYWORD = { "BEGIN","END"};
    string SEPARATER = { ';',',','{','}','[',']','(',')','\"','%' };
    string OPERATOR = { '+','-','*','/','>','<','=','!' };
    string FILTER = { ' ','\t','\r','\n' };


    vector<SimpleToken> tokens;


    bool IsKeyword(string word) {
        for (int i = 0; i < KEYWORD.size(); i++) {
            if (KEYWORD[i] == word) {
                return true;
            }
        }
        return false;
    }

    bool IsSeparater(char ch) {
        for (int i = 0; i < SEPARATER.length(); i++) {
            if (SEPARATER[i] == ch) {
                return true;
            }
        }
        return false;
    }


    bool IsOperator(char ch) {
        for (int i = 0; i < OPERATOR.length(); i++) {
            if (OPERATOR[i] == ch) {
                return true;
            }
        }
        return false;
    }

    bool IsFilter(char ch) {
        for (int i = 0; i < FILTER.length(); i++) {
            if (FILTER[i] == ch) {
                return true;
            }
        }
        return false;
    }

    bool IsUpLetter(char ch) {
        if (ch >= 'A' && ch <= 'Z') return true;
        return false;
    }

    bool IsLowLetter(char ch) {
        if (ch >= 'a' && ch <= 'z') return true;
        return false;
    }

    bool isLetter(char ch)
    {
        if (ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z' || ch == '_')
            return true;
        return false;
    }

    bool isWord(char ch)
    {
        if (ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z' || ch == '_' || ch >= '0' && ch <= '9')
            return true;
        return false;
    }


    bool IsDigit(char ch) {
        if (ch >= '0' && ch <= '9') return true;
        return false;
    }

    char skipAnnotation(string s,int &index)
    {
        if (s[index] == '/' && s[index + 1] == '*')
        {
            index+=2;
            while (s[index] != '\0' && s[index] != '*')
            {
                index++;
            }
            if (s[index] == '\0') return true;
            if (s[index + 1] == '/')
                index+=2;
            return true;
        }
        return false;
    }


    bool analyse(string s, int &index, vector<SimpleToken> &tokens) {
        char ch = ' ';
        string arr = "";

        skipAnnotation(s, index);
        while ((ch = s[index++]) != '\0') {
            arr = "";

            skipAnnotation(s, index);
            if (IsFilter(ch)) {}
            else if (isLetter(ch)) {
                while (isWord(ch)) {
                    arr += ch;
                    ch = s[index++];
                }

                if (IsKeyword(arr)) {

                    tokens.push_back(SimpleToken(CODE_KEYWORD, arr));
                    index--;
                }
                else
                {
                    tokens.push_back(SimpleToken(CODE_IDENTIFIER, arr));
                    index--;
                }
            }
            else if (IsDigit(ch)) {
                while (IsDigit(ch) || (ch == '.'&&IsDigit(s[index]))) {
                    arr += ch;
                    ch = s[index++];
                }
                index--;
                tokens.push_back(SimpleToken(CODE_NUMBER, arr));

            }

            else if (IsOperator(ch))
            {
                arr += ch;
                tokens.push_back(SimpleToken(CODE_OPERATOR, arr));

            }
            else if (IsSeparater(ch))
            {
                arr += ch;
                tokens.push_back(SimpleToken(CODE_DELIMETER, arr));

            }
            else
            {
                string info = "\"";
                info.push_back(ch);
                info +="\":Cannot identify this character£¡";
                spdlog::error(info);
                return false;
            }


        }
        return true;

    }


    bool success = false;
    int cursor = 0;
public:

    SimpleLexer(string text){
        int index = 0;
        this->success = this->analyse(text,index,tokens);
     //   for (auto t : tokens)
      //      cout << t.getType() << " " << t.getValue() << endl;
    }

    bool isSuccess()
    {
        return this->success;
    }

    bool isFinished()
    {
        return this->cursor >= this->tokens.size();
    }


    SimpleToken lex()
    {
        if (this->cursor >= this->tokens.size())
            return SimpleToken(CODE_END, "END");

        return this->tokens[cursor++];
    }

    SimpleToken ConsumeToken()
    {
        SimpleToken curToken = this->lex();
        spdlog::debug("{" +curToken.getValue() +"}");
        return curToken;
    }

    bool ConsumeToken(string token)
    {
        SimpleToken curToken = this->lex();

        spdlog::debug("{" +curToken.getValue() +"}");

        return curToken.getValue() == token;

    }
    SimpleToken peekcurrent()
    {
        return this->tokens[cursor];
    }

    SimpleToken peekahead()
    {
        if (this->cursor >= this->tokens.size())
            return SimpleToken(CODE_END, "END");

        return this->tokens[cursor + 1];
    }

    SimpleToken peekahead(int n)
    {
        if (this->cursor >= this->tokens.size())
            return SimpleToken(CODE_END, "END");

        return this->tokens[cursor + n];
    }




};


#endif //OLVP_SIMPLELEXER_HPP
