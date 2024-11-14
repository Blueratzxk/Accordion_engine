//
// Created by zxk on 5/31/23.
//

#ifndef OLVP_CLIENT_HPP
#define OLVP_CLIENT_HPP


#include <iostream>
#include "restclient.h"
#include <string>
#include "base64.hpp"
#include "nlohmann/json.hpp"
#include "spdlog/spdlog.h"
#include "../../Utils/UUID.hpp"
#include <memory>
using namespace std;

class http_Client
{

public:

    http_Client(){}



    static void POST(string addrDest,vector<string> data)
    {

        Base64 base64;
        for(int i = 0 ; i < data.size() ; i++) {
            char *encode_out = (char *) malloc(BASE64_ENCODE_OUT_SIZE(data[i].size()));
            base64.base64_encode(reinterpret_cast<const unsigned char *>(data[i].c_str()), data[i].size(), encode_out);
            string out(encode_out);
            addrDest += "/"+to_string(data[i].size()) + "/" + out;
            free(encode_out);
        }


        string uuid = UUID::create_uuid();


        //spdlog::info(uuid+"#POST---"+addrDest);
        RestClient::Response r = RestClient::post(addrDest, "application/json","");
        //spdlog::info(uuid+"#RESPONSE---"+r.body);


    }


    static string POST_GetResult(string addrDest,vector<string> data)
    {

        Base64 base64;
        for(int i = 0 ; i < data.size() ; i++) {
            char *encode_out = (char *) malloc(BASE64_ENCODE_OUT_SIZE(data[i].size()));
            base64.base64_encode(reinterpret_cast<const unsigned char *>(data[i].c_str()), data[i].size(), encode_out);
            string out(encode_out);
            addrDest += "/"+to_string(data[i].size()) + "/" + out;
            free(encode_out);
        }


        string uuid = UUID::create_uuid();
        RestClient::Response r = RestClient::post(addrDest, "application/json","");

        return r.body;

    }




    static void POST2(string addrDest,string data)
    {

        Base64 base64;
        RestClient::Response r = RestClient::post(addrDest, "application/json","");

        cout << r.body << endl;
        nlohmann::json result;
        result = nlohmann::json::parse(r.body);

        int len = atoi(string(result["length"]).c_str());
        string body = result["result"];

        unsigned char *decode_out = (unsigned char *)malloc(BASE64_DECODE_OUT_SIZE(body.size()));
        base64.base64_decode(body.c_str(),body.size(),decode_out);

        string out((char*)decode_out,len);
        cout << out <<"!!!!!!" <<endl;
        free(decode_out);

    }
    static void GET(string addrDest)
    {
        RestClient::Response r = RestClient::get("192.168.226.135:9080/v1/bin");
        cout << r.body<<endl;

    }

};



#endif //OLVP_CLIENT_HPP
