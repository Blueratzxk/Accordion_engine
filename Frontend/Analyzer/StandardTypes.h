//
// Created by zxk on 10/30/24.
//

#ifndef FRONTEND_STANDARDTYPES_H
#define FRONTEND_STANDARDTYPES_H

#include <string>
using namespace std;
class StandardTypes {
public:
    static string BIGINT;
    static string INTEGER;
    static string SMALLINT;
    static string TINYINT;
    static string BOOLEAN;
    static string DATE;
    static string DECIMAL;
    static string REAL;
    static string DOUBLE;
    static string HYPER_LOG_LOG;
    static string QDIGEST;
    static string TDIGEST;
    static string KLL_SKETCH;
    static string P4_HYPER_LOG_LOG;
    static string INTERVAL_DAY_TO_SECOND;
    static string INTERVAL_YEAR_TO_MONTH;
    static string TIMESTAMP;
    static string TIMESTAMP_MICROSECONDS;
    static string TIMESTAMP_WITH_TIME_ZONE;
    static string TIME;
    static string TIME_WITH_TIME_ZONE;
    static string VARBINARY;
    static string VARCHAR;
    static string CHAR;
    static string ROW;
    static string ARRAY;
    static string MAP;
    static string JSON;
    static string IPADDRESS;
    static string IPPREFIX;
    static string GEOMETRY;
    static string SPHERICAL_GEOGRAPHY;
    static string BING_TILE;
    static string BIGINT_ENUM;
    static string VARCHAR_ENUM;
    static string DISTINCT_TYPE;
    static string UUID;

    static bool isStandardType(string type);


};


#endif //FRONTEND_STANDARDTYPES_H
