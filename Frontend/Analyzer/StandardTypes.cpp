//
// Created by zxk on 10/30/24.
//


#include "StandardTypes.h"


bool StandardTypes::isStandardType(std::string type) {
    {
        if(    type == BIGINT ||
               type == INTEGER ||
               type == SMALLINT ||
               type == TINYINT ||
               type == BOOLEAN ||
               type == DATE ||
               type == DECIMAL ||
               type == REAL ||
               type == DOUBLE ||
               type == HYPER_LOG_LOG ||
               type == QDIGEST ||
               type == TDIGEST ||
               type == KLL_SKETCH ||
               type == P4_HYPER_LOG_LOG ||
               type == INTERVAL_DAY_TO_SECOND ||
               type == INTERVAL_YEAR_TO_MONTH ||
               type == TIMESTAMP ||
               type == TIMESTAMP_MICROSECONDS ||
               type == TIMESTAMP_WITH_TIME_ZONE ||
               type == TIME ||
               type == TIME_WITH_TIME_ZONE ||
               type == VARBINARY ||
               type == VARCHAR ||
               type == CHAR ||
               type == ROW ||
               type == ARRAY ||
               type == MAP ||
               type == JSON ||
               type == IPADDRESS ||
               type == IPPREFIX ||
               type == GEOMETRY ||
               type == SPHERICAL_GEOGRAPHY ||
               type == BING_TILE ||
               type == BIGINT_ENUM ||
               type == VARCHAR_ENUM ||
               type == DISTINCT_TYPE ||
               type == UUID )
            return true;

        return false;
    }
}


string StandardTypes::BIGINT = "bigint";
string StandardTypes::INTEGER = "integer";
string StandardTypes::SMALLINT = "smallint";
string StandardTypes::TINYINT = "tinyint";
string StandardTypes::BOOLEAN = "boolean";
string StandardTypes::DATE = "date";
string StandardTypes::DECIMAL = "decimal";
string StandardTypes::REAL = "real";
string StandardTypes::DOUBLE = "double";
string StandardTypes::HYPER_LOG_LOG = "HyperLogLog";
string StandardTypes::QDIGEST = "qdigest";
string StandardTypes::TDIGEST = "tdigest";
string StandardTypes::KLL_SKETCH = "kllsketch";
string StandardTypes::P4_HYPER_LOG_LOG = "P4HyperLogLog";
string StandardTypes::INTERVAL_DAY_TO_SECOND = "interval day to second";
string StandardTypes::INTERVAL_YEAR_TO_MONTH = "interval year to month";
string StandardTypes::TIMESTAMP = "timestamp";
string StandardTypes::TIMESTAMP_MICROSECONDS = "timestamp microseconds";
string StandardTypes::TIMESTAMP_WITH_TIME_ZONE = "timestamp with time zone";
string StandardTypes::TIME = "time";
string StandardTypes::TIME_WITH_TIME_ZONE = "time with time zone";
string StandardTypes::VARBINARY = "varbinary";
string StandardTypes::VARCHAR = "varchar";
string StandardTypes::CHAR = "char";
string StandardTypes::ROW = "row";
string StandardTypes::ARRAY = "array";
string StandardTypes::MAP = "map";
string StandardTypes::JSON = "json";
string StandardTypes::IPADDRESS = "ipaddress";
string StandardTypes::IPPREFIX = "ipprefix";
string StandardTypes::GEOMETRY = "Geometry";
string StandardTypes::SPHERICAL_GEOGRAPHY = "SphericalGeography";
string StandardTypes::BING_TILE = "BingTile";
string StandardTypes::BIGINT_ENUM = "BigintEnum";
string StandardTypes::VARCHAR_ENUM = "VarcharEnum";
string StandardTypes::DISTINCT_TYPE = "DistinctType";
string StandardTypes::UUID = "uuid";