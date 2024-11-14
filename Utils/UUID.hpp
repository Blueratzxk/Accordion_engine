//
// Created by zxk on 5/31/23.
//

#ifndef OLVP_UUID_HPP
#define OLVP_UUID_HPP

#include <iostream>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <functional>
#include <random>


class UUID
{

public:
    static  std::string create_uuid()
    {
        std::stringstream stream;
        auto random_seed = std::chrono::system_clock::now().time_since_epoch().count();
        std::mt19937 seed_engine(random_seed);
        std::uniform_int_distribution<std::size_t> random_gen ;
        std::size_t value = random_gen(seed_engine);
        stream << std::hex << value;

        return stream.str();
    }


};





#endif //OLVP_UUID_HPP
