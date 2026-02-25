#pragma once

#include <string>
#include <fstream>
#include <cstdint>

class HeliosDB;

class WAL {
public:
    explicit WAL(const std::string& path);
    ~WAL();

    void append_put(const std::string& key, const std::string& value);
    void append_delete(const std::string& key);

    void replay(HeliosDB& db);
    void reset();

private:
    std::string path_;
    std::ofstream out_;

    static uint32_t fnv1a_32(const uint8_t* data, size_t n);

    // record encoding helpers
    void append_record(uint8_t type, const std::string& key, const std::string* value);
};