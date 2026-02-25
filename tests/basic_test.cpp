#include "db.hpp"
#include <cassert>
#include <filesystem>
#include <fstream>

int main() {
    const std::string dir = "data_test";

    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    // Create multiple flushed tables
    {
        HeliosDB db(dir);
        for (int i = 0; i < 5000; i++) {
            db.put("k" + std::to_string(i), "v" + std::to_string(i));
        }
        db.flush();

        for (int i = 0; i < 5000; i++) {
            if (i % 2 == 0) db.del("k" + std::to_string(i));
        }
        db.flush();

        for (int i = 0; i < 5000; i++) {
            db.put("k" + std::to_string(i), "v2" + std::to_string(i));
        }
        db.flush();

        db.compact();
    }

    // Verify restart correctness
    {
        HeliosDB db(dir);
        for (int i = 0; i < 5000; i++) {
            auto v = db.get("k" + std::to_string(i));
            assert(v.has_value());
            assert(v.value() == "v2" + std::to_string(i));
        }
    }

    std::filesystem::remove_all(dir);
    return 0;
}