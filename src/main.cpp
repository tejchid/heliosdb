#include "db.hpp"
#include <iostream>

int main() {
    {
        HeliosDB db("data");
        db.put("name", "tejas");
        db.put("role", "engineer");
        db.del("old_key");          // tombstone example
        db.flush();                 // force SSTable
    }

    {
        HeliosDB db("data");
        auto v1 = db.get("name");
        auto v2 = db.get("role");

        std::cout << (v1 ? *v1 : "<missing>") << "\n";
        std::cout << (v2 ? *v2 : "<missing>") << "\n";
    }

    return 0;
}