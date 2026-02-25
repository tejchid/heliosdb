#include "wal.hpp"
#include "db.hpp"

#include <filesystem>
#include <fstream>
#include <vector>
#include <cstring>

namespace {
#pragma pack(push, 1)
struct WalHeader {
    uint32_t total_len;   // header+payload+checksum
    uint8_t  type;        // 1=put, 2=del
    uint32_t ksize;
    uint32_t vsize;       // 0 for delete
    uint32_t checksum;    // FNV-1a over (type,ksize,vsize,key,value)
};
#pragma pack(pop)
} // namespace

WAL::WAL(const std::string& path)
    : path_(path)
{
    out_.open(path_, std::ios::binary | std::ios::app);
}

WAL::~WAL() {
    if (out_.is_open()) out_.close();
}

uint32_t WAL::fnv1a_32(const uint8_t* data, size_t n) {
    uint32_t h = 2166136261u;
    for (size_t i = 0; i < n; ++i) {
        h ^= data[i];
        h *= 16777619u;
    }
    return h;
}

void WAL::append_record(uint8_t type, const std::string& key, const std::string* value) {
    const uint32_t ksize = static_cast<uint32_t>(key.size());
    const uint32_t vsize = value ? static_cast<uint32_t>(value->size()) : 0u;

    // compute checksum over a contiguous buffer: [type][ksize][vsize][key][value]
    std::vector<uint8_t> buf;
    buf.reserve(1 + sizeof(uint32_t) + sizeof(uint32_t) + ksize + vsize);

    buf.push_back(type);

    auto push_u32 = [&](uint32_t x) {
        uint8_t b[4];
        std::memcpy(b, &x, 4);
        buf.insert(buf.end(), b, b + 4);
    };

    push_u32(ksize);
    push_u32(vsize);
    buf.insert(buf.end(), reinterpret_cast<const uint8_t*>(key.data()),
               reinterpret_cast<const uint8_t*>(key.data()) + ksize);
    if (value && vsize) {
        buf.insert(buf.end(),
                   reinterpret_cast<const uint8_t*>(value->data()),
                   reinterpret_cast<const uint8_t*>(value->data()) + vsize);
    }

    WalHeader hdr{};
    hdr.type = type;
    hdr.ksize = ksize;
    hdr.vsize = vsize;
    hdr.checksum = fnv1a_32(buf.data(), buf.size());
    hdr.total_len = static_cast<uint32_t>(sizeof(WalHeader) + ksize + vsize);

    out_.write(reinterpret_cast<const char*>(&hdr), sizeof(hdr));
    out_.write(key.data(), ksize);
    if (value && vsize) out_.write(value->data(), vsize);
    out_.flush();
}

void WAL::append_put(const std::string& key, const std::string& value) {
    append_record(1, key, &value);
}

void WAL::append_delete(const std::string& key) {
    append_record(2, key, nullptr);
}

void WAL::replay(HeliosDB& db) {
    std::ifstream in(path_, std::ios::binary);
    if (!in.is_open()) return;

    while (true) {
        WalHeader hdr{};
        in.read(reinterpret_cast<char*>(&hdr), sizeof(hdr));
        if (!in) break; // EOF cleanly

        // Basic sanity checks to prevent insane allocations on corruption
        if (hdr.total_len < sizeof(WalHeader)) break;
        if (hdr.type != 1 && hdr.type != 2) break;
        if (hdr.type == 2 && hdr.vsize != 0) break;

        // Ensure remaining bytes are available; if not, tail is partial => stop safely
        const uint64_t payload_len = static_cast<uint64_t>(hdr.ksize) + static_cast<uint64_t>(hdr.vsize);
        if (hdr.total_len != sizeof(WalHeader) + payload_len) break;

        std::string key(hdr.ksize, '\0');
        in.read(key.data(), hdr.ksize);
        if (!in) break;

        std::string value;
        if (hdr.type == 1) {
            value.assign(hdr.vsize, '\0');
            in.read(value.data(), hdr.vsize);
            if (!in) break;
        }

        // recompute checksum
        std::vector<uint8_t> buf;
        buf.reserve(1 + 8 + hdr.ksize + hdr.vsize);
        buf.push_back(hdr.type);

        auto push_u32 = [&](uint32_t x) {
            uint8_t b[4];
            std::memcpy(b, &x, 4);
            buf.insert(buf.end(), b, b + 4);
        };

        push_u32(hdr.ksize);
        push_u32(hdr.vsize);
        buf.insert(buf.end(),
                   reinterpret_cast<const uint8_t*>(key.data()),
                   reinterpret_cast<const uint8_t*>(key.data()) + hdr.ksize);
        if (hdr.type == 1 && hdr.vsize) {
            buf.insert(buf.end(),
                       reinterpret_cast<const uint8_t*>(value.data()),
                       reinterpret_cast<const uint8_t*>(value.data()) + hdr.vsize);
        }

        const uint32_t chk = fnv1a_32(buf.data(), buf.size());
        if (chk != hdr.checksum) {
            // Corrupt record => stop safely (do NOT apply garbage)
            break;
        }

        if (hdr.type == 1) db.apply_put(key, value);
        else db.apply_delete(key);
    }
}

void WAL::reset() {
    out_.close();
    std::filesystem::remove(path_);
    out_.open(path_, std::ios::binary | std::ios::app);
}