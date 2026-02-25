#pragma once

#include <string>
#include <optional>
#include <vector>
#include <cstdint>
#include <memory>

#include "bloom.hpp"

class SSTable {
public:
    // get() returns:
    // - nullopt => not found in this table
    // - optional<string> == nullopt => tombstone
    // - optional<string> == value => found value
    explicit SSTable(const std::string& path);
    ~SSTable();

    std::optional<std::optional<std::string>> get(const std::string& key) const;

    // entries must be sorted by key ascending
    static void write_atomic(
        const std::string& final_path,
        const std::vector<std::pair<std::string, std::optional<std::string>>>& entries
    );

    static bool is_valid(const std::string& path);

private:
    struct IndexEntry {
        std::string key;
        uint64_t offset;
    };

    std::string path_;
    int fd_{-1};
    uint64_t end_{0}; // end of records region (exclude footer)
    bool valid_{false};

    std::vector<IndexEntry> index_;
    static constexpr uint32_t kIndexStride = 16;

    BloomFilter bloom_;
    bool bloom_ok_{false};

    static constexpr uint32_t TOMBSTONE_VSIZE = 0xFFFFFFFFu;

    static uint32_t fnv1a_32(const uint8_t* data, size_t n);

    bool pread_all(void* buf, size_t n, uint64_t off) const;

    bool read_record_at(
        uint64_t offset,
        std::string& out_key,
        std::optional<std::string>& out_value,
        uint64_t& out_next_offset
    ) const;

    static std::string bloom_path_for(const std::string& sstable_path);
};
