#pragma once

#include <string>
#include <optional>
#include <vector>
#include <cstdint>

class SSTable {
public:
    // get() returns:
    // - nullopt => not found in this table
    // - optional<string> == nullopt => tombstone
    // - optional<string> == value => found value
    explicit SSTable(const std::string& path);

    std::optional<std::optional<std::string>> get(const std::string& key) const;

    // entries must be sorted by key ascending
    static void write_atomic(
        const std::string& final_path,
        const std::vector<std::pair<std::string, std::optional<std::string>>>& entries
    );

    // Validate file footer and checksum. If invalid => treat as corrupt.
    static bool is_valid(const std::string& path);

private:
    struct IndexEntry {
        std::string key;
        uint64_t offset;
    };

    std::string path_;
    std::vector<IndexEntry> index_;
    static constexpr uint32_t kIndexStride = 16;

    static bool read_record_at(
        const std::string& path,
        uint64_t offset,
        std::string& out_key,
        std::optional<std::string>& out_value,
        uint64_t& out_next_offset
    );

    static uint32_t fnv1a_32(const uint8_t* data, size_t n);
};