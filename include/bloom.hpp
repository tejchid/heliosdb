#pragma once
#include <cstdint>
#include <string>
#include <vector>

class BloomFilter {
public:
    BloomFilter() = default;

    // Create with m bits and k hash functions
    BloomFilter(uint32_t m_bits, uint32_t k_hashes);

    void add(const std::string& key);
    bool possibly_contains(const std::string& key) const;

    // Serialize/deserialize to sidecar file
    void save(const std::string& path) const;
    static BloomFilter load(const std::string& path, bool& ok);

    uint32_t m_bits() const { return m_bits_; }
    uint32_t k_hashes() const { return k_hashes_; }

private:
    uint32_t m_bits_{0};
    uint32_t k_hashes_{0};
    std::vector<uint8_t> bits_; // packed bits

    static uint64_t fnv1a_64(const uint8_t* data, size_t n);
    static uint64_t hash64(const std::string& s, uint64_t seed);

    void set_bit(uint32_t idx);
    bool get_bit(uint32_t idx) const;
};
