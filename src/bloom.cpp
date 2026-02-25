#include "bloom.hpp"
#include <fstream>
#include <stdexcept>
#include <cstring>

BloomFilter::BloomFilter(uint32_t m_bits, uint32_t k_hashes)
    : m_bits_(m_bits), k_hashes_(k_hashes) {
    if (m_bits_ == 0 || k_hashes_ == 0) {
        m_bits_ = 0; k_hashes_ = 0; bits_.clear();
        return;
    }
    const uint32_t nbytes = (m_bits_ + 7) / 8;
    bits_.assign(nbytes, 0);
}

uint64_t BloomFilter::fnv1a_64(const uint8_t* data, size_t n) {
    uint64_t h = 1469598103934665603ULL;
    for (size_t i = 0; i < n; ++i) {
        h ^= data[i];
        h *= 1099511628211ULL;
    }
    return h;
}

uint64_t BloomFilter::hash64(const std::string& s, uint64_t seed) {
    uint64_t h = seed;
    h ^= fnv1a_64(reinterpret_cast<const uint8_t*>(s.data()), s.size());
    // mix
    h ^= (h >> 33);
    h *= 0xff51afd7ed558ccdULL;
    h ^= (h >> 33);
    h *= 0xc4ceb9fe1a85ec53ULL;
    h ^= (h >> 33);
    return h;
}

void BloomFilter::set_bit(uint32_t idx) {
    idx %= m_bits_;
    bits_[idx / 8] |= static_cast<uint8_t>(1u << (idx % 8));
}

bool BloomFilter::get_bit(uint32_t idx) const {
    idx %= m_bits_;
    return (bits_[idx / 8] & static_cast<uint8_t>(1u << (idx % 8))) != 0;
}

void BloomFilter::add(const std::string& key) {
    if (m_bits_ == 0 || k_hashes_ == 0) return;

    // double hashing: h1 + i*h2
    const uint64_t h1 = hash64(key, 0xA5A5A5A5A5A5A5A5ULL);
    const uint64_t h2 = hash64(key, 0x5A5A5A5A5A5A5A5AULL) | 1ULL;

    for (uint32_t i = 0; i < k_hashes_; ++i) {
        uint64_t h = h1 + static_cast<uint64_t>(i) * h2;
        set_bit(static_cast<uint32_t>(h % m_bits_));
    }
}

bool BloomFilter::possibly_contains(const std::string& key) const {
    if (m_bits_ == 0 || k_hashes_ == 0) return true; // conservative

    const uint64_t h1 = hash64(key, 0xA5A5A5A5A5A5A5A5ULL);
    const uint64_t h2 = hash64(key, 0x5A5A5A5A5A5A5A5AULL) | 1ULL;

    for (uint32_t i = 0; i < k_hashes_; ++i) {
        uint64_t h = h1 + static_cast<uint64_t>(i) * h2;
        if (!get_bit(static_cast<uint32_t>(h % m_bits_))) return false;
    }
    return true;
}

void BloomFilter::save(const std::string& path) const {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) throw std::runtime_error("Failed to open bloom file: " + path);

    const uint32_t magic = 0xB100B100u;
    const uint32_t nbytes = static_cast<uint32_t>(bits_.size());
    out.write(reinterpret_cast<const char*>(&magic), 4);
    out.write(reinterpret_cast<const char*>(&m_bits_), 4);
    out.write(reinterpret_cast<const char*>(&k_hashes_), 4);
    out.write(reinterpret_cast<const char*>(&nbytes), 4);
    if (nbytes) out.write(reinterpret_cast<const char*>(bits_.data()), nbytes);
    out.flush();
}

BloomFilter BloomFilter::load(const std::string& path, bool& ok) {
    ok = false;
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) return BloomFilter();

    uint32_t magic=0, m=0, k=0, nbytes=0;
    in.read(reinterpret_cast<char*>(&magic), 4);
    in.read(reinterpret_cast<char*>(&m), 4);
    in.read(reinterpret_cast<char*>(&k), 4);
    in.read(reinterpret_cast<char*>(&nbytes), 4);
    if (!in || magic != 0xB100B100u) return BloomFilter();

    BloomFilter bf(m, k);
    if (nbytes != bf.bits_.size()) return BloomFilter();

    if (nbytes) {
        in.read(reinterpret_cast<char*>(bf.bits_.data()), nbytes);
        if (!in) return BloomFilter();
    }

    ok = true;
    return bf;
}
