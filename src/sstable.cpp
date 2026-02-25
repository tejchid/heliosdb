#include "sstable.hpp"

#include <fstream>
#include <filesystem>
#include <algorithm>
#include <limits>
#include <vector>
#include <cstring>
#include <stdexcept>

#if defined(__unix__) || defined(__APPLE__)
#include <fcntl.h>
#include <unistd.h>
#endif

using namespace std;

static constexpr uint64_t FOOTER_MAGIC = 0x48454C494F535354ULL; // "HELIOSST"
#pragma pack(push, 1)
struct Footer {
    uint64_t magic;
    uint32_t checksum; // FNV-1a over records region
};
#pragma pack(pop)

uint32_t SSTable::fnv1a_32(const uint8_t* data, size_t n) {
    uint32_t h = 2166136261u;
    for (size_t i = 0; i < n; ++i) {
        h ^= data[i];
        h *= 16777619u;
    }
    return h;
}

static void fsync_file(const std::string& path) {
#if defined(__unix__) || defined(__APPLE__)
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd >= 0) {
        ::fsync(fd);
        ::close(fd);
    }
#else
    (void)path;
#endif
}

bool SSTable::is_valid(const std::string& path) {
    std::error_code ec;
    auto sz = std::filesystem::file_size(path, ec);
    if (ec || sz < sizeof(Footer)) return false;

    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) return false;

    in.seekg(static_cast<std::streamoff>(sz - sizeof(Footer)), std::ios::beg);
    Footer f{};
    in.read(reinterpret_cast<char*>(&f), sizeof(f));
    if (!in) return false;
    if (f.magic != FOOTER_MAGIC) return false;

    const uint64_t records_len = static_cast<uint64_t>(sz - sizeof(Footer));
    in.seekg(0, std::ios::beg);

    std::vector<uint8_t> buf(records_len);
    in.read(reinterpret_cast<char*>(buf.data()), records_len);
    if (!in) return false;

    uint32_t chk = fnv1a_32(buf.data(), buf.size());
    return chk == f.checksum;
}

std::string SSTable::bloom_path_for(const std::string& sstable_path) {
    return sstable_path + ".bloom";
}

SSTable::SSTable(const std::string& path)
    : path_(path)
{
    valid_ = SSTable::is_valid(path_);
    if (!valid_) return;

#if defined(__unix__) || defined(__APPLE__)
    fd_ = ::open(path_.c_str(), O_RDONLY);
    if (fd_ < 0) { valid_ = false; return; }
#else
    valid_ = false;
    return;
#endif

    auto total = std::filesystem::file_size(path_);
    end_ = static_cast<uint64_t>(total - sizeof(Footer));

    // Load bloom sidecar if exists
    bool ok = false;
    bloom_ = BloomFilter::load(bloom_path_for(path_), ok);
    bloom_ok_ = ok;

    // Build sparse index via single scan using pread
    uint64_t offset = 0;
    uint32_t count = 0;

    while (offset < end_) {
        uint32_t ksize = 0, vsize = 0;
        if (!pread_all(&ksize, 4, offset)) break;
        if (!pread_all(&vsize, 4, offset + 4)) break;

        if (offset + 8ULL + ksize > end_) break;

        std::string key(ksize, '\0');
        if (ksize && !pread_all(key.data(), ksize, offset + 8)) break;

        uint64_t next = offset + 8ULL + ksize;
        if (vsize != TOMBSTONE_VSIZE) {
            if (next + vsize > end_) break;
            next += vsize;
        }

        if (count % kIndexStride == 0) {
            index_.push_back({key, offset});
        }
        count++;
        offset = next;
    }
}

SSTable::~SSTable() {
#if defined(__unix__) || defined(__APPLE__)
    if (fd_ >= 0) ::close(fd_);
#endif
}

bool SSTable::pread_all(void* buf, size_t n, uint64_t off) const {
#if defined(__unix__) || defined(__APPLE__)
    uint8_t* p = reinterpret_cast<uint8_t*>(buf);
    size_t got = 0;
    while (got < n) {
        ssize_t r = ::pread(fd_, p + got, n - got, static_cast<off_t>(off + got));
        if (r <= 0) return false;
        got += static_cast<size_t>(r);
    }
    return true;
#else
    (void)buf; (void)n; (void)off;
    return false;
#endif
}

bool SSTable::read_record_at(
    uint64_t offset,
    std::string& out_key,
    std::optional<std::string>& out_value,
    uint64_t& out_next_offset
) const {
    if (!valid_) return false;
    if (offset >= end_) return false;

    uint32_t ksize = 0, vsize = 0;
    if (!pread_all(&ksize, 4, offset)) return false;
    if (!pread_all(&vsize, 4, offset + 4)) return false;

    if (offset + 8ULL + ksize > end_) return false;

    out_key.assign(ksize, '\0');
    if (ksize && !pread_all(out_key.data(), ksize, offset + 8)) return false;

    uint64_t pos = offset + 8ULL + ksize;
    if (vsize == TOMBSTONE_VSIZE) {
        out_value = std::nullopt;
        out_next_offset = pos;
        return true;
    }

    if (pos + vsize > end_) return false;

    std::string val(vsize, '\0');
    if (vsize && !pread_all(val.data(), vsize, pos)) return false;
    out_value = std::move(val);
    out_next_offset = pos + vsize;
    return true;
}

void SSTable::write_atomic(
    const std::string& final_path,
    const std::vector<std::pair<std::string, std::optional<std::string>>>& entries
) {
    const std::string tmp = final_path + ".tmp";

    std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) throw std::runtime_error("Failed to open SSTable tmp");

    uint32_t chk = 2166136261u;
    auto fnv_feed = [&](const void* p, size_t n) {
        const uint8_t* b = reinterpret_cast<const uint8_t*>(p);
        for (size_t i = 0; i < n; ++i) { chk ^= b[i]; chk *= 16777619u; }
    };

    // Build bloom for this table (tunable):
    // 10 bits/key, 7 hashes is a common-ish baseline.
    const uint32_t m_bits = static_cast<uint32_t>(entries.size() * 10ULL);
    const uint32_t k_hash = 7;
    BloomFilter bloom(m_bits ? m_bits : 8u, k_hash);

    for (const auto& [k, v] : entries) {
        uint32_t ksize = static_cast<uint32_t>(k.size());
        uint32_t vsize = v ? static_cast<uint32_t>(v->size()) : TOMBSTONE_VSIZE;

        out.write(reinterpret_cast<const char*>(&ksize), 4);
        out.write(reinterpret_cast<const char*>(&vsize), 4);
        out.write(k.data(), ksize);

        fnv_feed(&ksize, 4);
        fnv_feed(&vsize, 4);
        fnv_feed(k.data(), ksize);

        bloom.add(k);

        if (vsize != TOMBSTONE_VSIZE) {
            out.write(v->data(), v->size());
            fnv_feed(v->data(), v->size());
        }
    }

    Footer f{FOOTER_MAGIC, chk};
    out.write(reinterpret_cast<const char*>(&f), sizeof(f));
    out.flush();
    out.close();

    fsync_file(tmp);
    std::filesystem::rename(tmp, final_path);
    fsync_file(final_path);

    // Write bloom sidecar atomically too
    const std::string bloom_path = bloom_path_for(final_path);
    const std::string bloom_tmp = bloom_path + ".tmp";
    bloom.save(bloom_tmp);
    fsync_file(bloom_tmp);
    std::filesystem::rename(bloom_tmp, bloom_path);
    fsync_file(bloom_path);
}

std::optional<std::optional<std::string>> SSTable::get(const std::string& key) const {
    if (!valid_ || index_.empty()) return std::nullopt;

    // Bloom fast negative
    if (bloom_ok_ && !bloom_.possibly_contains(key)) {
        return std::nullopt;
    }

    auto it = std::upper_bound(
        index_.begin(), index_.end(), key,
        [](const std::string& k, const IndexEntry& e) { return k < e.key; }
    );

    uint64_t start = (it == index_.begin()) ? index_.front().offset : (--it)->offset;

    uint64_t off = start;
    while (true) {
        std::string k;
        std::optional<std::string> v;
        uint64_t next = 0;

        if (!read_record_at(off, k, v, next)) break;
        if (k == key) return v;
        if (k > key) return std::nullopt;
        off = next;
    }
    return std::nullopt;
}
