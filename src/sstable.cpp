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

static constexpr uint32_t TOMBSTONE_VSIZE = std::numeric_limits<uint32_t>::max();
static constexpr uint64_t FOOTER_MAGIC = 0x48454C494F535354ULL; // "HELIOSST"

#pragma pack(push, 1)
struct Footer {
    uint64_t magic;
    uint32_t checksum;
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

SSTable::SSTable(const std::string& path)
    : path_(path)
{
    if (!is_valid(path_)) return;

    std::ifstream in(path_, std::ios::binary);
    if (!in.is_open()) return;

    auto total = std::filesystem::file_size(path_);
    uint64_t end = static_cast<uint64_t>(total - sizeof(Footer));

    uint64_t offset = 0;
    uint32_t count = 0;

    while (offset < end) {
        in.seekg(offset);

        uint32_t ksize = 0, vsize = 0;
        in.read(reinterpret_cast<char*>(&ksize), sizeof(ksize));
        if (!in) break;
        in.read(reinterpret_cast<char*>(&vsize), sizeof(vsize));
        if (!in) break;

        if (offset + 8ULL + ksize > end) break;

        std::string key(ksize, '\0');
        in.read(key.data(), ksize);
        if (!in) break;

        if (vsize != TOMBSTONE_VSIZE) {
            if (offset + 8ULL + ksize + vsize > end) break;
            in.seekg(vsize, std::ios::cur);
            if (!in) break;
        }

        if (count % kIndexStride == 0) {
            index_.push_back({key, offset});
        }
        count++;

        offset = static_cast<uint64_t>(in.tellg());
        if (in.fail()) break;
    }
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

    for (const auto& [k, v] : entries) {
        uint32_t ksize = static_cast<uint32_t>(k.size());
        uint32_t vsize = v ? static_cast<uint32_t>(v->size()) : TOMBSTONE_VSIZE;

        out.write(reinterpret_cast<const char*>(&ksize), 4);
        out.write(reinterpret_cast<const char*>(&vsize), 4);
        out.write(k.data(), ksize);

        fnv_feed(&ksize, 4);
        fnv_feed(&vsize, 4);
        fnv_feed(k.data(), ksize);

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
}

bool SSTable::read_record_at(
    const std::string& path,
    uint64_t offset,
    std::string& out_key,
    std::optional<std::string>& out_value,
    uint64_t& out_next_offset
) {
    if (!is_valid(path)) return false;

    auto total = std::filesystem::file_size(path);
    uint64_t end = total - sizeof(Footer);
    if (offset >= end) return false;

    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) return false;

    in.seekg(offset);

    uint32_t ksize = 0, vsize = 0;
    in.read(reinterpret_cast<char*>(&ksize), 4);
    if (!in) return false;
    in.read(reinterpret_cast<char*>(&vsize), 4);
    if (!in) return false;

    if (offset + 8ULL + ksize > end) return false;

    out_key.assign(ksize, '\0');
    in.read(out_key.data(), ksize);
    if (!in) return false;

    if (vsize == TOMBSTONE_VSIZE) {
        out_value = std::nullopt;
    } else {
        if (offset + 8ULL + ksize + vsize > end) return false;
        std::string val(vsize, '\0');
        in.read(val.data(), vsize);
        if (!in) return false;
        out_value = std::move(val);
    }

    out_next_offset = static_cast<uint64_t>(in.tellg());
    return !in.fail();
}

std::optional<std::optional<std::string>> SSTable::get(const std::string& key) const {
    if (index_.empty()) return std::nullopt;

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

        if (!read_record_at(path_, off, k, v, next)) break;
        if (k == key) return v;
        if (k > key) return std::nullopt;
        off = next;
    }

    return std::nullopt;
}