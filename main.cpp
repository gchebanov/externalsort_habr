// Inspired by https://habr.com/ru/post/714524/

#include <string_view>
#include <algorithm>
#include <charconv>
#include <iostream>
#include <fstream>
#include <cassert>
#include <vector>
#include <chrono>
#include <array>
#include <ctime>
#include <map>
#if _WIN32
#include <windows.h>
#endif

using namespace std;

using u64 = uint64_t;

auto start_t = chrono::system_clock::now();

#if 0
const char* source = "D:\\tmp\\big.txt";
const char* destination = "D:\\tmp\\out.txt";
#else
const char* source = "D:\\tmp\\bigdata.txt";
const char* destination = "D:\\tmp\\output.txt";
#endif
const char* tmp = "D:\\tmp\\";

string_view get_mmaped(const char* filename) {
    ifstream ifs(filename, ios::binary);
    assert(ifs);
    ifs.seekg(0, ios::end);
    assert(ifs);
    auto sz = ifs.tellg();
    ifs.seekg(0, ios::beg);
    ifs.close();
#if _WIN32
    auto err = ::GetLastError();
    assert(err == 0);
    auto fhd = ::CreateFile(filename, GENERIC_READ, 0, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    err = ::GetLastError();
    assert(err == 0);
    assert(fhd);
    auto mhd = ::CreateFileMapping(fhd, NULL, PAGE_READONLY, 0, 0, NULL);
    err = ::GetLastError();
    assert(err == 0);
    assert(mhd);
    auto raw = reinterpret_cast<const char*>(::MapViewOfFile(mhd, FILE_MAP_READ, 0, 0, sz));
    err = ::GetLastError();
    assert(err == 0);
    assert(raw);
    return string_view{raw, raw + sz};
#endif
}

u64 get_lines(string_view data) {
    u64 n_lines = 0;
    for (char c: data) if (c == '\n') ++n_lines;
    return n_lines;
}

map<string, u64, less<>> all_keys;
vector<string> vec_keys;

struct fstring {
    array<char, 10> s;
    fstring() { s[0] = '\0'; }
    fstring(string_view data) {
        assert(data.size() <= s.size());
        memcpy(s.data(), data.data(), data.size());
        if (data.size() < s.size()) {
            s[data.size()] = '\0';
        }
    }
    operator string_view() const {
        return s.back() == '\0' ? string_view(s.data()) : string_view(s.begin(), s.end());
    }
    bool operator < (const fstring& other) const { return false; }
    bool operator == (const fstring& other) const { return false; }
};

vector<tuple<u64, uint32_t, fstring>> out_idx;

struct buffered_output {
    array<char, 1<<20> buf;
    int pos;
    ofstream ofs;
    u64 total_write;

    buffered_output(const char* filename): buf(), pos(0), ofs(filename, ios::binary), total_write(0) {
    }

    ~buffered_output() {
        flush();
        cerr << "total_write=" << total_write << '\n';
    }

    void flush() {
        total_write += pos;
        ofs.write(buf.data(), pos);
        pos = 0;
    }

    void append(string_view data) {
        if (pos + data.size() > buf.size())
            flush();
        assert(data.size() <= buf.size());
        memcpy(buf.data() + pos, data.data(), data.size());
        pos += data.size();
    }
};

void create_output() {
    vector<u64> idx(vec_keys.size());
    for (u64 i = 0; i < idx.size(); ++i)
        idx[i] = i;
    sort(idx.begin(), idx.end(), [&] (u64 i, u64 j) {
        return vec_keys[i] < vec_keys[j]; // TODO add utf-8 compare
    });
    vector<u64> inv_idx(idx.size());
    for (u64 i = 0; i < idx.size(); ++i) {
        inv_idx[idx[i]] = i;
    }
    for (auto& e: out_idx) {
        get<0>(e) = inv_idx[get<0>(e)];
    }
    sort(out_idx.begin(), out_idx.end());
    auto ofs = make_unique<buffered_output>(destination);
    string_view sep = ": ", end = "\n";
    for (auto [i, e_key, o_key]: out_idx) {
        ofs->append(o_key);
        ofs->append(sep);
        ofs->append(vec_keys[idx[i]]);
        ofs->append(end);
    }
}

auto get_key(string_view key) {
    auto it = all_keys.find(key);
    if (it != all_keys.end()) {
        return it->second;
    }
    auto pit = all_keys.emplace(key, all_keys.size());
    assert(pit.second);
    if (pit.second) {
        vec_keys.emplace_back(key);
    }
    return pit.first->second;
}

void process_lines(string_view data, u64 n_lines) {
    out_idx.reserve(n_lines);
    cerr << (out_idx.capacity() * sizeof(out_idx[0]) >> 20) << " extra Mib\n";
    u64 line = 0;
    for (u64 p = 0, p1 = 0, p2 = 0; p < data.size(); ++p) {
        if (data[p] == '\n') {
            auto key = get_key(data.substr(p1, p - p1));
            size_t stoi_ptr;
            uint32_t e_key;
            auto [ptr, ec] = std::from_chars(data.data() + p2, data.data() + p1 - 2, e_key);
            assert(ec == errc{0});
            assert(ptr == data.data() + p1 - 2);
            out_idx.emplace_back(key, e_key, data.substr(p2, p1 - 2 - p2));
            ++line;
            p2 = p + 1;
        } else if (data[p] == '.') {
            if (p1 <= p2) {
                assert(data[p + 1] == ' ');
                p1 = p + 2;
            }
        }
    }
    cerr << "all_keys=" << all_keys.size() << '\n';
}

int main() {
    auto data = get_mmaped(source);
    cerr << (uintptr_t)(data.data()) << ' ' << data.size() << '\n';
    if (0) {
        u64 cut = 1e9;
        while (data[cut] != '\n') ++cut;
        data = data.substr(0, cut + 1);
    }
    cerr << "total_input=" << data.size() << '\n';
//    auto n_lines = get_lines(data);
    auto n_lines = 463523186ULL;
    cerr << "lines=" << n_lines << '\n';
    process_lines(data, n_lines);
    cerr << "total_proc=" << clock() * 1000.0 / CLOCKS_PER_SEC << " ms.\n";
    cerr << "sys=" << chrono::duration_cast<chrono::milliseconds>(chrono::system_clock::now() - start_t).count() << " ms.\n";
    create_output();
    cerr << "total_output=" << clock() * 1000.0 / CLOCKS_PER_SEC << " ms.\n";
    cerr << "sys=" << chrono::duration_cast<chrono::milliseconds>(chrono::system_clock::now() - start_t).count() << " ms.\n";
    return 0;
}
