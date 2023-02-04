// Inspired by https://habr.com/ru/post/714524/
#undef NDEBUG

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
#include <icu.h>
#endif

using namespace std;

using u64 = uint64_t;

auto start_t = chrono::system_clock::now();

#if 0
const char* source = "D:\\tmp\\small.txt";
const char* destination = "D:\\tmp\\output.txt";
#else
const char* source = "D:\\tmp\\bigdata.txt";
const char* destination = "D:\\tmp\\output.txt";
#endif

u64 get_fsize(const char* filename) {
    ifstream ifs(filename, ios::binary);
    assert(ifs);
    ifs.seekg(0, ios::end);
    assert(ifs);
    return ifs.tellg();
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

struct UTFCmp {
    UCollator* uc;
    UTFCmp(): uc(nullptr) {
        UErrorCode ec = UErrorCode::U_ZERO_ERROR;
        uc = ucol_open("en_US", &ec);
        assert(uc != nullptr);
        assert(ec <= 0); // negative codes not an error
    }
    ~UTFCmp() {
        if (uc) {
            ucol_close(uc);
        }
    }
    bool operator ()(string_view lhs, string_view rhs) const {
        UErrorCode ec = UErrorCode::U_ZERO_ERROR;
        auto res = ucol_strcollUTF8(
                uc,
                lhs.data(), lhs.size(),
                rhs.data(), rhs.size(),
                &ec
        );
        assert(ec <= 0);
        return res == UCOL_LESS;
    }
} utfCmp;

void create_output() {
    vector<u64> idx(vec_keys.size());
    for (u64 i = 0; i < idx.size(); ++i)
        idx[i] = i;
    sort(idx.begin(), idx.end(), [&] (u64 i, u64 j) {
        return utfCmp(vec_keys[i], vec_keys[j]);
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
    string_view sep = ". ", end = "\n";
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

struct buffered_input {
    array<char, 1 << 24> buf;
    ifstream ifs;
    buffered_input(const char* filename) : buf(), ifs(filename, ios::binary) {
    }
    string_view next() {
        if (ifs) {
            ifs.read(buf.data(), buf.size());
            auto count = ifs.gcount();
            streamoff off = 0;
            while (count + off > 0 && buf[count + off - 1] != '\n') {
                off -= 1;
            }
            ifs.seekg(off, ios::cur);
            return string_view{buf.data(), buf.data() + count + off};
        }
        return string_view{};
    }
};

void process_lines(u64 total_bytes) {
    // just reserve *VIRTUAL* memory,
    // to avoid rellocations and do not calc number of lines;
    out_idx.reserve(total_bytes / 5);
    auto fin = make_unique<buffered_input>(source);
    u64 line = 0;
    for (string_view data = fin->next(); !data.empty(); data = fin->next()) {
        for (u64 p = 0, p1 = 0, p2 = 0; p < data.size(); ++p) {
            if (data[p] == '\n') {
                auto key = get_key(data.substr(p1, p - p1));
                uint32_t e_key;
                auto[ptr, ec] = std::from_chars(data.data() + p2, data.data() + p1 - 2, e_key);
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
    }
    cerr << "all_keys=" << all_keys.size() << '\n';
    cerr << "out_idx=" << out_idx.size() << '\n';
}

int main() {
    u64 total_size = get_fsize(source);
    cerr << "total_input=" << total_size << '\n';
    process_lines(total_size);
    cerr << "total_proc=" << clock() * 1000.0 / CLOCKS_PER_SEC << " ms.\n";
    cerr << "sys=" << chrono::duration_cast<chrono::milliseconds>(chrono::system_clock::now() - start_t).count() << " ms.\n";
    create_output();
    cerr << "total_output=" << clock() * 1000.0 / CLOCKS_PER_SEC << " ms.\n";
    cerr << "sys=" << chrono::duration_cast<chrono::milliseconds>(chrono::system_clock::now() - start_t).count() << " ms.\n";
    return 0;
}
