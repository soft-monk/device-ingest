// tools 目录下的开发期小工具源码（不参与构建）：仓库文件编码体检。
// 用途：确认所有 .h/.cc/.md/.json 都是"无 BOM 的 UTF-8"，
// 避免 Windows 控制台代码页（如 CP936）把中文源码读坏（本次踩过的坑见 doc/构建与排障.md）。
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

static bool isUtf8NoBom(const fs::path& p, std::string& why) {
    std::ifstream in(p, std::ios::binary);
    if (!in) { why = "打不开"; return false; }
    std::string s((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    if (s.size() >= 3 && static_cast<unsigned char>(s[0]) == 0xEF &&
        static_cast<unsigned char>(s[1]) == 0xBB && static_cast<unsigned char>(s[2]) == 0xBF) {
        why = "带 UTF-8 BOM";
        return false;
    }
    // 逐字节校验 UTF-8 合法性
    for (std::size_t i = 0; i < s.size();) {
        const unsigned char c = static_cast<unsigned char>(s[i]);
        std::size_t extra = 0;
        if (c < 0x80) extra = 0;
        else if ((c & 0xE0) == 0xC0) extra = 1;
        else if ((c & 0xF0) == 0xE0) extra = 2;
        else if ((c & 0xF8) == 0xF0) extra = 3;
        else { why = "非 UTF-8 起始字节 @ " + std::to_string(i); return false; }
        if (i + extra >= s.size()) { why = "UTF-8 截断 @ " + std::to_string(i); return false; }
        for (std::size_t k = 1; k <= extra; ++k) {
            if ((static_cast<unsigned char>(s[i + k]) & 0xC0) != 0x80) {
                why = "UTF-8 续字节非法 @ " + std::to_string(i + k);
                return false;
            }
        }
        i += extra + 1;
    }
    return true;
}

int main(int argc, char** argv) {
    const fs::path root = argc > 1 ? argv[1] : ".";
    std::vector<std::string> exts = {".h", ".cc", ".md", ".json", ".txt", ".cmake"};
    int bad = 0, total = 0;
    for (auto it = fs::recursive_directory_iterator(root);
         it != fs::recursive_directory_iterator(); ++it) {
        if (!it->is_regular_file()) continue;
        const fs::path p = it->path();
        const std::string name = p.filename().string();
        if (name == "CMakeLists.txt") { /* 下面按扩展名统一处理 */ }
        bool want = false;
        for (const auto& e : exts) {
            if (p.extension() == e) { want = true; break; }
        }
        if (name == "CMakeLists.txt") want = true;
        if (!want) continue;
        const std::string s = p.string();
        if (s.find("build") != std::string::npos && s.find("third_party") == std::string::npos) {
            if (s.find("\\build\\") != std::string::npos || s.find("/build/") != std::string::npos) continue;
        }
        ++total;
        std::string why;
        if (!isUtf8NoBom(p, why)) {
            ++bad;
            std::printf("[BAD ] %s  (%s)\n", s.c_str(), why.c_str());
        }
    }
    std::printf("检查 %d 个文件，%d 个不合格\n", total, bad);
    return bad == 0 ? 0 : 1;
}
