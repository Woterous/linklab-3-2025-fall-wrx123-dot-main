#pragma once

#include <functional>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

class ArgParser {
public:
    // 特殊异常：用户请求帮助
    struct HelpRequested : public std::exception { };

    ArgParser(std::string name)
        : program_name(std::move(name))
    {
        // 自动注册 help
        add_flag(internal_help_flag, "-h, --help", "Show this help message");
    }

    // ==========================================
    // 核心 API：回调模式 (最灵活)
    // ==========================================

    // 注册 Flag (无值)，触发时执行 callback
    void add_flag_cb(const std::string& flags, const std::string& help, std::function<void()> callback)
    {
        auto names = split_names(flags);
        for (const auto& name : names) {
            flag_map[name] = callback;
            register_help(names, help);
        }
    }

    // 注册 Option (带值)，触发时执行 callback
    void add_option_cb(const std::string& flags, const std::string& help, std::function<void(std::string)> callback)
    {
        auto names = split_names(flags);
        for (const auto& name : names) {
            option_map[name] = callback;
            register_help(names, help);
            if (is_short_opt(name))
                short_options.push_back(name[1]);
        }
    }

    // 注册位置参数的处理逻辑 (默认是存入 positional_args)
    void on_positional(std::function<void(std::string)> callback)
    {
        positional_callback = callback;
    }

    // ==========================================
    // 语法糖 API：变量绑定 (最方便)
    // ==========================================

    // 绑定 bool 变量
    void add_flag(bool& target, const std::string& flags, const std::string& help)
    {
        add_flag_cb(flags, help, [&target]() { target = true; });
    }

    // 绑定 string 变量
    void add_option(std::string& target, const std::string& flags, const std::string& help)
    {
        add_option_cb(flags, help, [&target](std::string val) { target = val; });
    }

    // 绑定 vector<string> (收集多次出现的参数，如 -L)
    void add_multi_option(std::vector<std::string>& target, const std::string& flags, const std::string& help)
    {
        add_option_cb(flags, help, [&target](std::string val) { target.push_back(val); });
    }

    // ==========================================
    // 解析逻辑
    // ==========================================

    void parse(const std::vector<std::string>& args)
    {
        for (size_t i = 0; i < args.size(); ++i) {
            const std::string& arg = args[i];

            if (arg[0] == '-') {
                // 1. 检查是否是 Flag
                if (flag_map.count(arg)) {
                    flag_map[arg]();
                }
                // 2. 检查是否是 Option (完整匹配)
                else if (option_map.count(arg)) {
                    if (i + 1 < args.size()) {
                        option_map[arg](args[++i]);
                    } else {
                        throw std::runtime_error("Option " + arg + " requires an argument");
                    }
                }
                // 3. 检查是否是 粘连 Option (如 -lmath)
                else {
                    bool handled = false;
                    for (char c : short_options) {
                        std::string prefix = std::string("-") + c;
                        // 匹配前缀且长度大于2 (例如 -lxxx)
                        if (arg.find(prefix) == 0 && arg.size() > 2) {
                            std::string value = arg.substr(2);
                            option_map[prefix](value);
                            handled = true;
                            break;
                        }
                    }
                    if (!handled)
                        throw std::runtime_error("Unknown option: " + arg);
                }
            } else {
                // 4. 位置参数
                if (positional_callback) {
                    positional_callback(arg);
                } else {
                    default_positional_args.push_back(arg);
                }
            }
        }

        if (internal_help_flag) {
            print_help();
            throw HelpRequested();
        }
    }

    // 如果没设置 on_positional，可以用这个获取
    const std::vector<std::string>& positional() const { return default_positional_args; }

private:
    std::string program_name;
    bool internal_help_flag = false;
    std::vector<char> short_options;

    // 默认的位置参数存储
    std::vector<std::string> default_positional_args;
    // 自定义位置参数回调
    std::function<void(std::string)> positional_callback;

    std::unordered_map<std::string, std::function<void()>> flag_map;
    std::unordered_map<std::string, std::function<void(std::string)>> option_map;

    struct HelpEntry {
        std::string flags;
        std::string desc;
    };
    std::vector<HelpEntry> help_entries;

    void register_help(const std::vector<std::string>& names, const std::string& help)
    {
        std::string primary = names[0];
        for (const auto& entry : help_entries) {
            // 简单去重：如果这一组Flag的主名已经存在，就不加了
            if (entry.flags.find(primary) != std::string::npos)
                return;
        }
        std::string flag_str;
        for (size_t i = 0; i < names.size(); ++i) {
            flag_str += names[i];
            if (i < names.size() - 1)
                flag_str += ", ";
        }
        help_entries.push_back({ flag_str, help });
    }

    void print_help() const
    {
        std::cerr << "Usage: " << program_name << " [options] <inputs...>\n\nOptions:\n";
        for (const auto& entry : help_entries) {
            std::cerr << "  " << std::left << std::setw(25) << entry.flags << entry.desc << "\n";
        }
        std::cerr << std::endl;
    }

    std::vector<std::string> split_names(const std::string& s)
    {
        std::vector<std::string> res;
        std::stringstream ss(s);
        std::string item;
        while (std::getline(ss, item, ',')) {
            size_t first = item.find_first_not_of(" ");
            if (first == std::string::npos)
                continue;
            size_t last = item.find_last_not_of(" ");
            res.push_back(item.substr(first, (last - first + 1)));
        }
        return res;
    }

    bool is_short_opt(const std::string& s) { return s.size() == 2 && s[0] == '-'; }
};
