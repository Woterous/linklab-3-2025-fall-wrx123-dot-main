#include "fle.hpp"
#include <iomanip>
#include <iostream>
#include <cstdio>
#include <cstring>

void FLE_nm(const FLEObject& obj)
{
    for (const Symbol& sym : obj.symbols) {

        /* 1. 不显示未定义符号 */
        if (sym.section.empty())
            continue;

        char type = '?';
        bool is_global = (sym.type == SymbolType::GLOBAL);
        bool is_weak   = (sym.type == SymbolType::WEAK);

        const char* sec = sym.section.c_str();

        /* 2. 判断节类型 */
        if (strncmp(sec, ".text", 5) == 0) {
            if (is_weak)
                type = 'W';
            else
                type = is_global ? 'T' : 't';
        }
        else if (strncmp(sec, ".data", 5) == 0) {
            if (is_weak)
                type = 'V';
            else
                type = is_global ? 'D' : 'd';
        }
        else if (strncmp(sec, ".bss", 4) == 0) {
            if (is_weak)
                type = 'V';
            else
                type = is_global ? 'B' : 'b';
        }
        else if (strncmp(sec, ".rodata", 7) == 0) {
            type = is_global ? 'R' : 'r';
        }
        else {
            continue;
        }

      
        printf("%016lx %c %s\n",
               sym.offset,
               type,
               sym.name.c_str());
    }
}
