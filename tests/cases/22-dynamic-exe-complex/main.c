// B2-3: 复杂调用场景 - 主程序
// 以非字母顺序调用多个函数，严格测试PLT offset计算

extern int func_a(int);
extern int func_b(int);
extern int func_c(int);
extern int func_d(int);
extern int func_e(int);

// --- Merge from Case 19 ---
extern int weak_default(int);
extern int strong_func(int);
extern int get_weak_value(void);
// --------------------------

int main()
{
    // 以不同顺序调用，测试PLT stub的正确性
    int r1 = func_e(10); // 10 + 4 = 14
    int r2 = func_a(5); // 5 + 0 = 5
    int r3 = func_c(7); // 7 + 2 = 9
    int r4 = func_b(3); // 3 + 1 = 4
    int r5 = func_d(2); // 2 + 3 = 5

    // 再次调用，验证多次调用的正确性
    int r6 = func_a(1); // 1 + 0 = 1
    int r7 = func_e(0); // 0 + 4 = 4

    // --- Merge from Case 19: Check Weak Symbols ---
    // strong_func(5) = weak_default(5) * 2 = (5 + 100) * 2 = 210
    int w1 = strong_func(5);
    // weak_default(10) = 10 + 100 = 110
    int w2 = weak_default(10);
    // get_weak_value() = 999
    int w3 = get_weak_value();
    // ----------------------------------------------

    // 总和: 14 + 5 + 9 + 4 + 5 + 1 + 4 = 42
    int sum_funcs = r1 + r2 + r3 + r4 + r5 + r6 + r7;

    // 验证结果
    if (sum_funcs == 42 && w1 == 210 && w2 == 110 && w3 == 999) {
        return 0; // 成功
    }
    return 1; // 失败
}
