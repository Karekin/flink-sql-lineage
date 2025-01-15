package com.hw.lineage.flink.common;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * @description: 自定义标量函数（ScalarFunction）MySuffixFunction，用于在字符串末尾追加固定后缀。
 * 此函数可用于 SQL 查询中对字符串字段进行处理。
 */
public class MySuffixFunction extends ScalarFunction {

    /**
     * 自定义函数逻辑的实现。
     * <p>
     * 此方法会接收一个字符串参数，将 "-HamaWhite" 作为后缀追加到输入字符串的末尾。
     * <p>
     * 示例：
     * 输入 "example"，返回 "example-HamaWhite"。
     *
     * @param input 输入的字符串
     * @return 在输入字符串末尾追加后缀后的新字符串
     */
    public String eval(String input) {
        // 调用 String 的 concat 方法，将 "-HamaWhite" 追加到输入字符串的末尾
        return input.concat("-HamaWhite");
    }
}

