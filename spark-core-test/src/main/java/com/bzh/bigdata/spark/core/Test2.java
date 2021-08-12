package com.bzh.bigdata.spark.core;

import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: bizh
 * @Date: 2021/8/11 14:04
 * @Description:
 */
public class Test2 {

    public static void main(String[] args) {
        String a = "aa";
        String b = "aa";

        System.out.println(a == b);
        System.out.println(a.equals(b));
        System.out.println(5 == 5);
        System.out.println("===============================");

        int[] oldArr = new int[]{1, 2, 3, 4};

        int[] newArr = Arrays.copyOf(oldArr, 10);

        System.out.println("oldArr地址：" + oldArr + "\tnewArr地址：" + newArr);

        System.out.println(Arrays.toString(oldArr));
        System.out.println(Arrays.toString(newArr));

        System.out.println("===============================");
        System.out.println(binary2Decimal("01001"));
        System.out.println(binary2Decimal("01101"));
        System.out.println(binary2Decimal("01100"));
        System.out.println(binary2Decimal("01111"));

        System.out.println("===============================");
        System.out.println(15&18);
        System.out.println(15&19);
        System.out.println(15&29);
        System.out.println(15&31);
        System.out.println(15&32);
        System.out.println(15&33);
        System.out.println(15&34);

        // hashmap进行扩容长度增长2倍为32
        System.out.println("-----------------");
        System.out.println(31&18);
        System.out.println(31&34);

    }

    /**
     * 二进制转十进制
     * @param number
     * @return
     */
    public static int binary2Decimal(String number) {
        return scale2Decimal(number, 2);
    }

    /**
     * 其他进制转十进制
     * @param number
     * @return
     */
    public static int scale2Decimal(String number, int scale) {
        checkNumber(number);
        if (2 > scale || scale > 32) {
            throw new IllegalArgumentException("scale is not in range");
        }
        // 不同其他进制转十进制,修改这里即可
        int total = 0;
        String[] ch = number.split("");
        int chLength = ch.length;
        for (int i = 0; i < chLength; i++) {
            total += Integer.valueOf(ch[i]) * Math.pow(scale, chLength - 1 - i);
        }
        return total;

    }

    /**
     * 二进制转十进制
     * @param number
     * @return
     */
    public static String decimal2Binary(int number) {
        return decimal2Scale(number, 2);
    }

    /**
     * 十进制转其他进制
     * @param number
     * @param scale
     * @return
     */
    public static String decimal2Scale(int number, int scale) {
        if (2 > scale || scale > 32) {
            throw new IllegalArgumentException("scale is not in range");
        }
        String result = "";
        while (0 != number) {
            result = number % scale + result;
            number = number / scale;
        }

        return result;
    }

    public static void checkNumber(String number) {
        String regexp = "^\\d+$";
        if (null == number || !number.matches(regexp)) {
            throw new IllegalArgumentException("input is not a number");
        }
    }
}
