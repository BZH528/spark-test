package com.bzh.bigdata;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: bizh
 * @Date: 2021/5/14 17:46
 * @Description:
 */
public class Test2 {

    public static void main(String[] args) {

        String s = "pay_success";
        if (!s.equals("pay_success") && s.equals("pay_success")) {
            System.out.println("true");
        } else {
            System.out.println("false");
        }
    }
}
