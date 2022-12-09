package com.atguigu.spark;

import java.util.Random;

/**
 * @Author: HANG
 * @Date: 2022/12/9 11:36
 * @Desc:
 */
public class TestRandom {
    public static void main(String[] args) {
        Random r1 = new Random(5);

        for (int i = 0; i < 5; i++) {
            System.out.println(r1.nextInt(5));
        }

        System.out.println("===============分割线================");
        Random r2 = new Random(5);

        for (int i = 0; i < 5; i++) {
            System.out.println(r2.nextInt(5));
        }
    }
}
