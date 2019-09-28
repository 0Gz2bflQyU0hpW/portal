package com.weibo.dip.data.platform.datacubic.youku.test;

/**
 * Created by delia on 2017/2/14.
 */



public class ClassTest {
    static class Gen<T> {
        private T t;

        public Gen(T t) {
            this.t = t;
        }

        public void showGenType() {
            System.out.println("T的实际类型是：" + t.getClass().getSimpleName());
        }
    }

    public static void main(String[] args) {
        Gen<Integer> intObj = new Gen<Integer>(0);
        intObj.showGenType();

        Gen<String> strObj = new Gen<String>("fefe");
        strObj.showGenType();

    }
}
