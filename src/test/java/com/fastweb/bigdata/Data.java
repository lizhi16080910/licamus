package com.fastweb.bigdata;

/**
 * Created by lfq on 2017/4/11.
 */
public class Data {
    private String a = "1";
    private String b = "2";
    private String c = "3";

    public String getA() {
        return a;
    }

    public String getB() {
        return b;
    }

    public String getC() {
        return c;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Data data = (Data) o;

        if (!a.equals(data.a)) return false;
        if (!b.equals(data.b)) return false;
        return c.equals(data.c);

    }

    @Override
    public int hashCode() {
        int result = a.hashCode();
        result = 31 * result + b.hashCode();
        result = 31 * result + c.hashCode();
        return result;
    }
}
