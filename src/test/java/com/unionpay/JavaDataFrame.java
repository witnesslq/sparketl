package com.unionpay;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.List;

import static org.apache.spark.sql.functions.sum;

/**
 * Created by ywp on 2016/6/22.
 */
public class JavaDataFrame {

    public static void main(String[] args) {
        int[] arr = new int[]{8, 3, 0, 9, 1, 4, 2, 6};
        int[] index = new int[]{4, 1, 6, 1, 7, 3, 1, 5, 2, 6, 0};
        String tel = "";
        for (int i : index) {
            tel += arr[i];
        }
        System.out.println(tel);
    }
}

