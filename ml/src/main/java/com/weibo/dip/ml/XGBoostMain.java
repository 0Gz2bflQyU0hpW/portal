package com.weibo.dip.ml;

import ml.dmlc.xgboost4j.java.Booster;
import ml.dmlc.xgboost4j.java.DMatrix;
import ml.dmlc.xgboost4j.java.XGBoost;
import org.apache.commons.lang.ArrayUtils;

import java.util.Arrays;

/**
 * Created by yurun on 17/6/27.
 */
public class XGBoostMain {

    public static void main(String[] args) throws Exception {
        Booster booster = XGBoost.loadModel(args[0]);

        DMatrix testMat1 = new DMatrix(args[1]);


        float[] data = new float[]{1f,1f,1f,1f,1f,1f,1f,1f,1f,1f,1f,1f,1f,1f,1f,1f,1f,1f,1f,1f,1f,1f};


        DMatrix testMat2 = new DMatrix(data, 1, 22, 0);

        float[][] predicts1 = booster.predict(testMat1);

        float[][] predicts2 = booster.predict(testMat2);

        if (ArrayUtils.isNotEmpty(predicts1)) {
            for (float[] predict : predicts1) {
                System.out.println(Arrays.toString(predict));
            }
        }
        if (ArrayUtils.isNotEmpty(predicts2)) {
            for (float[] predict : predicts2) {
                System.out.println(Arrays.toString(predict));
            }
        }

        System.out.println("hello xgboost");
    }

}
