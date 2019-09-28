package com.weibo.dip.ml.godeyes;

import com.weibo.dip.data.platform.commons.util.HDFSUtil;
import ml.dmlc.xgboost4j.java.Booster;
import ml.dmlc.xgboost4j.java.DMatrix;
import ml.dmlc.xgboost4j.java.XGBoost;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Created by yurun on 17/7/12.
 */
public class Predictor {

    private static final Logger LOGGER = LoggerFactory.getLogger(Predictor.class);

    private String modelPath;

    private Booster booster;

    private long lastBoosted = System.currentTimeMillis();

    private long boostInterval = 3600000;

    public Predictor(String modelPath) {
        this.modelPath = modelPath;
    }

    private Booster loadOrUpdateBooster() throws Exception {
        List<Path> modelFiles = HDFSUtil.listFiles(modelPath, false);
        if (CollectionUtils.isEmpty(modelFiles)) {
            LOGGER.warn("load/update booster, but model path[{}] dosen't have any model files, skip",
                modelPath);

            return null;
        }

        modelFiles.sort(Comparator.comparing(Path::getName));

        Path modelFilePath = modelFiles.get(modelFiles.size() - 1);

        LOGGER.info("load/update model from path: " + modelFilePath);

        return XGBoost.loadModel(HDFSUtil.openInputStream(modelFilePath));
    }

    public Float predict(DMatrix matrix) throws Exception {
        long now = System.currentTimeMillis();

        if (Objects.isNull(booster) || now - lastBoosted > boostInterval) {
            /*
                Load/Update Booster
             */
            booster = loadOrUpdateBooster();
            if (Objects.isNull(booster)) {
                LOGGER.warn("booster is null!");

                return null;
            }

            lastBoosted = now;
        }

        float[][] predicts = booster.predict(matrix);
        if (ArrayUtils.isNotEmpty(predicts)) {
            float[] values = predicts[0];
            if (ArrayUtils.isNotEmpty(values)) {
                return values[0];
            }
        }

        return null;
    }

}
