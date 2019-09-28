package com.weibo.dip.ml.commons;

/**
 * Created by yurun on 17/7/11.
 */
public class TerminationSignal {

    private boolean terminated = false;

    public TerminationSignal() {
    }

    public boolean isTerminated() {
        return terminated;
    }

    public void terminate() {
        terminated = true;
    }

}
