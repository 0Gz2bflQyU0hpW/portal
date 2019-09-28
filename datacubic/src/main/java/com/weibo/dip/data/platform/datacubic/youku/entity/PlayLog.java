package com.weibo.dip.data.platform.datacubic.youku.entity;

import org.apache.commons.lang.StringUtils;

/**
 * @author delia
 */

public class PlayLog {

    public static PlayLog build(String line) {
        if (StringUtils.isEmpty(line)) {
            return null;
        }

        line = line.substring(line.indexOf("|") + 1);

        String[] values = line.split("`");
        if (values.length != 18) {
            return null;
        }
        PlayLog playLog = new PlayLog();

        playLog.setLogtime(values[0]);
        playLog.setUid(values[1]);
        playLog.setActcode(values[2]);
        playLog.setOid(values[3]);
        playLog.setUicode(values[4]);
        playLog.setFid(values[5]);
        playLog.setLfid(values[6]);
        playLog.setLuicode(values[7]);
        playLog.setCardid(values[8]);
        playLog.setLcardid(values[9]);
        playLog.setFeaturecode(values[10]);
        playLog.setFromcode(values[11]);
        playLog.setWm(values[12]);
        playLog.setOldwm(values[13]);
        playLog.setClientip(values[14]);
        playLog.setLogversion(values[15]);
        playLog.setAid(values[16]);
        playLog.setExtstr(values[17]);

        return playLog;
    }

    private String logtime;
    private String uid;
    private String actcode;
    private String oid;
    private String uicode;
    private String fid;
    private String lfid;
    private String luicode;
    private String cardid;
    private String lcardid;
    private String featurecode;
    private String fromcode;
    private String wm;
    private String oldwm;
    private String clientip;
    private String logversion;
    private String aid;
    private String extstr;

    public PlayLog() {

    }

    public String getLogtime() {
        return logtime;
    }

    public void setLogtime(String logtime) {
        this.logtime = logtime;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getActcode() {
        return actcode;
    }

    public void setActcode(String actcode) {
        this.actcode = actcode;
    }

    public String getOid() {
        return oid;
    }

    public void setOid(String oid) {
        this.oid = oid;
    }

    public String getUicode() {
        return uicode;
    }

    public void setUicode(String uicode) {
        this.uicode = uicode;
    }

    public String getFid() {
        return fid;
    }

    public void setFid(String fid) {
        this.fid = fid;
    }

    public String getLfid() {
        return lfid;
    }

    public void setLfid(String lfid) {
        this.lfid = lfid;
    }

    public String getLuicode() {
        return luicode;
    }

    public void setLuicode(String luicode) {
        this.luicode = luicode;
    }

    public String getCardid() {
        return cardid;
    }

    public void setCardid(String cardid) {
        this.cardid = cardid;
    }

    public String getLcardid() {
        return lcardid;
    }

    public void setLcardid(String lcardid) {
        this.lcardid = lcardid;
    }

    public String getFeaturecode() {
        return featurecode;
    }

    public void setFeaturecode(String featurecode) {
        this.featurecode = featurecode;
    }

    public String getFromcode() {
        return fromcode;
    }

    public void setFromcode(String fromcode) {
        this.fromcode = fromcode;
    }

    public String getWm() {
        return wm;
    }

    public void setWm(String wm) {
        this.wm = wm;
    }

    public String getOldwm() {
        return oldwm;
    }

    public void setOldwm(String oldwm) {
        this.oldwm = oldwm;
    }

    public String getClientip() {
        return clientip;
    }

    public void setClientip(String clientip) {
        this.clientip = clientip;
    }

    public String getLogversion() {
        return logversion;
    }

    public void setLogversion(String logversion) {
        this.logversion = logversion;
    }

    public String getAid() {
        return aid;
    }

    public void setAid(String aid) {
        this.aid = aid;
    }

    public String getExtstr() {
        return extstr;
    }

    public void setExtstr(String extstr) {
        this.extstr = extstr;
    }

}
