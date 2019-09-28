package com.weibo.dip.data.platform.datacubic.druid.datasource;

/**
 * Created by yurun on 17/2/14.
 */
public class UnionDatasource extends DataSource {

    private static final String UNION = "union";

    private String[] dataSources;

    public UnionDatasource() {
        super(UNION);
    }

    public UnionDatasource(String[] dataSources) {
        super(UNION);

        this.dataSources = dataSources;
    }

    public String[] getDataSources() {
        return dataSources;
    }

    public void setDataSources(String[] dataSources) {
        this.dataSources = dataSources;
    }

}
