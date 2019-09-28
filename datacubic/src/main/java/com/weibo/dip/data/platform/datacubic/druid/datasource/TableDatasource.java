package com.weibo.dip.data.platform.datacubic.druid.datasource;

/**
 * Created by yurun on 17/2/14.
 */
public class TableDatasource extends DataSource {

    private static final String TABLE = "table";

    private String name;

    public TableDatasource() {
        super(TABLE);
    }

    public TableDatasource(String name) {
        super(TABLE);

        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public static void main(String[] args) {
        TableDatasource datasource = new TableDatasource();

        datasource.setName("test");

        System.out.println(datasource);
    }

}
