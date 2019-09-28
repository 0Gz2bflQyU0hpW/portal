package com.weibo.dip.data.platform.datacubic.videotrace.ha;

import com.weibo.dip.data.platform.datacubic.hive.sql.util.HiveSQLUtil;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yurun on 17/1/20.
 */
public class SQLMain {

    public static void main(String[] args) throws Exception {
        List<String> lines = new ArrayList<>();

        BufferedReader reader = new BufferedReader(new InputStreamReader(SQLMain.class.getClassLoader().getResourceAsStream("sql"), CharEncoding.UTF_8));

        String line;

        while ((line = reader.readLine()) != null) {
            lines.add(line);
        }

        String sql = StringUtils.join(lines, "\n");

        String[] names = HiveSQLUtil.getNoBuiltInFunctionNames(sql);

        System.out.println(ArrayUtils.toString(names));
    }

}
