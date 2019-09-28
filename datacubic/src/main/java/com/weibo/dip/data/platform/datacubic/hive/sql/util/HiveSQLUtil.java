package com.weibo.dip.data.platform.datacubic.hive.sql.util;

import com.weibo.dip.data.platform.datacubic.hive.sql.antlr.FunctionNamesVisitor;
import com.weibo.dip.data.platform.datacubic.hive.sql.antlr.HplsqlLexer;
import com.weibo.dip.data.platform.datacubic.hive.sql.antlr.HplsqlParser;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yurun on 17/1/17.
 */
public class HiveSQLUtil {

    public static final String[] BUILT_IN_FUNCTIONS = {"!", "!=", "%", "&", "*", "+", "-", "/", "<", "<=", "<=>", "<>",
        "=", "==", ">", ">=", "^", "abs", "acos", "add_months", "and", "array", "array_contains", "ascii", "asin",
        "assert_true", "atan", "avg", "base64", "between", "bin", "case", "cbrt", "ceil", "ceiling", "coalesce",
        "collect_list", "collect_set", "compute_stats", "concat", "concat_ws", "context_ngrams", "conv", "corr", "cos",
        "count", "covar_pop", "covar_samp", "create_union", "cume_dist", "current_database", "current_date",
        "current_timestamp", "current_user", "date_add", "date_format", "date_sub", "datediff", "day", "dayofmonth",
        "decode", "degrees", "dense_rank", "div", "e", "elt", "encode", "ewah_bitmap", "ewah_bitmap_and",
        "ewah_bitmap_empty", "ewah_bitmap_or", "exp", "explode", "factorial", "field", "find_in_set", "first_value",
        "floor", "format_number", "from_unixtime", "from_utc_timestamp", "get_json_object", "greatest", "hash", "hex",
        "histogram_numeric", "hour", "if", "in", "in_file", "index", "initcap", "inline", "instr", "isnotnull", "isnull",
        "java_method", "json_tuple", "lag", "last_day", "last_value", "lcase", "lead", "least", "length", "levenshtein",
        "like", "ln", "locate", "log", "log10", "log2", "lower", "lpad", "ltrim", "map", "map_keys", "map_values",
        "matchpath", "max", "min", "minute", "month", "months_between", "named_struct", "negative", "next_day", "ngrams",
        "noop", "noopstreaming", "noopwithmap", "noopwithmapstreaming", "not", "ntile", "nvl", "or", "parse_url",
        "parse_url_tuple", "percent_rank", "percentile", "percentile_approx", "pi", "pmod", "posexplode", "positive",
        "pow", "power", "printf", "radians", "rand", "rank", "reflect", "reflect2", "regexp", "regexp_extract",
        "regexp_replace", "repeat", "reverse", "rlike", "round", "row_number", "rpad", "rtrim", "second", "sentences",
        "shiftleft", "shiftright", "shiftrightunsigned", "sign", "sin", "size", "sort_array", "soundex", "space", "split",
        "sqrt", "stack", "std", "stddev", "stddev_pop", "stddev_samp", "str_to_map", "struct", "substr", "substring",
        "sum", "tan", "to_date", "to_unix_timestamp", "to_utc_timestamp", "translate", "trim", "trunc", "ucase", "unbase64",
        "unhex", "unix_timestamp", "upper", "var_pop", "var_samp", "variance", "weekofyear", "when", "windowingtablefunction",
        "xpath", "xpath_boolean", "xpath_double", "xpath_float", "xpath_int", "xpath_long", "xpath_number", "xpath_short",
        "xpath_string", "year", "|", "~"};

    public static String[] getBuiltInFunctions() {
        return BUILT_IN_FUNCTIONS;
    }

    public static String[] getFunctionNames(String sql) {
        if (StringUtils.isEmpty(sql)) {
            return null;
        }

        ANTLRInputStream input = new ANTLRInputStream(sql);

        HplsqlLexer lexer = new HplsqlLexer(input);

        CommonTokenStream tokens = new CommonTokenStream(lexer);

        HplsqlParser parser = new HplsqlParser(tokens);

        ParseTree tree = parser.program();

        FunctionNamesVisitor visitor = new FunctionNamesVisitor();

        visitor.visit(tree);

        return visitor.getNames();
    }

    public static String[] getNoBuiltInFunctionNames(String sql) {
        String[] names = getFunctionNames(sql);

        if (ArrayUtils.isEmpty(names)) {
            return null;
        }

        List<String> noBuiltInFunctionNames = new ArrayList<>();

        for (String name : names) {
            if (!ArrayUtils.contains(BUILT_IN_FUNCTIONS, name)) {
                noBuiltInFunctionNames.add(name);
            }
        }

        return noBuiltInFunctionNames.toArray(new String[noBuiltInFunctionNames.size()]);
    }

}
