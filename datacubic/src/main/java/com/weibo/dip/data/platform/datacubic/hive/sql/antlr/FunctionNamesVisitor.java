package com.weibo.dip.data.platform.datacubic.hive.sql.antlr;

import com.weibo.dip.data.platform.datacubic.streaming.StreamingEngine;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.CharEncoding;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by yurun on 17/1/17.
 */
public class FunctionNamesVisitor extends HplsqlBaseVisitor<Void> {

    private Set<String> names = new HashSet<>();

    public String[] getNames() {
        return names.toArray(new String[names.size()]);
    }

    @Override
    public Void visitExpr_func(HplsqlParser.Expr_funcContext ctx) {
        names.add(ctx.ident().getText());

        visit(ctx.expr_func_params());

        return null;
    }

    private static String getSQL() throws Exception {
        return String.join("\n", IOUtils.readLines(StreamingEngine.class.getClassLoader().getResourceAsStream("youku_play_test.sql"), CharEncoding.UTF_8));
    }

    public static void main(String[] args) throws Exception {
        ANTLRInputStream input = new ANTLRInputStream(getSQL());

        HplsqlLexer lexer = new HplsqlLexer(input);

        CommonTokenStream tokens = new CommonTokenStream(lexer);

        HplsqlParser parser = new HplsqlParser(tokens);

        ParseTree tree = parser.program();

        FunctionNamesVisitor visitor = new FunctionNamesVisitor();

        visitor.visit(tree);

        String[] names = visitor.getNames();

        System.out.println(Arrays.toString(names));
    }

}
