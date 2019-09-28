package com.weibo.dip.data.platform.commons.util;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ShellUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShellUtil.class);

    private static class StreamGobbler implements Callable<String> {

        private InputStream in;

        public StreamGobbler(InputStream in) {
            this.in = in;
        }

        @Override
        public String call() throws Exception {
            BufferedReader reader = null;

            try {
                reader = new BufferedReader(new InputStreamReader(in, CharEncoding.UTF_8));

                StringBuilder buffer = new StringBuilder("");

                String line;

                while ((line = reader.readLine()) != null) {
                    buffer.append(line);
                    buffer.append("\n");
                }

                return buffer.toString();
            } finally {
                if (reader != null) {
                    reader.close();
                }
            }
        }

    }

    public static class Result {

        private boolean success = false;
        private String output = "";
        private String error = "";

        public Result() {
        }

        public Result(boolean success, String output, String error) {
            this.success = success;
            this.output = output;
            this.error = error;
        }

        public boolean isSuccess() {
            return success;
        }

        public void setSuccess(boolean success) {
            this.success = success;
        }

        public String getOutput() {
            return output;
        }

        public void setOutput(String output) {
            this.output = output;
        }

        public String getError() {
            return error;
        }

        public void setError(String error) {
            this.error = error;
        }

        @Override
        public String toString() {
            return String.format("success: %s, output: %s, error: %s", success, output, error);
        }

    }

    public static Result execute(String command) throws Exception {
        LOGGER.info("command: " + command);

        Preconditions.checkState(StringUtils.isNotEmpty(command), "command can't be empty");

        Result record = new Result();

        ExecutorService executor = null;

        Process process = null;

        try {
            executor = Executors.newFixedThreadPool(2);

            process = Runtime.getRuntime().exec(command);

            Future<String> error = executor.submit(new StreamGobbler(process.getErrorStream()));
            Future<String> output = executor.submit(new StreamGobbler(process.getInputStream()));

            record.setError(error.get());
            record.setOutput(output.get());

            if (process.waitFor() == 0) {
                record.setSuccess(true);
            }
        } finally {
            if (Objects.nonNull(process)) {
                process.destroy();
            }

            if (Objects.nonNull(executor)) {
                executor.shutdownNow();
            }
        }

        return record;
    }

    public static void main(String[] args) throws Exception {
        String command = "ls -l /";

        Result result = ShellUtil.execute(command);

        System.out.println("isSuccess: " + result.isSuccess());
        System.out.println("output: " + result.getOutput());
        System.out.println("error: " + result.getError());
    }

}
