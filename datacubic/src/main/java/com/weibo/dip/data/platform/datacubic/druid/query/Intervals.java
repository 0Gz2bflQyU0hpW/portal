package com.weibo.dip.data.platform.datacubic.druid.query;

import com.google.gson.TypeAdapter;
import com.google.gson.annotations.JsonAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.weibo.dip.data.platform.commons.util.GsonUtil;
import org.apache.commons.collections.CollectionUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by yurun on 17/1/23.
 */
@JsonAdapter(Intervals.IntervalsTypeAdapter.class)
public class Intervals {

    public static class IntervalsTypeAdapter extends TypeAdapter<Intervals> {

        private static final String UTC_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssXXX";

        private static final String UTC = "UTC";

        private static final String SLASH = "/";

        private SimpleDateFormat utcFormat = new SimpleDateFormat(UTC_DATE_FORMAT);

        {
            utcFormat.setTimeZone(TimeZone.getTimeZone(UTC));
        }

        @Override
        public void write(JsonWriter out, Intervals value) throws IOException {
            out.beginArray();

            List<Interval> intervals = value.getIntervals();

            if (CollectionUtils.isNotEmpty(intervals)) {
                for (Interval interval : intervals) {
                    out.value(utcFormat.format(interval.getStartTime()) + SLASH + utcFormat.format(interval.getEndTime()));
                }
            }

            out.endArray();
        }

        @Override
        public Intervals read(JsonReader in) throws IOException {
            Intervals intervals = new Intervals();

            in.beginArray();

            while (in.hasNext()) {
                String[] pairs = in.nextString().split(SLASH);

                try {
                    intervals.add(utcFormat.parse(pairs[1]), utcFormat.parse(pairs[0]));
                } catch (ParseException e) {
                    throw new IOException(e);
                }
            }

            in.endArray();

            return intervals;
        }

    }

    private List<Interval> intervals;

    public List<Interval> getIntervals() {
        return intervals;
    }

    public void setIntervals(List<Interval> intervals) {
        this.intervals = intervals;
    }

    public void add(Interval interval) {
        if (Objects.isNull(intervals)) {
            intervals = new ArrayList<>();
        }

        intervals.add(interval);
    }

    public void add(Date startTime, Date endTime) {
        add(new Interval(startTime, endTime));
    }

    @Override
    public String toString() {
        return GsonUtil.toJson(this);
    }

}
