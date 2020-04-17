package com.demo.watermark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Created by zuihaodeziji on 2020/4/17.
 */
public class WaterMakerDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        /**
         * 如果不设置parallelism=1,接受到的数据可能是来自多个分区，即有多个并行的subtask. 每个subtask都会有waterMaker,这些watermater
         * 会取最小的watermaker作为"最终的"watermaker,并传到下游
         */
        env.setParallelism(1);
        SingleOutputStreamOperator<Word> data = env.socketTextStream("localhost",9999)
                .map(new MapFunction<String, Word>() {
                    @Override
                    public Word map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new Word(split[0],new Long(split[1])*1000,new Integer(split[2]));
                    }
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Word>(Time.seconds(10)) {
                    @Override
                    public long extractTimestamp(Word element) {
                        return element.getEventTime();
                    }
                });


        /**
         * 获取第一个窗口
         * timestamp - (timestamp - offset + windowSize) % windowSize;
         */
        SingleOutputStreamOperator<Word> winData = data
                .keyBy("name")
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<Word>() {
                    @Override
                    public Word reduce(Word value1, Word value2) throws Exception {
                         return new Word(value1.getName(),value2.getEventTime(),value1.getCount()+value2.getCount());
                    }
                });
        winData.print("agg count >>");
        data.print("input >>");


        env.execute("watermark demo");
    }
}
