package com.demo.cep;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;


/**
 * Created by lidaxue on 2020/4/28.
 */
public class CepDemo {
    public static void main(String[] args) throws  Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Event> dataStream = env
                .socketTextStream("localhost",9999)
                .flatMap(new FlatMapFunction<String, Event>() {
                    @Override
                    public void flatMap(String s, Collector<Event> collector) throws Exception {
                        if(StringUtils.isNotBlank(s)){
                            String[] arr = s.split(",");
                            if(arr.length == 2){
                                collector.collect(new Event(arr[0],arr[1]));
                            }
                        }
                    }
                });

        Pattern<Event,?> pattern = Pattern.<Event>begin("trigger")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                      return   "ldx".equals(event.getName());
                    }
                }).times(1,5);

        SingleOutputStreamOperator<String> cepStream = CEP.pattern(dataStream,pattern).select(new PatternSelectFunction<Event, String>() {
            @Override
            public String select(Map<String, List<Event>> map) throws Exception {
                List<Event> list = map.get("trigger");
                if(list != null && list.size() > 0){
                    StringBuilder stringBuilder = new StringBuilder();
                    list.forEach( e->{
                        stringBuilder.append(e.toString());
                    });
                    return stringBuilder.toString();
                }else{
                    return "null";
                }
            }
        });

        cepStream.print();

        env.execute("cep test");
    }
}
