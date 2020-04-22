package com.demo.broadcast;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.demo.util.MysqlConnUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zuihaodeziji on 2020/4/21.
 */
public class BroadcastAlarmRuleFromMysql {

    private static Logger logger = LoggerFactory.getLogger(BroadcastAlarmRuleFromMysql.class);

    private final static MapStateDescriptor<String,MysqlConnUtil.ExpressObjMini> alarmRuleMSD =
            new MapStateDescriptor<>(
            "alarm_rule",
            BasicTypeInfo.STRING_TYPE_INFO,
            TypeInformation.of(MysqlConnUtil.ExpressObjMini.class));


    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        logger.info("");

        DataStreamSource<Map<String,MysqlConnUtil.ExpressObjMini>> alarmRuleDs = env.addSource(new GetAlarmRuleSourceFunction()).setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Map<String, String>>> dataDs = env.socketTextStream("localhost",9999).map(new MapFunction<String, Tuple2<String, Map<String, String>>>() {
            public Tuple2<String, Map<String, String>> map(String value) throws Exception {
                try {
                    if (value != null) {
                        JSONObject jsonObject = JSON.parseObject(value);
                        String vid = jsonObject.getString("vid");
                        Map<String, String> dataMap = JSONObject.toJavaObject(jsonObject, Map.class);

                        return new Tuple2<String, Map<String, String>>(vid, dataMap);
                    } else {
                        return new Tuple2<String, Map<String, String>>(null, null);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("the parse the value exception.the exception is:{}", e.getMessage());
                    return new Tuple2<String, Map<String, String>>(null, null);
                }
            }
        }).filter(new FilterFunction<Tuple2<String, Map<String, String>>>() {
            public boolean filter(Tuple2<String, Map<String, String>> value) throws Exception {
                return StringUtils.isNotBlank(value.f0) && value.f1 != null;
            }
        });

        SingleOutputStreamOperator<String> finalDs = dataDs.keyBy(0).connect(alarmRuleDs.broadcast(alarmRuleMSD)).process(new KeyedBroadcastProcessFunction<String,Tuple2<String, Map<String, String>>,Map<String,MysqlConnUtil.ExpressObjMini>,String>(){


            @Override
            public void processElement(Tuple2<String, Map<String, String>> value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                ReadOnlyBroadcastState<String,MysqlConnUtil.ExpressObjMini> broadcastState = ctx.getBroadcastState(alarmRuleMSD);

                for(Map.Entry<String,MysqlConnUtil.ExpressObjMini> entry : broadcastState.immutableEntries()){

                    boolean isAlarm = false;
                    if(value.f1.containsKey("3801") && new Double(value.f1.get("3801")) > 0){
                        isAlarm = true;
                    }
                    if(isAlarm){
                        out.collect("the vid:"+value.f0+",the alarm rule is:"+"  has alarmed");
                    }
                }
            }

            /**
             * This method is called for each element in the broadcast stream
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processBroadcastElement(Map<String,MysqlConnUtil.ExpressObjMini> value, Context ctx, Collector<String> out) throws Exception {
                  if(value == null){
                      return;
                  }
                  BroadcastState<String,MysqlConnUtil.ExpressObjMini> broadcastState = ctx.getBroadcastState(alarmRuleMSD);
                  for(Map.Entry<String,MysqlConnUtil.ExpressObjMini> entry : value.entrySet()){
                      broadcastState.put(entry.getKey(),entry.getValue());
                  }

            }
        });

        finalDs.print();

        env.execute("broadcast test");


    }

}
