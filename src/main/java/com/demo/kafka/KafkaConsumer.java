package com.demo.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.demo.state.StateTemporaryStore;
import com.demo.util.ExpressUtil;
import com.demo.util.Field3801FillUtil;
import com.demo.util.MysqlConnUtil;
import com.demo.util.ParamConfigUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zuihaodeziji on 2020/3/16.
 * com.demo.kafka.KafkaConsumer
 */
public class KafkaConsumer {

    private static Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    private static Map<String, StateTemporaryStore> temporaryStoreMap = new ConcurrentHashMap<>();

    private static void consumer() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //使用manage state,flink自动实现state保存和恢复
        env.setStateBackend(new MemoryStateBackend());
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        //flink任务取消时，保留外部保存的CheckPoint 信息
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION.RETAIN_ON_CANCELLATION);

        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka01.bitnei.cn:9092,kafka02.bitnei.cn:9092");
        props.put("zookeeper.connect", "zk01.bitnei.cn:2181,zk02.bitnei.cn:2181");
        props.put("group.id", "ldx_flink_test_topic_group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  //key 反序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<String>(
                "ldx_flink_test_topic",
                new SimpleStringSchema(),
                props
        )).setParallelism(3);


        SingleOutputStreamOperator<Tuple2<String, Map<String, String>>> mapOutputStream = dataStreamSource.map(new MapFunction<String, Tuple2<String, Map<String, String>>>() {
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
        }).setParallelism(3).filter(new FilterFunction<Tuple2<String, Map<String, String>>>() {
            public boolean filter(Tuple2<String, Map<String, String>> value) throws Exception {
                return StringUtils.isNotBlank(value.f0) && value.f1 != null;
            }
        }).setParallelism(3);

        //TODO 处理多项报警信息

        //TODO 1.3801字段报警
        SingleOutputStreamOperator alarm3801 = mapOutputStream.keyBy(0).process(new Alarm3801Status()).uid("state_uid_3801").setParallelism(3);

        //TODO 2.GPS异常报警
        /*SingleOutputStreamOperator alarmGps = mapOutputStream.keyBy(0).flatMap(new RichFlatMapFunction<Tuple2<String, Map<String, String>>, String>() {

            private ValueState<AlarmInfo> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<AlarmInfo>("valueStateGps", AlarmInfo.class));
            }

            public void flatMap(Tuple2<String, Map<String, String>> value, Collector<String> out) throws Exception {
                String str2502 = value.f1.get("2502");
                String str2503 = value.f1.get("2503");
                //TODO 抽象出来
                boolean isAlarm = false;
                if (StringUtils.isBlank(str2502) || StringUtils.isBlank(str2503)) {
                    isAlarm = true;
                }
                if (isAlarm) {
                    AlarmInfo alarmInfo = valueState.value();
                    if (alarmInfo == null) {
                        alarmInfo = new AlarmInfo(0, "end");
                    }
                    alarmInfo.setCount(alarmInfo.getCount() + 1);
                    if (!"begin".equals(alarmInfo.getAlarmStatus()) && alarmInfo.getCount() >= 3) {
                        alarmInfo.setAlarmStatus("begin");
                        out.collect("the vid:" + value.f0 + ",the alarm msg begin,the alarm type is gps");
                    }

                    valueState.update(alarmInfo);


                }

            }
        }).uid("state_uid_gsp_exception");*/
        alarm3801.print();
        //alarmGps.print();

        env.execute();
    }


    private static class Alarm3801Status extends KeyedProcessFunction<Tuple, Tuple2<String, Map<String, String>>, String> {

        private MapState<String, AlarmInfo> mapState;


        @Override
        public void open(Configuration parameters) throws Exception {

            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, AlarmInfo>("mapStateParam", String.class, AlarmInfo.class));

        }

        public void processElement(Tuple2<String, Map<String, String>> dataMapTuple, Context context, Collector<String> collector) throws Exception {
            String vid = dataMapTuple.f0;
            Map<String, Object> dataMap = fill3801Field(dataMapTuple.f1);

            if (dataMap != null) {
                Map<String, MysqlConnUtil.ExpressObj> expressionMap = ParamConfigUtil.getParamConf(ParamConfigUtil.paramSql);

                for(Map.Entry<String,MysqlConnUtil.ExpressObj> entry : expressionMap.entrySet()){
                    String mapKey = vid+"_"+entry.getKey();
                    boolean isAlarm = ExpressUtil.isMeetExpression(entry.getValue().getExpression(), dataMap);
                    long currentTs = System.currentTimeMillis();
                    if(isAlarm){
                        if(!mapState.contains(mapKey)){
                          handleAlarm(mapKey,currentTs,entry.getValue().getName(),"begin",temporaryStoreMap,mapState,collector);
                        }else{
                            temporaryStoreMapUpdate(mapKey,currentTs,"begin");
                        }
                    }else{
                       if(mapState.contains(mapKey)){
                           handleAlarm(mapKey,currentTs,entry.getValue().getName(),"end",temporaryStoreMap,mapState,collector);
                       }else{
                           temporaryStoreMapUpdate(mapKey,currentTs,"end");
                       }
                    }

                }


            }



        }

        private Map<String, Object> fill3801Field(Map<String, String> dataMap) {

            if (dataMap == null || !dataMap.containsKey("3801")) {
                return null;
            }
            Field3801FillUtil.fillAlarm(dataMap);
            Map<String, Object> finalMap = new HashMap<>();
            dataMap.forEach((k, v) -> {
                if (NumberUtils.isNumber(v)) {
                    finalMap.put("d" + k, new Double(v));
                }
            });

            return finalMap;
        }

        private void handleAlarm(String mapKey,long currentTs,String alarmName,String alarmStatus ,Map<String, StateTemporaryStore> temporaryStoreMap,MapState<String, AlarmInfo> mapState,Collector<String> collector) throws Exception{
            StateTemporaryStore sts = temporaryStoreMap.get(mapKey);
            if(sts != null && alarmStatus.equals(sts.getAlarmState())){
                sts.setCurrentTs(currentTs);
                sts.setCount(sts.getCount()+1);
                if(isAlarmStartEndThreshold(sts,3000,3)){
                    alarmStartEndOpe(mapKey,alarmName,alarmStatus,temporaryStoreMap,mapState,collector);
                }
            }else{
                boolean isOriStsIsNull = false;
                if(sts == null){
                    isOriStsIsNull = true;
                }
                sts = initStateTemporaryStore(sts,currentTs,1,alarmStatus);
                if(isOriStsIsNull){
                    temporaryStoreMap.put(mapKey,sts);
                }
                if(isAlarmStartEndThreshold(sts,3000,3)){
                    alarmStartEndOpe(mapKey,alarmName,alarmStatus,temporaryStoreMap,mapState,collector);
                }
            }
        }

        private void temporaryStoreMapUpdate(String mapKey,long currentTs,String alarmStatus){
            StateTemporaryStore sts =  temporaryStoreMap.get(mapKey);
            if(sts != null && !alarmStatus.equals(sts.getAlarmState())){
                initStateTemporaryStore(sts,currentTs,1,alarmStatus);
            }
        }

        private void alarmStartEndOpe(String mapKey,String alarmName,String alarmStatus ,Map<String, StateTemporaryStore> temporaryStoreMap,MapState<String, AlarmInfo> mapState,Collector<String> collector) throws Exception{
            AlarmInfo alarmInfo =  buildAlarmInfo(mapKey,alarmName,alarmStatus);
            if("begin".equals(alarmStatus)){
                mapState.put(mapKey,alarmInfo);
            }else if("end".equals(alarmStatus)){
                mapState.remove(mapKey);
            }
            collector.collect(alarmInfo.getMsg());
            //临时存储map去掉对应的key
            temporaryStoreMap.remove(mapKey);
        }

        private boolean isAlarmStartEndThreshold(StateTemporaryStore sts,long continueTime,int continueCount){
            if(sts.getCurrentTs()-sts.getFirstTs() >= continueTime && sts.getCount() >= continueCount){
                return true;
            }
            return false;
        }
        private StateTemporaryStore  initStateTemporaryStore(StateTemporaryStore sts,long firstTs,int count,String alarmStatus){
            if(sts == null){
                sts = new StateTemporaryStore();
            }
            sts.setFirstTs(firstTs);
            sts.setCurrentTs(firstTs);
            sts.setCount(count);
            sts.setAlarmState(alarmStatus);
            return sts;
        }

        private AlarmInfo buildAlarmInfo(String alarmMsg,String alarmName,String alarmStatus){
            AlarmInfo alarmInfo = new AlarmInfo();
            alarmInfo.setMsg(alarmMsg+",and the alarm name is:"+alarmName+",and the alarm status is :"+alarmStatus);
            return alarmInfo;
        }

    }

    public static class AlarmInfo {
        private Integer count;
        private String alarmStatus;
        private String msg;

        public AlarmInfo(Integer count, String alarmStatus) {
            this.count = count;
            this.alarmStatus = alarmStatus;
        }

        public AlarmInfo(){

        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }

        public String getAlarmStatus() {
            return alarmStatus;
        }

        public void setAlarmStatus(String alarmStatus) {
            this.alarmStatus = alarmStatus;
        }

        public String getMsg() {
            return msg;
        }

        public void setMsg(String msg) {
            this.msg = msg;
        }
    }

    public static void main(String[] args) throws Exception {
        consumer();
    }


}
