package com.demo.util;


import java.util.Map;

/**
 * Created by zuihaodeziji on 2020/4/8.
 */
public class ParamConfigUtil {

    private final static long LOAD_INTERVAL_TS = 5*60*1000L;
    private static long lastLoadTs = 0L;
    private static Map<String,MysqlConnUtil.ExpressObj> paramConfMap;
    public static final String paramSql = "select r.id as ruleIdentity,r.name as name,r.veh_model_id as modelIdentities,r.formula as formula,r.begin_threshold as beginTimeThresholdSecond,r.end_threshold as endTimeThresholdSecond from fault_parameter_rule as r where r.enabled_status=1 and r.delete_status=0 AND r.preset_rule=1";

    public static Map<String,MysqlConnUtil.ExpressObj> getParamConf(String sql) throws Exception{
        long currentTs = System.currentTimeMillis();
        if(currentTs - lastLoadTs >= LOAD_INTERVAL_TS){
            return getParamConfFromDB(currentTs,sql);
        }
        return paramConfMap;

    }

    private static synchronized Map<String,MysqlConnUtil.ExpressObj> getParamConfFromDB(long currentTs,String sql) throws Exception{
         if(currentTs-lastLoadTs >= LOAD_INTERVAL_TS){
             paramConfMap = null;

             paramConfMap = MysqlConnUtil.getParseParamConf(sql);

             lastLoadTs = currentTs;
         }

         return paramConfMap;

    }
}
