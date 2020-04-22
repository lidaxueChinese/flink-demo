package com.demo.broadcast;

import com.demo.util.MysqlConnUtil;
import com.demo.util.ParamConfigUtil;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by zuihaodeziji on 2020/4/21.
 */
public class GetAlarmRuleSourceFunction extends RichSourceFunction<Map<String,MysqlConnUtil.ExpressObjMini>> {

    private static Logger logger = LoggerFactory.getLogger(GetAlarmRuleSourceFunction.class);

    @Override
    public void run(SourceContext<Map<String,MysqlConnUtil.ExpressObjMini>> ctx) throws Exception {
        while(true) {
            Map<String, MysqlConnUtil.ExpressObjMini> ruleMap = MysqlConnUtil.getParseParamConfMini(ParamConfigUtil.paramSql);
            logger.info("get the alarm rule from mysql....");
            ctx.collect(ruleMap);
            //暂停程序
            Thread.sleep(5000L);
        }
    }

    @Override
    public void cancel() {

    }
}
