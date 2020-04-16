package com.demo.util;

import com.googlecode.aviator.Expression;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zuihaodeziji on 2020/4/8.
 */
public class MysqlConnUtil {
    static Logger  log = LoggerFactory.getLogger(MysqlConnUtil.class);

    static ComboPooledDataSource cpds;

    static{
        try {
            init();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws  Exception {
        Map<String,ExpressObj> map  = getParseParamConf(ParamConfigUtil.paramSql);
        map.entrySet().stream().forEach(r->{
            //System.out.println("formula is:"+r.getValue())
        });

    }

    private static void init() throws PropertyVetoException {
        cpds = new ComboPooledDataSource();
        cpds.setDriverClass( "com.mysql.jdbc.Driver" );
        cpds.setJdbcUrl("jdbc:mysql://10.11.6.2:3306/evsmc2?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=utf-8&useSSL=false");
        cpds.setUser("root");
        cpds.setPassword("smc@z9w6");
        cpds.setMinPoolSize(5);
        cpds.setMaxPoolSize(100);
        cpds.setAcquireIncrement(5);
    }

    public static Map<String,ExpressObj> getParseParamConf(String sql) throws Exception{


        Connection conn = cpds.getConnection();
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);

        final int ruleIdentityColumn = resultSet.findColumn("ruleIdentity");
        final int nameColumn = resultSet.findColumn("name");
        final int modelIdentitiesColumn = resultSet.findColumn("modelIdentities");
        final int formulaColumn = resultSet.findColumn("formula");
        final int beginTimeThresholdSecondColumn = resultSet.findColumn("beginTimeThresholdSecond");
        final int endTimeThresholdSecondColumn = resultSet.findColumn("endTimeThresholdSecond");
        Map<String,ExpressObj> expressionMap = new HashMap();

         while(resultSet.next()){
             try {
                 final String ruleIdentity = resultSet.getString(ruleIdentityColumn);
                 if (StringUtils.isBlank(ruleIdentity)) {
                     continue;
                 }

                 final String name = resultSet.getString(nameColumn);


                 String modelIdentitiesColumnStr = resultSet.getString(modelIdentitiesColumn);
                 String[] modelIdentitiesArr = parseModelIdentity(modelIdentitiesColumnStr);

                 final String formula = resultSet.getString(formulaColumn);
                 if (StringUtils.isBlank(formula)) {

                     continue;
                 }
                 final Expression expression;
                 try {
                     expression = ExpressUtil.getExpression(formula);
                 } catch (final Exception e) {
                     log.warn("参数报警规则 RULE_ID:{} ROLE_NAME:{} 无法构建表达式[{}]函数, {}", ruleIdentity, name, formula, e.getLocalizedMessage(), e);
                     continue;
                 }

                 final int beginTimeThresholdSecond = resultSet.getInt(beginTimeThresholdSecondColumn);
                 if (beginTimeThresholdSecond < 0) {
                     log.warn("参数报警规则 RULE_ID:{} ROLE_NAME:{} 开始时间阈值[{}]小于0", ruleIdentity, name, beginTimeThresholdSecond);
                 }
                 final int endTimeThresholdSecond = resultSet.getInt(endTimeThresholdSecondColumn);
                 if (endTimeThresholdSecond < 0) {
                     log.warn("参数报警规则 RULE_ID:{} ROLE_NAME:{} 结束时间阈值[{}]小于0", ruleIdentity, name, endTimeThresholdSecond);
                 }
                 ExpressObj expressObj = new ExpressObj(ruleIdentity, name, modelIdentitiesArr, expression, beginTimeThresholdSecond, endTimeThresholdSecond);

                 expressionMap.put(ruleIdentity,expressObj);
             }catch(Exception e){
                 e.printStackTrace();
             }

         }

         conn.close();
         return expressionMap;
    }

    private static String[] parseModelIdentity(String modelIdentitiesColumn){
        if(StringUtils.isNotBlank(modelIdentitiesColumn)){
            return modelIdentitiesColumn.split(",");
        }else{
            return null;
        }
    }

    public static void closeMysqlResource(){
        if(cpds != null){
            cpds.close();
        }
    }

    public  static class ExpressObj{


        private ExpressObj(String ruleIdentity,String name,String[] modelIdentitiesArr,Expression expression,int beginTimeThresholdSecond,int endTimeThresholdSecond){
            this.ruleIdentity = ruleIdentity;
            this.name = name;
            this.modelIdentitiesArr = modelIdentitiesArr;
            this.expression = expression;
            this.beginTimeThresholdSecond = beginTimeThresholdSecond;
            this.endTimeThresholdSecond = endTimeThresholdSecond;
        }
        private String ruleIdentity;
        private String name;
        private String[] modelIdentitiesArr;
        private Expression expression;
        private int beginTimeThresholdSecond;
        private int endTimeThresholdSecond;

        public String getRuleIdentity() {
            return ruleIdentity;
        }

        public String getName() {
            return name;
        }

        public String[] getModelIdentitiesArr() {
            return modelIdentitiesArr;
        }

        public Expression getExpression() {
            return expression;
        }

        public int getBeginTimeThresholdSecond() {
            return beginTimeThresholdSecond;
        }

        public int getEndTimeThresholdSecond() {
            return endTimeThresholdSecond;
        }
    }


}
