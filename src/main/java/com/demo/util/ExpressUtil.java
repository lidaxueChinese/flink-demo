package com.demo.util;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Map;

/**
 * Created by zuihaodeziji on 2020/4/8.
 */
public class ExpressUtil {

    private static final AviatorEvaluatorInstance AVIATOR_EVALUATOR_INSTANCE = AviatorEvaluator.newInstance();

    public static Expression getExpression(String formula){
        Expression expression =  AVIATOR_EVALUATOR_INSTANCE.compile(formula);
        return expression;
    }

    public static boolean isMeetExpression(Expression expression,Map<String,Object> dataMap){
        try{
           Boolean b =  (Boolean)expression.execute(dataMap);
           return b;
        }catch (Exception e){
            return false;
        }
    }

}
