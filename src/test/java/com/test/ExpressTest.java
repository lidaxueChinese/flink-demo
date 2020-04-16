package com.test;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;

import java.util.HashMap;
import java.util.Map;


/**
 * Created by zuihaodeziji on 2020/4/8.
 */
public class ExpressTest {


    public static void main(String[] args) {
        //Expression expression =  AVIATOR_EVALUATOR_INSTANCE.compile("d2301==1.0");
        Map<String,Object> hashMap = new HashMap<String,Object>();
        hashMap.put("d343",new Double("1"));
        hashMap.put("d2301",new Double("1"));

       // Object ff = expression.execute(hashMap);

        //System.out.println("ff:"+ff);

    }
}
