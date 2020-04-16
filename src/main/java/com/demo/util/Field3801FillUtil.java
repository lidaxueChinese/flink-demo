package com.demo.util;

import org.apache.commons.lang3.math.NumberUtils;

import java.util.Map;

/**
 * Created by zuihaodeziji on 2020/4/8.
 */
public class Field3801FillUtil {

    public static void fillAlarm(final Map<String, String> data) {

        final int alarmMark = NumberUtils.toInt(data.get("3801"), 0);

        // 温度差异报警
        data.put("2901", String.valueOf((alarmMark >>> 0) & 1));
        // 电池高温报警
        data.put("2902", String.valueOf((alarmMark >>> 1) & 1));
        // 车载储能装置类型过压报警
        data.put("2903", String.valueOf((alarmMark >>> 2) & 1));
        // 车载储能装置类型欠压报警
        data.put("2904", String.valueOf((alarmMark >>> 3) & 1));
        // SOC低报警
        data.put("2905", String.valueOf((alarmMark >>> 4) & 1));
        // 单体电池过压报警
        data.put("2906", String.valueOf((alarmMark >>> 5) & 1));
        // 单体电池欠压报警
        data.put("2907", String.valueOf((alarmMark >>> 6) & 1));
        // SOC过高报警
        data.put("2909", String.valueOf((alarmMark >>> 7) & 1));
        // SOC跳变报警
        data.put("2930", String.valueOf((alarmMark >>> 8) & 1));
        // 可充电储能系统不匹配报警
        data.put("2910", String.valueOf((alarmMark >>> 9) & 1));
        // 电池单体一致性差报警
        data.put("2911", String.valueOf((alarmMark >>> 10) & 1));
        // 绝缘报警
        data.put("2912", String.valueOf((alarmMark >>> 11) & 1));
        // DC-DC温度报警
        data.put("2913", String.valueOf((alarmMark >>> 12) & 1));
        // 制动系统报警
        data.put("2914", String.valueOf((alarmMark >>> 13) & 1));
        // DC-DC状态报警
        data.put("2915", String.valueOf((alarmMark >>> 14) & 1));
        // 驱动电机控制器温度报警
        data.put("2916", String.valueOf((alarmMark >>> 15) & 1));
        // 高压互锁状态报警
        data.put("2917", String.valueOf((alarmMark >>> 16) & 1));
        // 驱动电机温度报警
        data.put("2918", String.valueOf((alarmMark >>> 17) & 1));
        // 车载储能装置类型过充(第18位)
        data.put("2919", String.valueOf((alarmMark >>> 18) & 1));
    }
}
