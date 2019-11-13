package com.p7.framework.redis.timing.task.redis;

public class CacheKeyGenerator {

    /**
     * 判定指定的 Class 对象是否表示一个基本类型或者包装器类型
     *
     * @param clazz
     * @return
     */
    @SuppressWarnings("rawtypes")
    public static boolean isPrimitive(Class clazz) {
        if (clazz.isPrimitive()) {
            return true;
        } else
            try {
                if (clazz.getField("TYPE") != null &&
                        ((Class) (clazz.getField("TYPE").get(null))).isPrimitive()) {
                    return true;
                }
            } catch (Exception e) {

            }
        return false;
    }

}
