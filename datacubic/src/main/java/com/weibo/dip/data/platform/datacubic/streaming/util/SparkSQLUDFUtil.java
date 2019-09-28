package com.weibo.dip.data.platform.datacubic.streaming.util;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.sql.UDFRegistration;
import org.apache.spark.sql.api.java.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Created by yurun on 17/1/16.
 */
public class SparkSQLUDFUtil {

    private static final String UDF_METHOD_NAME = "call";

    private static boolean isUDF(Class<?> udf) {
        if (!Objects.nonNull(udf)) {
            return false;
        }

        return UDF1.class.isAssignableFrom(udf) || UDF2.class.isAssignableFrom(udf) || UDF3.class.isAssignableFrom(udf)
            || UDF4.class.isAssignableFrom(udf) || UDF5.class.isAssignableFrom(udf) || UDF6.class.isAssignableFrom(udf)
            || UDF7.class.isAssignableFrom(udf) || UDF8.class.isAssignableFrom(udf) || UDF9.class.isAssignableFrom(udf)
            || UDF10.class.isAssignableFrom(udf) || UDF11.class.isAssignableFrom(udf) || UDF12.class.isAssignableFrom(udf)
            || UDF13.class.isAssignableFrom(udf) || UDF14.class.isAssignableFrom(udf) || UDF15.class.isAssignableFrom(udf)
            || UDF16.class.isAssignableFrom(udf) || UDF17.class.isAssignableFrom(udf) || UDF18.class.isAssignableFrom(udf)
            || UDF19.class.isAssignableFrom(udf) || UDF20.class.isAssignableFrom(udf) || UDF21.class.isAssignableFrom(udf)
            || UDF22.class.isAssignableFrom(udf);
    }

    private static DataType javaClassToDataType(Class<?> clazz) {
        if (clazz.equals(Byte.class)) {
            return DataTypes.ByteType;
        } else if (clazz.equals(Short.class)) {
            return DataTypes.ShortType;
        } else if (clazz.equals(Integer.class)) {
            return DataTypes.IntegerType;
        } else if (clazz.equals(Long.class)) {
            return DataTypes.LongType;
        } else if (clazz.equals(Float.class)) {
            return DataTypes.FloatType;
        } else if (clazz.equals(Double.class)) {
            return DataTypes.DoubleType;
        } else if (clazz.equals(BigDecimal.class)) {
            return DataTypes.createDecimalType();
        } else if (clazz.equals(String.class)) {
            return DataTypes.StringType;
        } else if (clazz.equals(Boolean.class)) {
            return DataTypes.BooleanType;
        } else if (clazz.equals(Timestamp.class)) {
            return DataTypes.TimestampType;
        } else if (clazz.equals(Date.class)) {
            return DataTypes.DateType;
        } else {
            throw new UnsupportedOperationException(clazz.getTypeName());
        }
    }

    public static DataType getReturnType(Class<?> udf) {
        Preconditions.checkState(Objects.nonNull(udf) && isUDF(udf), udf.getName() + " isn't a Spark SQL UDF");

        Type[] types = udf.getGenericInterfaces();

        Preconditions.checkState(ArrayUtils.isNotEmpty(types) && types.length == 1, udf.getName() + " must only implement Spark SQL UDF Interface");

        ParameterizedType interfaceType = (ParameterizedType) types[0];

        Type[] actualTypeArguments = interfaceType.getActualTypeArguments();

        Type type = actualTypeArguments[actualTypeArguments.length - 1];//get last type

        if (type instanceof Class) {
            return javaClassToDataType((Class<?>) type);
        } else if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;

            Type rawType = parameterizedType.getRawType();

            if (rawType instanceof Class) {
                Class clazz = (Class) rawType;

                if (clazz.equals(List.class)) {
                    Type actualTypeArgument = parameterizedType.getActualTypeArguments()[0];

                    if (actualTypeArgument instanceof Class) {
                        return DataTypes.createArrayType(javaClassToDataType((Class<?>) actualTypeArgument));
                    }
                } else if (clazz.equals(Map.class)) {
                    Type actualKeyTypeArgument = parameterizedType.getActualTypeArguments()[0];
                    Type actualValueTypeArgument = parameterizedType.getActualTypeArguments()[1];

                    if ((actualKeyTypeArgument instanceof Class) && (actualValueTypeArgument instanceof Class)) {
                        return DataTypes.createMapType(javaClassToDataType((Class<?>) actualKeyTypeArgument), javaClassToDataType((Class<?>) actualValueTypeArgument));
                    }
                }
            }
        }

        throw new UnsupportedOperationException(udf.getTypeName());
    }

    public static void registerUDF(UDFRegistration registration, String name, String classImpl) throws Exception {
        Class<?> clazz = Class.forName(classImpl);

        if (UDF1.class.isAssignableFrom(clazz)) {
            registration.register(name, (UDF1) clazz.newInstance(), getReturnType(clazz));
        } else if (UDF2.class.isAssignableFrom(clazz)) {
            registration.register(name, (UDF2) clazz.newInstance(), getReturnType(clazz));
        } else if (UDF3.class.isAssignableFrom(clazz)) {
            registration.register(name, (UDF3) clazz.newInstance(), getReturnType(clazz));
        } else if (UDF4.class.isAssignableFrom(clazz)) {
            registration.register(name, (UDF4) clazz.newInstance(), getReturnType(clazz));
        } else if (UDF5.class.isAssignableFrom(clazz)) {
            registration.register(name, (UDF5) clazz.newInstance(), getReturnType(clazz));
        } else if (UDF6.class.isAssignableFrom(clazz)) {
            registration.register(name, (UDF6) clazz.newInstance(), getReturnType(clazz));
        } else if (UDF7.class.isAssignableFrom(clazz)) {
            registration.register(name, (UDF7) clazz.newInstance(), getReturnType(clazz));
        } else if (UDF8.class.isAssignableFrom(clazz)) {
            registration.register(name, (UDF8) clazz.newInstance(), getReturnType(clazz));
        } else if (UDF9.class.isAssignableFrom(clazz)) {
            registration.register(name, (UDF9) clazz.newInstance(), getReturnType(clazz));
        } else if (UDF10.class.isAssignableFrom(clazz)) {
            registration.register(name, (UDF10) clazz.newInstance(), getReturnType(clazz));
        } else if (UDF11.class.isAssignableFrom(clazz)) {
            registration.register(name, (UDF11) clazz.newInstance(), getReturnType(clazz));
        } else if (UDF12.class.isAssignableFrom(clazz)) {
            registration.register(name, (UDF12) clazz.newInstance(), getReturnType(clazz));
        } else if (UDF13.class.isAssignableFrom(clazz)) {
            registration.register(name, (UDF13) clazz.newInstance(), getReturnType(clazz));
        } else if (UDF14.class.isAssignableFrom(clazz)) {
            registration.register(name, (UDF14) clazz.newInstance(), getReturnType(clazz));
        } else if (UDF15.class.isAssignableFrom(clazz)) {
            registration.register(name, (UDF15) clazz.newInstance(), getReturnType(clazz));
        } else if (UDF16.class.isAssignableFrom(clazz)) {
            registration.register(name, (UDF16) clazz.newInstance(), getReturnType(clazz));
        } else if (UDF17.class.isAssignableFrom(clazz)) {
            registration.register(name, (UDF17) clazz.newInstance(), getReturnType(clazz));
        } else if (UDF18.class.isAssignableFrom(clazz)) {
            registration.register(name, (UDF18) clazz.newInstance(), getReturnType(clazz));
        } else if (UDF19.class.isAssignableFrom(clazz)) {
            registration.register(name, (UDF19) clazz.newInstance(), getReturnType(clazz));
        } else if (UDF20.class.isAssignableFrom(clazz)) {
            registration.register(name, (UDF20) clazz.newInstance(), getReturnType(clazz));
        } else if (UDF21.class.isAssignableFrom(clazz)) {
            registration.register(name, (UDF21) clazz.newInstance(), getReturnType(clazz));
        } else if (UDF22.class.isAssignableFrom(clazz)) {
            registration.register(name, (UDF22) clazz.newInstance(), getReturnType(clazz));
        } else {
            throw new UnsupportedOperationException(clazz.getTypeName());
        }

    }

}
