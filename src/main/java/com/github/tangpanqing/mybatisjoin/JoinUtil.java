package com.github.tangpanqing.mybatisjoin;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.github.tangpanqing.mybatisjoin.entity.JoinResCount;
import com.github.tangpanqing.mybatisjoin.entity.JoinWhereItem;
import com.github.tangpanqing.mybatisjoin.utils.SpringUtil;
import com.github.tangpanqing.mybatisjoin.utils.StringUtil;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionUtils;

import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/*
    JoinUtils
    .staticTable(SaleOrder.class, "o")
    .select("o.*")
    .select("s", Store::getStoreName, SaleOrderVO::getStoreName)
    .leftJoin(Store.class, "s", Store::getStoreId, "o", SaleOrder::getStoreId)
    .list(SaleOrderVO.class);
*/
public class JoinUtil {

    String tableName = "";
    List<String> fieldList = new ArrayList<>();
    List<String> joinList = new ArrayList<>();
    List<JoinWhereItem> whereList = new ArrayList<>();
    List<Integer> limitList = new ArrayList<>();

    SqlSessionFactory sqlSessionFactory;

    @FunctionalInterface
    public interface SFunction<T, R> extends Function<T, R>, Serializable {
    }

    public JoinUtil() {
    }

    public JoinUtil setSqlSessionFactory(SqlSessionFactory sqlSessionFactory) {
        this.sqlSessionFactory = sqlSessionFactory;
        return this;
    }

    public JoinUtil where(List<JoinWhereItem> whereList) {
        this.whereList.addAll(whereList);
        return this;
    }

    public JoinUtil select(String fieldName) {
        this.fieldList.add(fieldName);
        return this;
    }

    public <T, R> JoinUtil select(String entityAlias, SFunction<T, ?> entityField, SFunction<R, ?> newEntityField) {
        String name = entityAlias + "." + getFieldName(entityField) + " as " + getFieldName(newEntityField);
        this.fieldList.add(name);
        return this;
    }

    public JoinUtil table(Class<?> entity, String entityAlias) {
        this.tableName = getTableName(entity) + " " + entityAlias;
        return this;
    }

    protected String getTableName(Class<?> entity) {
        TableName table = entity.getAnnotation(TableName.class);
        if (null != table) {
            return table.value();
        } else {
            List<String> names = Arrays.asList(entity.getName().split("\\."));
            String className = names.get(names.size() - 1);
            return StringUtil.camelToUnderline(className);
        }
    }

    public static JoinUtil staticTable(Class<?> entity, String entityAlias) {
        return new JoinUtil().table(entity, entityAlias);
    }

    public <T, R> JoinUtil leftJoin(Class<?> thisEntity, String thisEntityAlias, SFunction<T, ?> thisField, String otherEntityAlias, SFunction<R, ?> otherField) {
        String tableName = getTableName(thisEntity);
        String str = "LEFT JOIN " + tableName + " " + thisEntityAlias + " ON " + thisEntityAlias + "." + getFieldName(thisField) + "=" + otherEntityAlias + "." + getFieldName(otherField);
        this.joinList.add(str);
        return this;
    }

    public <T> JoinUtil eq(String entityAlias, SFunction<T, ?> entityField, Object val) {
        String fieldName = entityAlias + "." + getFieldName(entityField);
        this.whereList.add(new JoinWhereItem(fieldName, "=", val));
        return this;
    }

    public <T> JoinUtil inFromObject(String aliasName, SFunction<T, ?> thisEntityField, Object... val) {
        return in(aliasName, thisEntityField, Arrays.asList(val));
    }

    public <T> JoinUtil in(String aliasName, SFunction<T, ?> thisEntityField, Object[] val) {
        return in(aliasName, thisEntityField, Arrays.asList(val));
    }

    public <T> JoinUtil in(String aliasName, SFunction<T, ?> thisEntityField, List<Object> val) {
        String fieldName = aliasName + "." + getFieldName(thisEntityField);
        this.whereList.add(new JoinWhereItem(fieldName, "IN", val));
        return this;
    }

    public <T> JoinUtil between(String aliasName, SFunction<T, ?> thisEntityField, Object fromObject, Object endObject) {
        String fieldName = aliasName + "." + getFieldName(thisEntityField);

        List<Object> vals = new ArrayList<>();
        vals.add(fromObject);
        vals.add(endObject);

        this.whereList.add(new JoinWhereItem(fieldName, "BETWEEN", vals));
        return this;
    }

    public <T> JoinUtil notBetween(String aliasName, SFunction<T, ?> thisEntityField, Object fromObject, Object endObject) {
        String fieldName = aliasName + "." + getFieldName(thisEntityField);

        List<Object> vals = new ArrayList<>();
        vals.add(fromObject);
        vals.add(endObject);

        this.whereList.add(new JoinWhereItem(fieldName, "NOT BETWEEN", vals));
        return this;
    }

    public <T> JoinUtil notIn(String aliasName, SFunction<T, ?> thisEntityField, List<Object> val) {
        String fieldName = aliasName + "." + getFieldName(thisEntityField);
        this.whereList.add(new JoinWhereItem(fieldName, "NOT IN", val));
        return this;
    }

    public <T> JoinUtil gt(String aliasName, SFunction<T, ?> thisEntityField, Object val) {
        String fieldName = aliasName + "." + getFieldName(thisEntityField);
        this.whereList.add(new JoinWhereItem(fieldName, ">", val));
        return this;
    }

    public <T> JoinUtil ge(String aliasName, SFunction<T, ?> thisEntityField, Object val) {
        String fieldName = aliasName + "." + getFieldName(thisEntityField);
        this.whereList.add(new JoinWhereItem(fieldName, ">=", val));
        return this;
    }

    public <T> JoinUtil lt(String aliasName, SFunction<T, ?> thisEntityField, Object val) {
        String fieldName = aliasName + "." + getFieldName(thisEntityField);
        this.whereList.add(new JoinWhereItem(fieldName, "<", val));
        return this;
    }

    public <T> JoinUtil le(String aliasName, SFunction<T, ?> thisEntityField, Object val) {
        String fieldName = aliasName + "." + getFieldName(thisEntityField);
        this.whereList.add(new JoinWhereItem(fieldName, "<=", val));
        return this;
    }

    public JoinUtil pagination(Integer pageNum, Integer pageSize) {
        this.limitList.add(pageNum);
        this.limitList.add(pageSize);
        return this;
    }

    protected String handleLimit(ArrayList<Object> params) {
        if (this.limitList.size() == 0) {
            return "";
        }

        Integer from = (this.limitList.get(0) - 1) * this.limitList.get(1);
        params.add(from);
        params.add(this.limitList.get(1));

        return " Limit ?,?";
    }

    public <T> Page<T> page(Class<T> clazz) {
        Long total = count();
        List<T> records = list(clazz);

        Page<T> page = new Page<>();
        page.setRecords(records);
        page.setTotal(total);
        page.setCurrent(this.limitList.get(0));
        page.setSize(this.limitList.get(1));
        return page;
    }

    public Long count() {
        ArrayList<Object> params = new ArrayList();
        String sql = "SELECT count(*) as total FROM " + this.tableName + " " + handleJoin() + " " + handleWhere(params);

        //System.out.println(sql);
        //System.out.println(params);

        List<JoinResCount> res = executeSql(sql, params, JoinResCount.class);
        if (res.size() == 0) {
            return 0L;
        }

        return res.get(0).getTotal();
    }

    public <T> List<T> list(Class<T> clazz) {
        ArrayList<Object> params = new ArrayList();
        String sql = "SELECT " + handleField() + " FROM " + this.tableName + " " + handleJoin() + " " + handleWhere(params) + " " + handleLimit(params);

        //System.out.println(sql);
        //System.out.println(params);

        return executeSql(sql, params, clazz);
    }

    public <T> T getOne(Class<T> entityClass) {
        List<T> list = list(entityClass);

        return list.get(0);
    }

    protected String handleJoin() {
        if (this.joinList.size() == 0) {
            return "";
        }
        return String.join(" ", this.joinList);
    }

    protected String handleField() {
        if (this.fieldList.size() == 0) {
            return "*";
        }

        return String.join(",", this.fieldList);
    }

    protected String handleWhere(ArrayList<Object> params) {
        if (this.whereList.size() == 0) {
            return "";
        }

        List<String> arr = new ArrayList<>(this.whereList.size());
        for (int i = 0; i < this.whereList.size(); i++) {
            JoinWhereItem whereItem = this.whereList.get(i);

            if (whereItem.getOptName().equals("=") || whereItem.getOptName().equals(">") || whereItem.getOptName().equals(">=") || whereItem.getOptName().equals("<") || whereItem.getOptName().equals("<=")) {
                arr.add(whereItem.getFieldName() + " " + whereItem.getOptName() + " " + "?");
                params.add(whereItem.getVal());
            }

            if (whereItem.getOptName().equals("IN") || whereItem.getOptName().equals("NOT IN")) {
                ArrayList<Object> valList = new ArrayList<>();
                if (whereItem.getVal() instanceof ArrayList<?>) {
                    for (Object o : (List<?>) whereItem.getVal()) {
                        valList.add(Object.class.cast(o));
                    }
                }
                arr.add(whereItem.getFieldName() + " " + whereItem.getOptName() + " " + "(" + String.join(",", valList.stream().map(item -> "?").collect(Collectors.toList())) + ")");
                params.addAll(valList);
            }

            if (whereItem.getOptName().equals("BETWEEN") || whereItem.getOptName().equals("NOT BETWEEN")) {
                ArrayList<Object> valList = new ArrayList<>();
                if (whereItem.getVal() instanceof ArrayList<?>) {
                    for (Object o : (List<?>) whereItem.getVal()) {
                        valList.add(Object.class.cast(o));
                    }
                }

                arr.add(whereItem.getFieldName() + " " + whereItem.getOptName() + " ? AND ?");
                params.addAll(valList);
            }
        }

        return " WHERE " + String.join(" AND ", arr);
    }

    protected <T> List<T> executeSql(String sql, ArrayList<Object> params, Class<T> clazz) {
        List<T> list = new ArrayList<>();
        PreparedStatement pst = null;
        ResultSet result = null;

        if (null == sqlSessionFactory) {
            sqlSessionFactory = SpringUtil.getBean(SqlSessionFactory.class);
        }

        SqlSession session = getSqlSession(sqlSessionFactory);

        try {
            pst = session.getConnection().prepareStatement(sql);

            for (int i = 0; i < params.size(); i++) {
                pst.setObject(i + 1, params.get(i));
            }

            result = pst.executeQuery();
            ResultSetMetaData md = result.getMetaData(); //获得结果集结构信息,元数据

            int columnCount = md.getColumnCount();   //获得列数

            while (result.next()) {
                T rowData = clazz.newInstance();

                for (int i = 1; i <= columnCount; i++) {
                    String columnLabel = md.getColumnLabel(i);
                    String propName = StringUtil.underlineToCamel(columnLabel);

                    Field[] fields = clazz.getDeclaredFields();
                    List<String> fieldNames = new ArrayList<>();
                    for (int m = 0; m < fields.length; m++) {
                        fieldNames.add(fields[m].getName());
                    }
                    if (fieldNames.contains(propName)) {
                        Class<?> propType = clazz.getDeclaredField(propName).getType();
                        String setName = "set" + propName.substring(0, 1).toUpperCase() + propName.substring(1);

                        try {
                            Field field = clazz.getDeclaredField(propName);
                            Method setMethod = clazz.getMethod(setName, field.getType());

                            if (null == result.getObject(columnLabel)) {
                                setMethod.invoke(rowData, null);
                            } else {
                                if (propType.isAssignableFrom(String.class)) {
                                    setMethod.invoke(rowData, result.getString(columnLabel));
                                } else if (propType.isAssignableFrom(int.class) || propType.isAssignableFrom(Integer.class)) {
                                    setMethod.invoke(rowData, result.getInt(columnLabel));
                                } else if (propType.isAssignableFrom(long.class) || propType.isAssignableFrom(Long.class)) {
                                    setMethod.invoke(rowData, result.getLong(columnLabel));
                                } else if (propType.isAssignableFrom(Boolean.class) || propType.isAssignableFrom(boolean.class)) {
                                    setMethod.invoke(rowData, result.getBoolean(columnLabel));
                                } else if (propType.isAssignableFrom(Date.class)) {
                                    setMethod.invoke(rowData, result.getDate(columnLabel));
                                } else if (propType.isAssignableFrom(LocalDateTime.class)) {
                                    try {
                                        setMethod.invoke(rowData, LocalDateTime.parse(result.getString(columnLabel), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                                    } catch (Exception e) {
                                        setMethod.invoke(rowData, null);
                                    }
                                } else if (propType.isAssignableFrom(BigDecimal.class)) {
                                    setMethod.invoke(rowData, result.getBigDecimal(columnLabel));
                                } else {
                                    setMethod.invoke(rowData, result.getObject(columnLabel));
                                }
                            }
                        } catch (Exception e) {

                        }
                    }
                }

                list.add(rowData);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (pst != null) {
                try {
                    pst.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        closeSqlSession(session, sqlSessionFactory);

        return list;
    }

    protected static <T> String getFieldName(SFunction<T, ?> fn) {
        String split = "_";
        Integer toType = 2;

        SerializedLambda serializedLambda = getSerializedLambda(fn);

        // 从lambda信息取出method、field、class等
        String fieldName = serializedLambda.getImplMethodName().substring("get".length());
        fieldName = fieldName.replaceFirst(fieldName.charAt(0) + "", (fieldName.charAt(0) + "").toLowerCase());
        Field field;
        try {
            field = Class.forName(serializedLambda.getImplClass().replace("/", ".")).getDeclaredField(fieldName);
        } catch (ClassNotFoundException | NoSuchFieldException e) {
            throw new RuntimeException(e);
        }

        // 从field取出字段名，可以根据实际情况调整
        TableField tableField = field.getAnnotation(TableField.class);
        if (tableField != null && tableField.value().length() > 0) {
            return tableField.value();
        } else {

            //0.不做转换 1.大写 2.小写
            switch (toType) {
                case 1:
                    return fieldName.replaceAll("[A-Z]", split + "$0").toUpperCase();
                case 2:
                    return fieldName.replaceAll("[A-Z]", split + "$0").toLowerCase();
                default:
                    return fieldName.replaceAll("[A-Z]", split + "$0");
            }

        }

    }

    protected static <T> SerializedLambda getSerializedLambda(SFunction<T, ?> fn) {
        // 从function取出序列化方法
        Method writeReplaceMethod;
        try {
            writeReplaceMethod = fn.getClass().getDeclaredMethod("writeReplace");
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }

        // 从序列化方法取出序列化的lambda信息
        boolean isAccessible = writeReplaceMethod.isAccessible();
        writeReplaceMethod.setAccessible(true);
        SerializedLambda serializedLambda;
        try {
            serializedLambda = (SerializedLambda) writeReplaceMethod.invoke(fn);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        writeReplaceMethod.setAccessible(isAccessible);
        return serializedLambda;
    }

    protected SqlSession getSqlSession(SqlSessionFactory sf) {
        return sf.openSession();
    }

    protected void closeSqlSession(SqlSession session, SqlSessionFactory sf) {
        SqlSessionUtils.closeSqlSession(session, sf);
    }

}
