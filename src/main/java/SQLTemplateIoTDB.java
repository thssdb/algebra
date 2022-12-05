public enum SQLTemplateIoTDB {
    IOTDB_DROP_UDF("drop function %s"),
    IOTDB_CREATE_UDF("create function %s as '%s'"),
    IOTDB_UDF_FUNCTION("%s(%s)"),
    IOTDB_SELECT_FROM("select %s from %s"),
    IOTDB_SELECT_FROM_WHERE("select %s from %s where %s"),
    IOTDB_SELECT_FROM_GROUP("select %s from %s group by %s"),
    IOTDB_INSERT("insert into %s values(%s)"),

    GREATER_EQ_THAN("%s >= %s"),
    LESS_THAN("%s < %s"),
    RANGE(" [%s,%s) "),
    PRC("(%s)"),
    FUNC_PRC(" %s(%s) "),

    CONJUNCTION(" %s AND %s "),
    DISJUNCTION(" %s OR %s "),

    FUNCTION_CALL_2ary("%s,%s"),
    FUNCTION_CALL_3ary("%s,%s,%s"),
    FUNCTION_CALL_4ary("%s,%s,%s,%s"),
    FUNCTION_CALL_5ary("%s,%s,%s,%s,%s"),
    FUNCTION_CALL_6ary("%s,%s,%s,%s,%s,%s"),
    ;

    final String template;
    SQLTemplateIoTDB(String template) {
        this.template = template;
    }

    public String getTemplate() {
        return template;
    }
}
