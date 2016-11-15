package org.apache.nifi.processors.rowstandardizers;


import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class AvroUtils {


    public static Schema.Type getFieldType(Schema.Field field) {
        Schema.Type type = field.schema().getType();

        if (type.equals(Schema.Type.UNION)) {

            List<Schema> fieldTypes = field.schema().getTypes();

            for (Schema fieldSchema : fieldTypes) {

                if (!fieldSchema.getType().equals(Schema.Type.NULL)) {
                    type = fieldSchema.getType();
                    break;
                }

            }

        }

        return type;
    }

    public static Object convertValue(String originalValue, Schema.Type fieldType) {

        Object value = null;

        originalValue = StringUtils.trimToNull(originalValue);

        if (originalValue != null) {

            switch (fieldType) {

                case STRING:
                    value = originalValue;
                    break;
                case INT:
                    value = Integer.valueOf(originalValue);
                    break;
                case LONG:
                    value = Long.valueOf(originalValue);
                    break;
                case FLOAT:
                    value = Float.valueOf(originalValue);
                    break;
                case DOUBLE:
                    value = Double.valueOf(originalValue);
                    break;
                case BOOLEAN:
                    value = Boolean.valueOf(originalValue);
                    break;
                default:
                    break;
            }
        }

        return value;
    }

}
