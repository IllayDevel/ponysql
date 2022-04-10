package com.pony.database;

public class SqlCastManager {
    static class SQLCastFunction extends AbstractFunction {

        private TType cast_to_type;

        public SQLCastFunction(Expression[] params) {
            super("sql_cast", params);

            // Two parameters - the value to cast and the type to cast to (encoded)
            if (parameterCount() != 2) {
                throw new RuntimeException(
                        "'sql_cast' function must have only 2 arguments.");
            }

            // Get the encoded type and parse it into a TType object and cache
            // locally in this object.  We expect that the second parameter of this
            // function is always constant.
            Expression exp = params[1];
            if (exp.size() != 1) {
                throw new RuntimeException(
                        "'sql_cast' function must have simple second parameter.");
            }

            Object vob = params[1].last();
            if (vob instanceof TObject) {
                TObject ob = (TObject) vob;
                String encoded_type = ob.getObject().toString();
                cast_to_type = TType.decodeString(encoded_type);
            } else {
                throw new RuntimeException(
                        "'sql_cast' function must have simple second parameter.");
            }
        }

        public TObject evaluate(GroupResolver group, VariableResolver resolver,
                                QueryContext context) {

            TObject ob = getParameter(0).evaluate(group, resolver, context);
            // If types are the same then no cast is necessary and we return this
            // object.
            if (ob.getTType().getSQLType() == cast_to_type.getSQLType()) {
                return ob;
            }
            // Otherwise cast the object and return the new typed object.
            Object casted_ob = TType.castObjectToTType(ob.getObject(), cast_to_type);
            return new TObject(cast_to_type, casted_ob);

        }

        public TType returnTType(VariableResolver resolver, QueryContext context) {
            return cast_to_type;
        }

    }


}
