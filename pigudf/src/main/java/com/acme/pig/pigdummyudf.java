package com.acme.pig;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;


public class pigdummyudf extends EvalFunc<Long> {
	public Long exec(Tuple input) throws IOException {
		try {
			/*
			 * Rather than give you explicit arguments, UDFs are always handed a
			 * tuple. The UDF must know the arguments it expects and pull them
			 * out of the tuple. These next two lines get the first and second
			 * fields out of the input tuple that was handed in. Since Tuple.get
			 * returns Objects, we must cast them to Integers. If the case
			 * fails, an exception will be thrown.
			 */
			int base = (Integer) input.get(0);
			int exponent = (Integer) input.get(1);
			long result = 1;
			/* Probably not the most efficient method... */
			for (int i = 0; i < exponent; i++) {
				long preresult = result;
				result *= base;
				if (preresult > result) {
					// We overflowed. Give a warning, but do not throw an
					// exception.
					warn("Overflow!", PigWarning.TOO_LARGE_FOR_INT);
					// Returning null will indicate to Pig that we failed but
					// we want to continue execution.
					return null;
				}
			}
			return result;
		} catch (Exception e) {
			// Throwing an exception will cause the task to fail.
			throw new IOException("Something bad happened!", e);
		}
	}

	public Schema outputSchema(Schema input) {
		// Check that we were passed two fields
		if (input.size() != 2) {
			throw new RuntimeException(
					"Expected (int, int), input does not have 2 fields");
		}
		try {
			// Get the types for both columns and check them. If they are
			// wrong, figure out what types were passed and give a good error
			// message.
			if (input.getField(0).type != DataType.INTEGER
					|| input.getField(1).type != DataType.INTEGER) {
				String msg = "Expected input (int, int), received schema (";
				msg += DataType.findTypeName(input.getField(0).type);
				msg += ", ";
				msg += DataType.findTypeName(input.getField(1).type);
				msg += ")";
				throw new RuntimeException(msg);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		// Construct our output schema, which is one field that is a long
		return new Schema(new FieldSchema(null, DataType.LONG));
	}
}
