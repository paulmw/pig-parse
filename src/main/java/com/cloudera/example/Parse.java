package com.cloudera.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class Parse extends EvalFunc<DataBag> {

	private TupleFactory tupleFactory = TupleFactory.getInstance();
	private BagFactory bagFactory = BagFactory.getInstance();
	private DataBag output = bagFactory.newDefaultBag();

	public DataBag exec(Tuple input) throws IOException {
		try {

			String line = (String) input.get(0);
			String [] lines = line.split(" ");
			String date = lines[0];

			for(int i = 1; i < lines.length; i++) {
				String [] portions = lines[i].split(",");
				if(portions[0].equals("A")) {
					outputA(date, Integer.parseInt(portions[1]));
				}
				if(portions[0].equals("B")) {
					outputB(date, Float.parseFloat(portions[1]), Boolean.parseBoolean(portions[2]));
				}
			}

			return output;
		} catch (Exception e) {
			return null;
		}
	} 

	private void outputA(String date, int i) {
		List<Object> tuple = new ArrayList<Object>();
		tuple.add(date);
		tuple.add(i);
		tuple.add(null);
		tuple.add(null);
		output.add(tupleFactory.newTuple(tuple));
	}

	private void outputB(String date, float f, boolean b) {
		List<Object> tuple = new ArrayList<Object>();
		tuple.add(date);
		tuple.add(null);
		tuple.add(f);
		tuple.add(b);
		output.add(tupleFactory.newTuple(tuple));
	}

	public Schema outputSchema(Schema input) {
		Schema schema = new Schema();
		schema.add(new Schema.FieldSchema("date", DataType.CHARARRAY));
		schema.add(new Schema.FieldSchema("a1", DataType.INTEGER));
		schema.add(new Schema.FieldSchema("b1", DataType.FLOAT));
		schema.add(new Schema.FieldSchema("b2", DataType.BOOLEAN));
		return schema;
	}
}
