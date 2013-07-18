package com.cloudera.example;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class ParseTest {

	private TupleFactory tupleFactory = TupleFactory.getInstance();
	
	@Test
	public void testA() throws IOException {
		Parse parse = new Parse();
		DataBag bag = parse.exec(tupleFactory.newTuple("201307181540 A,123"));
		assertEquals(1l, bag.size());
		Iterator<Tuple> it = bag.iterator();
		Tuple t1 = it.next();
		assertEquals("201307181540", t1.get(0));
		assertEquals(123, t1.get(1));
		assertEquals(null, t1.get(2));
		assertEquals(null, t1.get(3));
		assertEquals(4, t1.size());
	}
	
	@Test
	public void testB() throws IOException {
		Parse parse = new Parse();
		DataBag bag = parse.exec(tupleFactory.newTuple("201307181542 B,0.125,true"));
		assertEquals(1l, bag.size());
		Iterator<Tuple> it = bag.iterator();
		Tuple t1 = it.next();
		assertEquals("201307181542", t1.get(0));
		assertEquals(null, t1.get(1));
		assertEquals(0.125f, t1.get(2));
		assertEquals(true, t1.get(3));
		assertEquals(4, t1.size());
	}

	@Test
	public void testAA() throws IOException {
		Parse parse = new Parse();
		DataBag bag = parse.exec(tupleFactory.newTuple("201307181540 A,123 A,124"));
		assertEquals(2l, bag.size());
		Iterator<Tuple> it = bag.iterator();
		Tuple t1 = it.next();
		assertEquals("201307181540", t1.get(0));
		assertEquals(123, t1.get(1));
		assertEquals(null, t1.get(2));
		assertEquals(null, t1.get(3));
		assertEquals(4, t1.size());
		Tuple t2 = it.next();
		assertEquals("201307181540", t2.get(0));
		assertEquals(124, t2.get(1));
		assertEquals(null, t2.get(2));
		assertEquals(null, t2.get(3));
		assertEquals(t2.size(), 4);
	}
	
	@Test
	public void testAB() throws IOException {
		Parse parse = new Parse();
		DataBag bag = parse.exec(tupleFactory.newTuple("201307181540 A,123 B,0.125,true"));
		assertEquals(2l, bag.size());
		Iterator<Tuple> it = bag.iterator();
		Tuple t1 = it.next();
		assertEquals("201307181540", t1.get(0));
		assertEquals(123, t1.get(1));
		assertEquals(null, t1.get(2));
		assertEquals(null, t1.get(3));
		assertEquals(4, t1.size());
		Tuple t2 = it.next();
		assertEquals("201307181540", t2.get(0));
		assertEquals(null, t2.get(1));
		assertEquals(0.125f, t2.get(2));
		assertEquals(true, t2.get(3));
		assertEquals(4, t2.size());
	}
	
	@Test
	public void testBA() throws IOException {
		Parse parse = new Parse();
		DataBag bag = parse.exec(tupleFactory.newTuple("201307181540 B,0.125,true A,123"));
		assertEquals(2l, bag.size());
		Iterator<Tuple> it = bag.iterator();
		Tuple t1 = it.next();
		assertEquals("201307181540", t1.get(0));
		assertEquals(null, t1.get(1));
		assertEquals(0.125f, t1.get(2));
		assertEquals(true, t1.get(3));
		assertEquals(4, t1.size());
		Tuple t2 = it.next();
		assertEquals("201307181540", t2.get(0));
		assertEquals(123, t2.get(1));
		assertEquals(null, t2.get(2));
		assertEquals(null, t2.get(3));
		assertEquals(4, t2.size());
	}

	@Test
	public void testBB() throws IOException {
		Parse parse = new Parse();
		DataBag bag = parse.exec(tupleFactory.newTuple("201307181540 B,0.125,true B,0.126,false"));
		assertEquals(2l, bag.size());
		Iterator<Tuple> it = bag.iterator();
		Tuple t1 = it.next();
		assertEquals("201307181540", t1.get(0));
		assertEquals(null, t1.get(1));
		assertEquals(0.125f, t1.get(2));
		assertEquals(true, t1.get(3));
		assertEquals(4, t1.size());
		Tuple t2 = it.next();
		assertEquals("201307181540", t2.get(0));
		assertEquals(null, t2.get(1));
		assertEquals(0.126f, t2.get(2));
		assertEquals(false, t2.get(3));
		assertEquals(4, t2.size());
	}
	
	@Test
	public void testABB() throws IOException {
		Parse parse = new Parse();
		DataBag bag = parse.exec(tupleFactory.newTuple("201307181540 A,123 B,0.125,true B,0.126,false"));
		assertEquals(3l, bag.size());
		Iterator<Tuple> it = bag.iterator();
		Tuple t1 = it.next();
		assertEquals("201307181540", t1.get(0));
		assertEquals(123, t1.get(1));
		assertEquals(null, t1.get(2));
		assertEquals(null, t1.get(3));
		assertEquals(4, t1.size());
		Tuple t2 = it.next();
		assertEquals("201307181540", t2.get(0));
		assertEquals(null, t2.get(1));
		assertEquals(0.125f, t2.get(2));
		assertEquals(true, t2.get(3));
		assertEquals(4, t2.size());
		Tuple t3 = it.next();
		assertEquals("201307181540", t3.get(0));
		assertEquals(null, t3.get(1));
		assertEquals(0.126f, t3.get(2));
		assertEquals(false, t3.get(3));
		assertEquals(4, t3.size());
	}
	
	@Test
	public void testBAB() throws IOException {
		Parse parse = new Parse();
		DataBag bag = parse.exec(tupleFactory.newTuple("201307181540 B,0.125,true A,123 B,0.126,false"));
		assertEquals(3l, bag.size());
		Iterator<Tuple> it = bag.iterator();
		Tuple t1 = it.next();
		assertEquals("201307181540", t1.get(0));
		assertEquals(null, t1.get(1));
		assertEquals(0.125f, t1.get(2));
		assertEquals(true, t1.get(3));
		assertEquals(4, t1.size());
		Tuple t2 = it.next();
		assertEquals("201307181540", t2.get(0));
		assertEquals(123, t2.get(1));
		assertEquals(null, t2.get(2));
		assertEquals(null, t2.get(3));
		assertEquals(4, t2.size());
		Tuple t3 = it.next();
		assertEquals("201307181540", t3.get(0));
		assertEquals(null, t3.get(1));
		assertEquals(0.126f, t3.get(2));
		assertEquals(false, t3.get(3));
		assertEquals(4, t3.size());
	}
}
