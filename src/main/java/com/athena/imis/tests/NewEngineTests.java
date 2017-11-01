package com.athena.imis.tests;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.collections4.keyvalue.MultiKey;
import org.apache.commons.collections4.map.MultiKeyMap;

public class NewEngineTests {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
		Random random = new Random();
		HashSet<List<Integer>> table_a = new HashSet<List<Integer>>();
		HashSet<List<Integer>> table_b = new HashSet<List<Integer>>();
		long start = System.nanoTime();
		for(int i = 0; i < 1000000; i++){
			List<Integer> list_a = new ArrayList<Integer>();
			List<Integer> list_b = new ArrayList<Integer>();
			list_a.add(random.nextInt(10000));
			list_a.add(random.nextInt(10000));
			list_a.add(random.nextInt(10000));
			list_a.add(random.nextInt(10000));
			table_a.add(list_a);			
		}
		for(int i = 0; i < 1000000; i++){			
			List<Integer> list_b = new ArrayList<Integer>();					
			list_b.add(random.nextInt(50000));
			list_b.add(random.nextInt(50000));
			list_b.add(random.nextInt(50000));
			table_b.add(list_b);
		}
		long end = System.nanoTime();
		System.out.println((end-start));
		start = System.nanoTime();
		//MultiKeyMap multiKeyMap=MultiKeyMap.decorate(new HashMap());
		 MultiKeyMap<Integer, List<Integer>> probeMap = new MultiKeyMap<Integer, List<Integer>>();
		 int count = 0;
		 for(List<Integer> table_a_row : table_a){
			 probeMap.put( table_a_row.get(0), table_a_row.get(1), table_a_row); 
		 }
		 
		 for(List<Integer> table_b_row : table_b){
			 if(probeMap.containsKey(table_b_row.get(0), table_b_row.get(1))){
				 count++;
			 }
		 }   
		 end = System.nanoTime();
		    //multiKeyMap.put( "a2", "b2", "c2", "value1");

//		    for(Map.Entry<MultiKey<? extends Integer>, Integer> entry: multiKeyMap.entrySet()){
//		        System.out.println(entry.getKey().getKey(0)
//		                +" "+entry.getKey().getKey(1)		                
//		                + " value: "+entry.getValue());
//		    }
		
		System.out.println(count);
		System.out.println((end-start));
		
//		for(int j = 0; j < 10; j++){
//			long start = System.nanoTime();
//			IndexedCollection<TripleAsIntStatic> a_triples = new ConcurrentIndexedCollection<TripleAsIntStatic>();
//			IndexedCollection<TripleAsIntStatic> b_triples = new ConcurrentIndexedCollection<TripleAsIntStatic>();
//			TripleAsIntStatic tai ;
//			Random random = new Random();
//			for(int i = 0; i < 1000000; i++){
//				tai = new TripleAsIntStatic(i, random.nextInt(50), random.nextInt(1000000));
//				a_triples.add(tai);
//				tai = new TripleAsIntStatic(i, random.nextInt(50), random.nextInt(1000000));
//				b_triples.add(tai);
//			}
//			a_triples.addIndex(HashIndex.onAttribute(TripleAsIntStatic.O));
//			b_triples.addIndex(HashIndex.onAttribute(TripleAsIntStatic.S));
//			
//			Query<TripleAsIntStatic> a_query =                 
//	                existsIn(a_triples,
//	                		TripleAsIntStatic.S,
//	                		TripleAsIntStatic.O                                       
//	        );
//			int count = 0;
//			for (TripleAsIntStatic a_triple : a_triples.retrieve(a_query)) {
//	            //System.out.println(a_triple.s + " joins with " + );
//				Query<TripleAsIntStatic> b_query =                 
//		                equal(
//		                		TripleAsIntStatic.S,
//		                		a_triple.o                                       
//		        );
//				for (TripleAsIntStatic b_triple : b_triples.retrieve(b_query)) {
//					//System.out.println("a_triple (o): " + a_triple.o + ", b_triple (s): " + b_triple.s);
//					count++;
//				}
//				
//	        }
//			long end = System.nanoTime();
//			System.out.println("joined triples: " + count);
//			System.out.println("elapsed time: " + (end-start));
//			count = 0;
//			start = System.nanoTime();
//			HashSet<Integer> hashTable = new HashSet();
//			for(TripleAsIntStatic next_a : a_triples){
//				hashTable.add(next_a.o);
//			}
//			for(TripleAsIntStatic next_b : b_triples){
//				if(hashTable.contains(next_b.s)){
//					count++;
//				}
//			}
//			end = System.nanoTime();
//			System.out.println("joined triples: " + count);
//			System.out.println("elapsed time: " + (end-start));
//		}
		
		

	}

}
