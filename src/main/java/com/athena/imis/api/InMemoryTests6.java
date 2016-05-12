package com.athena.imis.tests;

import java.io.File;
import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.Vector;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.vocabulary.RDF;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import com.athena.imis.models.AnswerPattern;
import com.athena.imis.models.CharacteristicSet;
import com.athena.imis.models.ECSQuery;
import com.athena.imis.models.ECSTuple;
import com.athena.imis.models.ExtendedCharacteristicSet;
import com.athena.imis.models.QueryPattern;
import com.athena.imis.models.StackRow;
import com.athena.imis.models.TripleAsInt;

public class InMemoryTests6 {

	public static String prefix = "http://example.com/schema#";
	//public static String directory = "C:/temp/TDB_sp";
	//public static String directory = "C:/temp/TDB100";
	public static String directory = "C:/temp/TDB";
	//public static String directory = "/data1/mmeimaris/TDB";
	public static HashMap<String, Integer> propertiesSet = new HashMap<String, Integer>();
	public static HashMap<Integer, String> reversePropertiesSet = new HashMap<Integer, String>();
	//public static HashMap<String, Integer> propertiesSetNodes = new HashMap<String, Integer>();
	public static HashSet<ExtendedCharacteristicSet> visited = new HashSet<ExtendedCharacteristicSet>();
	public static HashMap<Node, Integer> intMap = new HashMap<Node, Integer>(200000);
	public static HashMap<Integer, Node> reverseIntMap = new HashMap<Integer, Node>(200000);
	public static Map<BigInteger, long[]> explicitHash;
	public static Map<Integer, long[]> dbECSMap;
	public static HashMap<Integer, long[]> cacheMap = new HashMap<Integer, long[]>();
	public static HashMap<Integer, long[]> cacheMapOS = new HashMap<Integer, long[]>();
	public static Map<Integer, long[]> dbECSMapOS;
	public static int nextECSTuple = 0;
	
	
	public static HashMap<ExtendedCharacteristicSet, Integer> ecsIntegerMap ;
	public static HashMap<Integer, ExtendedCharacteristicSet> integerECSMap ;
	public static HashMap<ECSTuple, Integer> ecsTupleIntegerMap ;
	public static HashMap<Integer, ECSTuple> reverseECSTupleIntegerMap ;
	
	public static HashMap<Node, Integer> varIndexMap ;
	
	public static HashMap<ArrayList<ExtendedCharacteristicSet>, HashSet<Integer>> qpVarMap;
	
	public static HashMap<ArrayList<ECSTuple>, HashSet<Integer>> qpVarMap2;
	
	public static HashMap<ExtendedCharacteristicSet, ArrayList<Long>> ecsLongMap = 
				new HashMap<ExtendedCharacteristicSet, ArrayList<Long>>();
	
	public static HashMap<ExtendedCharacteristicSet, long[]> ecsLongArrayMap ;
	
	public static HashMap<Integer, HashMap<Integer, Integer>> propIndexMap ;
	public static HashMap<Integer, HashMap<Integer, Integer>> propIndexMapReverse ;
	
	public static HashMap<Integer, HashMap<Integer, HashMap<Integer, Integer>>> subjectIndexMap ;
	public static HashMap<Integer, HashMap<Integer, HashMap<Integer, Integer>>> subjectIndexMapReverse ;
	
	public static HashMap<ExtendedCharacteristicSet, long[]> ecsLongArrayMapOS ;
	
	public static HashMap<Long, ExtendedCharacteristicSet> longECSMap = 
			new HashMap<Long, ExtendedCharacteristicSet>();
	
	public static long[] spoIndex, opsIndex ;
	
	public static HashMap<ExtendedCharacteristicSet, ArrayList<Long>> ecsLongOSMap = 
				new HashMap<ExtendedCharacteristicSet, ArrayList<Long>>();
	
	public static DB db ;
	
	public static Model inmem ;
	
	public static HashSet<CharacteristicSet> sskilllist ;
	
	public static HashSet<CharacteristicSet> ookilllist ;
	
	public static HashMap<Integer, HashSet<ECSTuple>> sslist ;
	
	public static HashMap<Integer, HashSet<ECSTuple>> oolist ;
	
	//public static HashMap<Integer, BloomFilter<Long>> bloom = new HashMap<Integer, BloomFilter<Long>>();
	
	public static ArrayList<Long> times = new ArrayList<Long>();
	
	public static HashMap<Node, CharacteristicSet> characteristicSetMap = new HashMap<Node, CharacteristicSet>(200000);
	
	public static void main(String[] args){
		
		
		Dataset dataset = TDBFactory.createDataset(directory);	   
		
		inmem = dataset.getDefaultModel();		
		
		File file = new File("C:/temp/testMap2");
		
		//File file = new File("/data1/mmeimaris/testMap2");
		
 		db = DBMaker.newFileDB(file)
 				.transactionDisable()
 				.fileChannelEnable() 			
 				.fileMmapEnable()
 				.cacheSize(1000000000) 				
 				.closeOnJvmShutdown()
 				.make();
 		
		ResIterator subres = inmem.listSubjects();
		int propIndex = 0;
		int nextInd = 0;
		
		HashSet<Node> properties = new HashSet<Node>();
		//HashSet<Integer> propertiesInt = new HashSet<Integer>();
		NodeIterator it = null;
		Triple tr ;
		Node predicate ;
		Node object ;
		Resource subject ;
		StmtIterator propertiesOfSubject = null;
		CharacteristicSet cs ;
		HashSet<CharacteristicSet> ucs = new HashSet<CharacteristicSet>();
		while(subres.hasNext()){
			subject = subres.next();
			propertiesOfSubject = subject.listProperties();
			properties = new HashSet<Node>();
			//propertiesInt = new HashSet<Integer>();
			if(!intMap.containsKey(subject.asNode())){
    			reverseIntMap.put(nextInd, subject.asNode());
    			intMap.put(subject.asNode(), nextInd++);
    		}
			while(propertiesOfSubject.hasNext()){
				tr = propertiesOfSubject.next().asTriple();
				predicate = tr.getPredicate();
				
				if(!propertiesSet.containsKey(predicate.getURI())){
			 		reversePropertiesSet.put(propIndex, predicate.getURI());
		    		propertiesSet.put(predicate.getURI(), propIndex++);
		    		//properties.add(predicate);
		    		intMap.put(predicate, propertiesSet.get(predicate.getURI()));
		    	}
				if(!properties.contains(predicate))
					properties.add(predicate);
				/*if(!propertiesInt.contains(propertiesSet.get(predicate.getURI())))
					propertiesInt.add(propertiesSet.get(predicate.getURI()));*/
		 		
				it = inmem.listObjectsOfProperty(subject, ResourceFactory.createProperty(predicate.toString()));			 	
			 									
			    while(it.hasNext()){
			    		object = it.next().asNode();	    		
			    					    		
			    		if(!intMap.containsKey(object)){
			    			reverseIntMap.put(nextInd, object);			    	
			    			intMap.put(object, nextInd++);
			    		}
			    }
			    
			    
			}	
			cs = new CharacteristicSet(subject.asNode(), properties, true);
			if(!ucs.contains(cs)){
				ucs.add(cs);
				/*for(Node p : properties){
					System.out.print(p.toString()+", ");
				}
				System.out.println();*/
			}
			/*cs = new CharacteristicSet(propertiesInt, true);
			ucs.add(cs);*/
	 		characteristicSetMap.put(subject.asNode(), cs);
				
		}
		subres.close();
		propertiesOfSubject.close();
		it.close();
		System.out.println(inmem.size());
		System.out.println("ucs " + ucs.size());
		/*for(CharacteristicSet c2s : ucs){
			System.out.println(c2s.longRep);
		}*/
	 	
 		System.out.println("Unique nodes with CS: " + characteristicSetMap.size() + " p " + propIndex);
 		
 		ecsIntegerMap = new HashMap<ExtendedCharacteristicSet, Integer>();
 		
 		integerECSMap = new HashMap<Integer, ExtendedCharacteristicSet>();
 		
 		ecsTupleIntegerMap = new HashMap<ECSTuple, Integer>();
 		
 		reverseECSTupleIntegerMap= new HashMap<Integer, ECSTuple>();
 		
 		HashMap<ExtendedCharacteristicSet, HashSet<Triple>> ecsTripleMap = 
 				new HashMap<ExtendedCharacteristicSet, HashSet<Triple>>();
 		
 		HashMap<Node, HashSet<ExtendedCharacteristicSet>> subjectECSMap = 
 				new HashMap<Node, HashSet<ExtendedCharacteristicSet>>();
 		
 		
 		CharacteristicSet subjectCS, objectCS; 	
 		
 		ArrayList<Long> dummyLong;
 		HashSet<ExtendedCharacteristicSet> dummyExtendedCharacteristicSet;
 		int ecsIndex = 0;
 		
 		spoIndex = new long[(int) inmem.size()];
 		
 		opsIndex = new long[(int) inmem.size()];
 		
 		ExtendedCharacteristicSet ecsobject;
 		long tripleLong ;
 		long tripleOSLong ;
 		long tripleSPOLong ;
 		long tripleOPSLong ;
 		
 		int tripleIndex = 0;
 		
 		StmtIterator trit = inmem.listStatements();
 		HashMap<CharacteristicSet, HashSet<ExtendedCharacteristicSet>> subjectCStoECS = new 
 				HashMap<CharacteristicSet, HashSet<ExtendedCharacteristicSet>>();
 		
 		HashMap<CharacteristicSet, HashSet<ExtendedCharacteristicSet>> objectCStoECS = new 
 				HashMap<CharacteristicSet, HashSet<ExtendedCharacteristicSet>>();
 		
 		HashMap<ExtendedCharacteristicSet, Integer> ecsCounts = new HashMap<ExtendedCharacteristicSet, Integer>();
 		int cou ;
 		while(trit.hasNext()){
 			Triple triple = trit.next().asTriple();
 			subjectCS = characteristicSetMap.get(triple.getSubject());
 			
 			objectCS = null;
 			
 			if(characteristicSetMap.containsKey(triple.getObject())){
 				objectCS = characteristicSetMap.get(triple.getObject());
 			}
 			
 			ecsobject = new ExtendedCharacteristicSet(subjectCS, objectCS);
 			if(!ecsIntegerMap.containsKey(ecsobject)){ 		
 				integerECSMap.put(ecsIndex, ecsobject);
 				ecsIntegerMap.put(ecsobject, ecsIndex++);
 			}
 			cou = ecsCounts.getOrDefault(ecsobject, 0);
 			ecsCounts.put(ecsobject, ++cou);
 			HashSet<ExtendedCharacteristicSet> d = subjectCStoECS.getOrDefault(subjectCS, new HashSet<ExtendedCharacteristicSet>());
 			d.add(ecsobject);
 			subjectCStoECS.put(subjectCS, d);
 			HashSet<ExtendedCharacteristicSet> d2 = objectCStoECS.getOrDefault(objectCS, new HashSet<ExtendedCharacteristicSet>());
 			d2.add(ecsobject);
 			objectCStoECS.put(objectCS, d2);
 			
 		}
 		trit.close();
 		dbECSMap = db.hashMapCreate("ecsMap")
 				.keySerializer(Serializer.INTEGER)
 				.valueSerializer(Serializer.LONG_ARRAY)
 				.makeOrGet();
 		dbECSMapOS = db.hashMapCreate("mapECS")
 				.keySerializer(Serializer.INTEGER)
 				.valueSerializer(Serializer.LONG_ARRAY) 				
 				.makeOrGet();
 		HashMap<ExtendedCharacteristicSet, Integer> nextIndMap = new HashMap<ExtendedCharacteristicSet, Integer>();
 		HashMap<Integer, ArrayList<Long>> newMap = new HashMap<Integer, ArrayList<Long>>(); 
 		/*for(ExtendedCharacteristicSet ecs : ecsCounts.keySet()){
 			//dbECSMap.put(ecsIntegerMap.get(ecs), new long[ecsCounts.get(ecs)]);
 			//newMap.put(ecsIntegerMap.get(ecs), new long[ecsCounts.get(ecs)]);
 			//newMap.put(ecsIntegerMap.get(ecs), new ArrayList<Long>());
 			//nextIndMap.put(ecs, 0);
 		}*/
 		ExtendedCharacteristicSet curECS;
 		//long[] arr ;
 		trit = inmem.listStatements();
 		ArrayList<Long> list ;
 		Triple triple ;
 		long[] result ;
 		if(dbECSMap.isEmpty())
 		while(trit.hasNext()){
 			triple = trit.next().asTriple();
 			curECS = getTripleECS(triple);
 			list = newMap.getOrDefault(ecsIntegerMap.get(curECS), new ArrayList<Long>() ); 			
 			tripleLong = ((long)propertiesSet.get(triple.getPredicate().getURI()) << 54 | (long)intMap.get(triple.getSubject()) << 27 | (long)intMap.get(triple.getObject()));
 			list.add(tripleLong);
 			newMap.put(ecsIntegerMap.get(curECS), list);
 			if(list.size() == ecsCounts.get(curECS)) {
 				if(!dbECSMap.containsKey(ecsIntegerMap.get(curECS))){
 					//System.out.println("list size: " + list.size());
 					result = list.stream().mapToLong(l -> l).toArray();
 					//System.out.println(Arrays.toString(result));
 					Arrays.sort(result);
 					dbECSMap.put(ecsIntegerMap.get(curECS), result);
 					newMap.remove(ecsIntegerMap.get(curECS));
 				}
 				continue;
 			}
 			//nextIndMap.put(curECS, nextIndMap.get(curECS)+1);
 		}
 		trit.close();
 		System.out.println("Step 1");
 		int zer = 0;
 		for(Integer ecs : dbECSMap.keySet()){
 			ExtendedCharacteristicSet ec = integerECSMap.get(ecs);
 			if(ec.objectCS == null){
 				zer += dbECSMap.get(ecs).length;
 			}
 		}
 		System.out.println("zer length: " + zer);
 		/*int size = 0;
 		for(Integer iter : dbECSMap.keySet()){
 			long[] array = dbECSMap.get(iter); 
 			Arrays.sort(array);
 			dbECSMap.put(iter, array);
 			//System.out.println(Arrays.toString(dbECSMap.get(iter)));
 			size += dbECSMap.get(iter).length;
 		}
 		System.out.println("size: " + size);
 		System.out.println("Step 2");*/
 		//System.out.println()
 		
 		while(trit.hasNext() && false){
 			triple = trit.next().asTriple();
 			
 			//if(triple.getPredicate().getURI().contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#_")) continue;
 			
 			subjectCS = characteristicSetMap.get(triple.getSubject());
 			
 			objectCS = null;
 			
 			if(characteristicSetMap.containsKey(triple.getObject())){
 				objectCS = characteristicSetMap.get(triple.getObject());
 			}
 			
 			ecsobject = new ExtendedCharacteristicSet(subjectCS, objectCS);
 			HashSet<ExtendedCharacteristicSet> d = subjectCStoECS.getOrDefault(subjectCS, new HashSet<ExtendedCharacteristicSet>());
 			d.add(ecsobject);
 			subjectCStoECS.put(subjectCS, d);
 			HashSet<ExtendedCharacteristicSet> d2 = objectCStoECS.getOrDefault(objectCS, new HashSet<ExtendedCharacteristicSet>());
 			d2.add(ecsobject);
 			objectCStoECS.put(objectCS, d2);
 			if(!ecsIntegerMap.containsKey(ecsobject)){
 				//integerECSMap.put(ecsIndex, ecsobject);
 				ecsIntegerMap.put(ecsobject, ecsIndex++);
 			}
 			 			
 			/*if(ecsTripleMap.containsKey(ecsobject)){
 				ecsTripleMap.get(ecsobject).add(triple);
 			}
 			else{
 				dummyTriple = new HashSet<Triple>();
 				dummyTriple.add(triple);
 				ecsTripleMap.put(ecsobject, dummyTriple);
 			}
 			reverseEcsTripleMap.put(triple, ecsobject);*/
 			 			
 			tripleLong = ((long)propertiesSet.get(triple.getPredicate().getURI()) << 54 | (long)intMap.get(triple.getSubject()) << 27 | (long)intMap.get(triple.getObject()));
 			tripleOSLong = ((long)propertiesSet.get(triple.getPredicate().getURI()) << 54 | (long)intMap.get(triple.getObject()) << 27 | (long)intMap.get(triple.getSubject()));
 			tripleSPOLong = ((long)intMap.get(triple.getSubject()) << 37 | (long)propertiesSet.get(triple.getPredicate().getURI()) << 27 | (long)intMap.get(triple.getObject()));
 			
 			spoIndex[tripleIndex] = tripleSPOLong;
 			
 			tripleOPSLong = ((long)intMap.get(triple.getObject()) << 37 | (long)propertiesSet.get(triple.getPredicate().getURI()) << 27 | (long)intMap.get(triple.getSubject()));
 			
 			opsIndex[tripleIndex++] = tripleOPSLong;
 			
 			longECSMap.put(tripleLong, ecsobject);
 			if(ecsLongMap.containsKey(ecsobject)){
 				ecsLongMap.get(ecsobject).add(tripleLong);
 				ecsLongOSMap.get(ecsobject).add(tripleOSLong);
 				
 			}
 			else{
 				dummyLong = new ArrayList<Long>();
 				dummyLong.add(tripleLong);
 				ecsLongMap.put(ecsobject, dummyLong);
 				
 				ecsLongOSMap.put(ecsobject, dummyLong);
 			}
 			 		 			
 			if(subjectECSMap.containsKey(triple.getSubject())){
 				subjectECSMap.get(triple.getSubject()).add(ecsobject);
 			}
 			else{
 				dummyExtendedCharacteristicSet = new HashSet<ExtendedCharacteristicSet>();
 				dummyExtendedCharacteristicSet.add(ecsobject);
 				subjectECSMap.put(triple.getSubject(), dummyExtendedCharacteristicSet);
 			}
 			//if(objectCS == null) continue;
 		}
 			
 		for(ExtendedCharacteristicSet e : ecsLongMap.keySet()){
 			Collections.sort(ecsLongMap.get(e));
 			Collections.sort(ecsLongOSMap.get(e));
 		}
 		
 		Arrays.sort(spoIndex);
 		Arrays.sort(opsIndex);
 		System.out.println("Unique ECS: " + ecsIntegerMap.size()); 		
 		System.out.println("Unique ECS in Map: " + ecsTripleMap.size());
 		int total = 0;
 		/*
 		for(ExtendedCharacteristicSet ecs  : ecsTripleMap.keySet()){
 			total += ecsTripleMap.get(ecs).size(); 			 			
 		}
 		//this checks out => one ecs per triple
 		System.out.println("Total Triples in map: " + total);
 		total = 0;
 		for(ExtendedCharacteristicSet ecs  : ecsLongMap.keySet()){
 			total += ecsLongMap.get(ecs).size(); 			 			
 		}*/
 		//this checks out => one ecs per triple
 		//System.out.println("Total Longs in map: " + total);
 		
 		HashMap<ExtendedCharacteristicSet, HashSet<ExtendedCharacteristicSet>> ecsLinks = new HashMap<ExtendedCharacteristicSet, HashSet<ExtendedCharacteristicSet>>(); 		
 		
 		//trit = inmem.listStatements();
 		
 		/*Triple t ; 		 		
 		
 		ExtendedCharacteristicSet subjectECS;
 		HashSet<ExtendedCharacteristicSet> objectECSSet ; 	*/
 		Map<Integer, int[]> dbLinks = db.hashMapCreate("ecsLinks")
 				.keySerializer(Serializer.INTEGER)
 				.valueSerializer(Serializer.INT_ARRAY)
 				.makeOrGet();
 		if(dbLinks.isEmpty()){
 			for(CharacteristicSet cs1 : objectCStoECS.keySet()){
 	 			if(subjectCStoECS.containsKey(cs1)){
 	 				for(ExtendedCharacteristicSet e1 : objectCStoECS.get(cs1)){
 	 					for(ExtendedCharacteristicSet e2 : subjectCStoECS.get(cs1)){
 	 						HashSet<ExtendedCharacteristicSet> d = ecsLinks.getOrDefault(e1, new HashSet<ExtendedCharacteristicSet>());
 	 						d.add(e2);
 	 						ecsLinks.put(e1, d); 						
 	 					}
 	 				}
 	 				
 	 			}
 	 		}
 			for(ExtendedCharacteristicSet ecs : ecsLinks.keySet()){
 				int[] links = new int[ecsLinks.get(ecs).size()];
 				int c = 0;
 				for(ExtendedCharacteristicSet link : ecsLinks.get(ecs)){
 					links[c++] = ecsIntegerMap.get(link); 					
 				} 					
 				dbLinks.put(ecsIntegerMap.get(ecs), links);
 			}
 		}
 		
 		for(Integer ecs : dbLinks.keySet()){
 			HashSet<ExtendedCharacteristicSet> set = new HashSet<ExtendedCharacteristicSet>();
 			for(Integer link : dbLinks.get(ecs)){
 				set.add(integerECSMap.get(link));
 			}
 			ecsLinks.put(integerECSMap.get(ecs), set);
 		}
 		
 	
 		/*for(ExtendedCharacteristicSet e1 : ecsIntegerMap.keySet()){
 			for(ExtendedCharacteristicSet e2 : ecsIntegerMap.keySet()){ 				
 				if(e1.objectCS != null && e1.objectCS.longRep == e2.subjectCS.longRep){
 					objectECSSet = ecsLinks.getOrDefault(e1, new HashSet<ExtendedCharacteristicSet>());
 					objectECSSet.add(e2);
 					ecsLinks.put(e1, objectECSSet);
 				}
 			}
 		}*/
 		/*while(trit.hasNext()){
 			t = trit.next().asTriple();
 			if(t.getPredicate().getURI().contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#_")) continue;
 			
 			subjectECS = reverseEcsTripleMap.get(t);
 			
 			if(subjectECSMap.containsKey(t.getObject())){
 				
 				objectECSSet = subjectECSMap.get(t.getObject());
 				 				 				
 				if(ecsLinks.containsKey(subjectECS)){
 					ecsLinks.get(subjectECS).addAll(objectECSSet);
 				}
 				else{
 					
 					ecsLinks.put(subjectECS, objectECSSet);
 				} 				
 			}
 			
 		}*/
 		
 		
 		//reverseEcsTripleMap = null; 		
 		
 		System.out.println("ECS Links size: " + ecsLinks.size()); 		
 		 		 		 		 		
 		System.out.println("start");
 		 		 		
 		System.out.println("starting new index");
 	
 		ecsLongArrayMap = new HashMap<ExtendedCharacteristicSet, long[]>();
 		ecsLongArrayMapOS = new HashMap<ExtendedCharacteristicSet, long[]>();
 		
 		propIndexMap = new HashMap<Integer, HashMap<Integer,Integer>>();
 		
 		subjectIndexMap = new HashMap<Integer, HashMap<Integer,HashMap<Integer,Integer>>>();
 		
 		for(Integer ecs : dbECSMap.keySet()){
				propIndexMap.put(ecs, new HashMap<Integer, Integer>());
				//subjectIndexMap.put(ecs, new HashMap<Integer, HashMap<Integer,Integer>>());	
				long[] array = dbECSMap.get(ecs);
				//System.out.println(Arrays.toString(array));
 			for(String prop : propertiesSet.keySet()){
 				int ps = propertiesSet.get(prop);
 				int pstart = indexOfProperty(array, ps);
 				if(pstart < 0)continue;
 				propIndexMap.get(ecs).put(ps, pstart);
 				//System.out.println(Arrays.toString(array));
 			}
 			
 			/*for(long t : array){
 				
 				int tripleS = (int)((t >> 27) & 0x7FFFFFF);
 				int tripleP = (int)((t >> 54)  & 0x3ff) ; 
 	 			int startingIndex = indexOfSubject(array, tripleS, propIndexMap.get(ecs).get(tripleP));
 	 			if(startingIndex < 0) continue;
 	 			if(subjectIndexMap.get(ecs).containsKey(tripleS)){
 	 				subjectIndexMap.get(ecs).get(tripleS).put(tripleP, startingIndex);
 	 			}
 	 			else{
 	 				HashMap<Integer, Integer> ds = new HashMap<Integer, Integer>();
 	 				ds.put(tripleP, startingIndex);
 	 				subjectIndexMap.get(ecs).put(tripleS, ds);
 	 			}
 			} 	 	*/	
 		}
 		/*System.out.println("subejct index map: " + subjectIndexMap.size());
 		System.out.println("prop index map: " + propIndexMap.size());*/
 		/*for(ExtendedCharacteristicSet ecs : ecsLongMap.keySet()){
 				propIndexMap.put(ecs, new HashMap<Integer, Integer>());
 				subjectIndexMap.put(ecs, new HashMap<Integer, HashMap<Integer,Integer>>());	
	 			long[] d = new long[ecsLongMap.get(ecs).size()];
	 			int i = 0;
	 			for(long next : ecsLongMap.get(ecs)){
	 				d[i++] = next;
	 			}
	 			ecsLongArrayMap.put(ecs, d);
	 			for(String prop : propertiesSet.keySet()){
	 				int ps = propertiesSet.get(prop);
	 				int pstart = indexOfProperty(ecsLongArrayMap.get(ecs), ps);
	 				if(pstart < 0)continue;
	 				propIndexMap.get(ecs).put(ps, pstart);
	 				
	 			}
	 			for(long triple : ecsLongMap.get(ecs)){
	 				int tripleS = (int)((triple >> 27) & 0x7FFFFFF);
	 				int tripleP = (int)((triple >> 54)  & 0x3ff) ; 
	 	 			int startingIndex = indexOfSubject(ecsLongArrayMap.get(ecs), tripleS, propIndexMap.get(ecs).get(tripleP));
	 	 			if(startingIndex < 0) continue;
	 	 			if(subjectIndexMap.get(ecs).containsKey(tripleS)){
	 	 				subjectIndexMap.get(ecs).get(tripleS).put(tripleP, startingIndex);
	 	 			}
	 	 			else{
	 	 				HashMap<Integer, Integer> ds = new HashMap<Integer, Integer>();
	 	 				ds.put(tripleP, startingIndex);
	 	 				subjectIndexMap.get(ecs).put(tripleS, ds);
	 	 			}
	 			} 	
	 		}*/
 		propIndexMapReverse = new HashMap<Integer, HashMap<Integer,Integer>>();
 		
 		subjectIndexMapReverse = new HashMap<Integer, HashMap<Integer,HashMap<Integer,Integer>>>();
 		
 		/*for(ExtendedCharacteristicSet ecs : ecsLongOSMap.keySet()){
 			propIndexMapReverse.put(ecs, new HashMap<Integer, Integer>());
 			subjectIndexMapReverse.put(ecs, new HashMap<Integer, HashMap<Integer,Integer>>());	
 			long[] d = new long[ecsLongOSMap.get(ecs).size()];
 			int i = 0;
 			for(long next : ecsLongOSMap.get(ecs)){
 				d[i++] = next;
 			}
 			ecsLongArrayMapOS.put(ecs, d);
 			for(String prop : propertiesSet.keySet()){
 				int ps = propertiesSet.get(prop);
 				int pstart = indexOfProperty(ecsLongArrayMapOS.get(ecs), ps);
 				if(pstart < 0)continue;
 				propIndexMapReverse.get(ecs).put(ps, pstart);
 				
 			}
 			for(long triple : ecsLongMap.get(ecs)){
 				int tripleS = (int)((triple >> 27) & 0x7FFFFFF);
 				int tripleP = (int)((triple >> 54)  & 0x3ff) ; 
 	 			int startingIndex = indexOfSubject(ecsLongArrayMapOS.get(ecs), tripleS, propIndexMapReverse.get(ecs).get(tripleP));
 	 			if(startingIndex < 0) continue;
 	 			if(subjectIndexMapReverse.get(ecs).containsKey(tripleS)){
 	 				subjectIndexMapReverse.get(ecs).get(tripleS).put(tripleP, startingIndex);
 	 			}
 	 			else{
 	 				HashMap<Integer, Integer> ds = new HashMap<Integer, Integer>();
 	 				ds.put(tripleP, startingIndex);
 	 				subjectIndexMapReverse.get(ecs).put(tripleS, ds);
 	 			}
 			} 	
 		}*/
 		/*if(dbECSMapOS.isEmpty()){
 			for(ExtendedCharacteristicSet ecs : ecsLongArrayMapOS.keySet()){ 	 			
 	 			dbECSMapOS.put(ecsIntegerMap.get(ecs), ecsLongArrayMapOS.get(ecs));
 	 			//cacheMapOS.put(ecsIntegerMap.get(ecs), ecsLongArrayMapOS.get(ecs));
 	 		}
 		}*/
 		/*if(dbECSMap.isEmpty()){
 			for(ExtendedCharacteristicSet ecs : ecsLongArrayMap.keySet()){ 	 			
 	 			dbECSMap.put(ecsIntegerMap.get(ecs), ecsLongArrayMap.get(ecs));
 	 			//cacheMap.put(ecsIntegerMap.get(ecs), ecsLongArrayMap.get(ecs));
 	 		}
 		}*/
 		
 		System.out.println("done");
 		total = 0;
 		
 		HashMap<ExtendedCharacteristicSet, HashSet<Vector<ExtendedCharacteristicSet>>> ecsVectorMap = new HashMap<ExtendedCharacteristicSet, HashSet<Vector<ExtendedCharacteristicSet>>>();
 		
 		HashSet<Vector<ExtendedCharacteristicSet>> ecsVectors = new HashSet<Vector<ExtendedCharacteristicSet>>(); 
 		
 		for(ExtendedCharacteristicSet ecs : ecsLinks.keySet()){
 			
 			HashSet<ExtendedCharacteristicSet> visited = new HashSet<ExtendedCharacteristicSet>();
 			
 			Stack<Vector<ExtendedCharacteristicSet>> stack = new Stack<>();
 			
 			Vector<ExtendedCharacteristicSet> v = new Vector<ExtendedCharacteristicSet>();
 			
 			v.add(ecs);
 			
 			stack.push(v);
 			
 			while(!stack.empty()){
 				
 				v = stack.pop();
 				
 				ExtendedCharacteristicSet current = v.lastElement();
 				
 				visited.add(current);
 				
 				if(!ecsLinks.containsKey(current)){
 				
 					if(ecsVectorMap.containsKey(current))
 						ecsVectorMap.get(current).add(v);
 					else{
 						HashSet<Vector<ExtendedCharacteristicSet>> d = new HashSet<Vector<ExtendedCharacteristicSet>>();
 						d.add(v);
 						ecsVectorMap.put(current, d); 						
 					}
 					ecsVectors.add(v);
 					continue;
 					
 				}
 				
 				for(ExtendedCharacteristicSet child : ecsLinks.get(current)){
 					if(!visited.contains(child)){
 						Vector<ExtendedCharacteristicSet> _v = new Vector<ExtendedCharacteristicSet>();
 						_v.addAll(v);
 						_v.add(child);
 						stack.push(_v);
 					}
 				}
 				
 				
 			}
 		}
 		
 		System.out.println("total patterns: " + ecsVectors.size());
 		 		 		
 		System.out.println("done");
 		
 		
 		Query q = QueryFactory.create(" SELECT DISTINCT ?x ?y ?z ?w WHERE { "
 															+ "?x <"+FOAF.knows.getURI()+"> ?y . "
 															+ "?x <"+RDF.type+"> ?x1 ."
 															+ "?y <"+FOAF.knows.getURI()+"> ?z . "
 															+ "?z <"+FOAF.accountName+"> ?y1 ."
 															+ "?z <"+prefix+"naya> ?y2."
 															+ "?z <"+FOAF.knows.getURI()+"> ?w . "
 															+ "?w <"+RDF.type+"> ?w1 ."
 															+ "?w <"+FOAF.knows.getURI()+"> ?w2 ."
 															+ "}");
 		
 		String queryString;
 		LUBMQueries lubm = new LUBMQueries();
 		for(String qs : lubm.getQueries()){
 			queryString = qs;
 			queryString = lubm.q9;
 			times = new ArrayList<Long>();
 		
 		System.out.println(queryString);
 		q=QueryFactory.create(queryString);
 		List<Var> projectVariables = q.getProjectVars();
 		long tstart = System.nanoTime();
 		int totalresults = 0;
 		long tend ;
 		 		
 		ECSQuery ecsq = new ECSQuery(q);
 		
 		ecsq.findJoins();
 		//ECSTree queryTree = ecsq.getEcsTree();
 		
 		
 		HashSet<LinkedHashSet<ExtendedCharacteristicSet>> queryListSet = ECSQuery.getListSet();
 		queryAnswerListSet2 = new HashMap<QueryPattern, HashSet<ArrayList<ECSTuple>>>();
 		for(LinkedHashSet<ExtendedCharacteristicSet> thisQueryList : queryListSet){ 			
 			  	
 				ArrayList<ExtendedCharacteristicSet> qlist = new ArrayList<>(thisQueryList);
 				
 				ArrayList<ECSTuple> qlistTuple = new ArrayList<ECSTuple>();
 				for(ExtendedCharacteristicSet qe : qlist){
 					ECSTuple nt = new ECSTuple(qe, propertiesSet.get(qe.predicate.getURI()), getQueryTriplePattern(qe));
 					
 					nt.subjectBinds = qe.subjectBinds;
 					nt.objectBinds = qe.objectBinds;
 					qlistTuple.add(nt);
 				}
 				//System.out.println(qlistTuple.toString());
 				boolean fl = false;
 				for(ExtendedCharacteristicSet ecs1 : ecsLinks.keySet()){ 			
 						
 					visited = new HashSet<ExtendedCharacteristicSet>();							
 					if(dfsOnECSLinks2(ecs1, ecsLinks, qlistTuple, qlistTuple, 
							new ArrayList<ECSTuple>())){
 						fl = true;
					}						
 				}
 				/*if(queryAnswerListSet2.size() == 0){
 					for(ExtendedCharacteristicSet ecs1 : ecsIntegerMap.keySet()){
 						visited = new HashSet<ExtendedCharacteristicSet>();							
 	 					if(dfsOnECSLinks3(ecs1, ecsLinks, qlistTuple, qlistTuple, 
 								new ArrayList<ECSTuple>())){
 	 						fl = true;
 						}
 					}
 				}*/
 		}
 		System.out.println("size of query patterns " + queryAnswerListSet2.size());
 		totalresults = 0; 		 			
 		 		 		
 		HashMap<Node, HashSet<Long>> outerBindings = new HashMap<>();
 		 		
 		varIndexMap = new HashMap<Node, Integer>();
 		int varIndex = 0;
 		
 		for(QueryPattern key : queryAnswerListSet2.keySet()){
 			for(ArrayList<ECSTuple> dataPattern : queryAnswerListSet2.get(key)){
 				for(ECSTuple ecsTuple : dataPattern){
 					TripleAsInt tai = ecsTuple.triplePattern;
 					if(!outerBindings.containsKey(reverseVarMap.get(tai.s)))
 	 					outerBindings.put(reverseVarMap.get(tai.s), new HashSet<Long>());
 						if(!outerBindings.containsKey(reverseVarMap.get(tai.o)))
 							outerBindings.put(reverseVarMap.get(tai.o), new HashSet<Long>());
 	 				if(!varIndexMap.containsKey(reverseVarMap.get(tai.s)))
 	 					varIndexMap.put(reverseVarMap.get(tai.s), varIndex++);
 	 				if(!varIndexMap.containsKey(reverseVarMap.get(tai.o)))
 	 					varIndexMap.put(reverseVarMap.get(tai.o), varIndex++);
 				}
 			} 			
 		}
 		qpVarMap = new HashMap<ArrayList<ExtendedCharacteristicSet>, HashSet<Integer>>();
 		qpVarMap2 = new HashMap<ArrayList<ECSTuple>, HashSet<Integer>>();
 		//HashMap<ArrayList<ExtendedCharacteristicSet>, HashSet<ArrayList<ExtendedCharacteristicSet>>> skipping = new HashMap<ArrayList<ExtendedCharacteristicSet>, HashSet<ArrayList<ExtendedCharacteristicSet>>>();
 		HashMap<QueryPattern, HashSet<ArrayList<ExtendedCharacteristicSet>>> skipping = new HashMap<QueryPattern, HashSet<ArrayList<ExtendedCharacteristicSet>>>();
 		HashMap<QueryPattern, HashSet<ArrayList<ECSTuple>>> skipping2 = new HashMap<QueryPattern, HashSet<ArrayList<ECSTuple>>>();
 		for(QueryPattern queryPattern : queryAnswerListSet2.keySet()){
 			int count = 0;
 			
 			skipping.put(queryPattern, new HashSet<ArrayList<ExtendedCharacteristicSet>>());
 			skipping2.put(queryPattern, new HashSet<ArrayList<ECSTuple>>());
 			for(ArrayList<ECSTuple> dataPattern : queryAnswerListSet2.get(queryPattern)){
 				if(dataPattern.size() > 1){
 					//System.out.println("data pattern: " + dataPattern.toString());
 	 				boolean cont = false;
 	 				ArrayList<ExtendedCharacteristicSet> dataPatECS = new ArrayList<ExtendedCharacteristicSet>();
 	 				for(ECSTuple et : dataPattern)
 	 					dataPatECS.add(et.ecs);
 	 				for(Vector<ExtendedCharacteristicSet> vector : ecsVectors){
 	 					if(vector.containsAll(dataPatECS)){
 	 						cont = true;
 	 						break;
 	 					}
 	 				}
 	 				if(!cont){
 	 					count++;
 	 					skipping.get(queryPattern).add(dataPatECS);
 	 					skipping2.get(queryPattern).add(dataPattern);
 	 				}
 				}
 				 					 				
 				HashSet<Integer> vars = new HashSet<Integer>();
 				for(ECSTuple et : dataPattern){
 					TripleAsInt tai = et.triplePattern;
 					//if(reverseVarMap.get(tai.s).isVariable()){
 					if(tai.s < 0){
 						vars.add(varIndexMap.get(reverseVarMap.get(tai.s)));
 						}
 					if(tai.o < 0){
 						vars.add(varIndexMap.get(reverseVarMap.get(tai.o)));
 						}
 				}
 				
 				qpVarMap2.put(dataPattern, vars);
 				qpVarMap2.put(queryPattern.queryPattern, vars);
 				
 			}
 			
 			System.out.println("not contains all: " + count);
 			System.out.println("from total: " + queryAnswerListSet2.get(queryPattern).size());
 			
 		}
	
 		
 		totalresults = 0;
 		//ConcurrentHashMap<int[], Boolean> outerPathProbe = null;
 		//HashMap<int[], Boolean> outerPathProbe = null;
 		List<int[]> outerPathProbe = null;
 		//ExecutorService executorService = Executors.newFixedThreadPool(2);
 		for(int i = 0; i < 10; i++){
 		
 		

/* 		 Thread th= new InMemoryTests4().new Exec(projectVariables, skipping);
 	      Runtime.getRuntime().addShutdownHook(th);*/
 		
 		//outerPathProbe = new ConcurrentHashMap<int[], Boolean>();
 			//outerPathProbe = new HashMap<int[], Boolean>();
 			outerPathProbe = new ArrayList<int[]>();
 		
 		//ArrayList<ExtendedCharacteristicSet> previousQueryPattern = null;
 		QueryPattern previousQueryPattern = null;
 		
 		ArrayList<QueryPattern> f = new ArrayList<>(queryAnswerListSet2.keySet());
 		
 		Collections.sort(f, new ECSTupleComparator());
 		
 		HashMap<QueryPattern, HashMap<ECSTuple, AnswerPattern>> qans = new HashMap<QueryPattern, HashMap<ECSTuple,AnswerPattern>>();
 		
 		HashMap<QueryPattern, HashMap<ECSTuple, AnswerPattern>> qansReverse = new HashMap<QueryPattern, HashMap<ECSTuple,AnswerPattern>>();
 		
 		for(QueryPattern queryPattern : f){
 			
 			qans.put(queryPattern, new HashMap<ECSTuple, AnswerPattern>());
 			
 			qansReverse.put(queryPattern, new HashMap<ECSTuple, AnswerPattern>());
 			
 			if(queryPattern.queryPattern.size() == 1 )
 				{
 				
 				for(ArrayList<ECSTuple> dataPattern1 : queryAnswerListSet2.get(queryPattern)){
 					AnswerPattern ans = new AnswerPattern(dataPattern1.get(0), queryPattern);
 					if(qans.get(queryPattern).containsKey(ans.root)){
 						ans = qans.get(queryPattern).get(ans.root); 						
 					} 
 					qans.get(queryPattern).put(ans.root, ans);
 					qansReverse.get(queryPattern).put(ans.root, ans); 		
 				
 				}
 				/*for(ECSTuple tup : qans.get(queryPattern).keySet()){
 	 				System.out.println("ecs tuple: " + tup.ecs.toString());
 	 				System.out.println("answer set: " + qans.get(queryPattern).get(tup).children.toString());
 	 			}*/
 				continue;
 			}
 			 			
 			
 			for(ArrayList<ECSTuple> dataPattern1 : queryAnswerListSet2.get(queryPattern)){
 			 				 				
 				for(int i1 = 0 ; i1 < dataPattern1.size()-1; i1++){
 					
 					AnswerPattern ans = new AnswerPattern(dataPattern1.get(i1), queryPattern);
 					if(qans.get(queryPattern).containsKey(ans.root)){
 						ans = qans.get(queryPattern).get(ans.root);
 					} 					
 					AnswerPattern nextAns = new AnswerPattern(dataPattern1.get(i1+1), queryPattern);
 	 				if(qans.get(queryPattern).containsKey(nextAns.root)){
 	 					nextAns = qans.get(queryPattern).get(nextAns.root);
 	 				}
 	 				ans.addChild(nextAns); 					
 					
 					qans.get(queryPattern).put(ans.root, ans); 	
 					//qans.get(queryPattern).put(nextAns.root, nextAns);
 					
 				}
 				for(int i1 = dataPattern1.size()-1 ; i1 >= 1; i1--){
 					
 					AnswerPattern ans = new AnswerPattern(dataPattern1.get(i1), queryPattern);
 					if(qansReverse.get(queryPattern).containsKey(ans.root)){
 						ans = qansReverse.get(queryPattern).get(ans.root);
 					} 					
 					AnswerPattern nextAns = new AnswerPattern(dataPattern1.get(i1-1), queryPattern);
 	 				if(qansReverse.get(queryPattern).containsKey(nextAns.root)){
 	 					nextAns = qansReverse.get(queryPattern).get(nextAns.root);
 	 				}
 	 				ans.addChild(nextAns); 					
 					
 	 				qansReverse.get(queryPattern).put(ans.root, ans); 	
 					//qans.get(queryPattern).put(nextAns.root, nextAns);
 					
 				}
 			
 			}
 			
 			/*for(ECSTuple tup : qans.get(queryPattern).keySet()){
 				System.out.println("ecs tuple: " + tup.ecs.toString());
 				System.out.println("answer set: " + qans.get(queryPattern).get(tup).children.toString());
 			}*/
 		}
 		//for(ArrayList<ExtendedCharacteristicSet> queryPatterns : f){	
 		
 	/*	for(QueryPattern queryPatterns : f){
 			ConcurrentHashMap<int[], Boolean> paths = new ConcurrentHashMap<int[], Boolean>();
 			HashMap<Integer, HashSet<Integer>> singleBindings = new HashMap<Integer, HashSet<Integer>>();
 			
 			for(ECSTuple rootTuple : qans.get(queryPatterns).keySet()){
 				
 			} 			

 		}*/
 		HashMap<QueryPattern, HashSet<QueryPattern>> joinedQueryPatterns = new HashMap<QueryPattern, HashSet<QueryPattern>>();
 		HashMap<QueryPattern, HashSet<ArrayList<Integer>>> commonVarsMap = new HashMap<QueryPattern, HashSet<ArrayList<Integer>>>();
 		for(QueryPattern queryPatterns1 : f){
 			commonVarsMap.put(queryPatterns1, new HashSet<ArrayList<Integer>>());
 			joinedQueryPatterns.put(queryPatterns1, new HashSet<QueryPattern>());
 		}
 		for(QueryPattern queryPatterns1 : f){
 			for(QueryPattern queryPatterns2 : f){
 				if(queryPatterns1 == queryPatterns2) continue;
 				
 				HashSet<Integer> vars = qpVarMap2.get(queryPatterns1.queryPattern);
 	 			HashSet<Integer> previousVars = qpVarMap2.get(queryPatterns2.queryPattern); 			
 	 			ArrayList<Integer> commonVars = new ArrayList<Integer>(); 			
 	 			for(Integer var : vars){
 					if(previousVars.contains(var)){
 						commonVars.add(var);
 					} 					
 	 			}
 	 			if(commonVars.isEmpty()) continue;
 	 			commonVarsMap.get(queryPatterns1).add(commonVars);
 	 			commonVarsMap.get(queryPatterns2).add(commonVars);
 	 			joinedQueryPatterns.get(queryPatterns1).add(queryPatterns2);
 	 			joinedQueryPatterns.get(queryPatterns2).add(queryPatterns1);
 			}
 			
 		}
 		
 		sskilllist = new HashSet<CharacteristicSet>();
 		ookilllist = new HashSet<CharacteristicSet>();
 		sslist = new HashMap<Integer, HashSet<ECSTuple>>();
 		oolist = new HashMap<Integer, HashSet<ECSTuple>>();
 		for(QueryPattern qq : joinedQueryPatterns.keySet()){
 			//System.out.println("for q: " + qq.toString());
 			//System.out.println("\t"+joinedQueryPatterns.get(qq).toString());
 			int outCount = 0;
 			for(ECSTuple outside : qq.queryPattern){
 				for(QueryPattern qqinner : joinedQueryPatterns.get(qq)){
 					
 					if(qq == qqinner) continue;
 					int inCount = 0;
 					for(ECSTuple inside : qqinner.queryPattern){
 						//System.out.println("\tnext: "+outside.toString()+", "+inside.toString());
 						if(outside.triplePattern.s == inside.triplePattern.s){
 							//System.out.println("subject-subject join ("+outCount+","+inCount+")");
 							//sslist.add(inside);
 							//sslist.add(outside);
 							HashSet<CharacteristicSet> joinmap = new HashSet<CharacteristicSet>();
 							for(ArrayList<ECSTuple> outsideDP : queryAnswerListSet2.get(qq)){
 								//sslist.add(outsideDP.get(outCount));
 								
 								if(sslist.containsKey(outside.triplePattern.s))
 									sslist.get(outside.triplePattern.s).add(outsideDP.get(outCount));
 								else{
 									HashSet<ECSTuple> d = new HashSet<ECSTuple>();
 									d.add(outsideDP.get(outCount));
 									sslist.put(outside.triplePattern.s, d);
 								}
 								joinmap.add(outsideDP.get(outCount).ecs.subjectCS); 								
 							}
 							for(ArrayList<ECSTuple> insideDP : queryAnswerListSet2.get(qqinner)){
 								if(sslist.containsKey(inside.triplePattern.s))
 									sslist.get(inside.triplePattern.s).add(insideDP.get(inCount));
 								else{
 									HashSet<ECSTuple> d = new HashSet<ECSTuple>();
 									d.add(insideDP.get(inCount));
 									sslist.put(inside.triplePattern.s, d);
 								}
	 								if(joinmap.contains(insideDP.get(inCount).ecs.subjectCS)){
	 									//System.out.println("contains this (ss)");
	 									sskilllist.add(insideDP.get(inCount).ecs.subjectCS);
	 									//System.out.println("added subject " + insideDP.get(inCount).toString());
	 									//killlist.add(outsideDP.get(outCount).ecs.subjectCS);
	 								}
	 						}
 							
 						}
 						if(outside.triplePattern.o == inside.triplePattern.o){
 							//System.out.println("object-object join ("+outCount+","+inCount+")");
 							//oolist.add(inside);
 							//oolist.add(outside);
 							HashSet<CharacteristicSet> joinmap = new HashSet<CharacteristicSet>();
 							for(ArrayList<ECSTuple> outsideDP : queryAnswerListSet2.get(qq)){
 								//System.out.println("outside DP : " + outsideDP.toString());
 								//oolist.add(outsideDP.get(outCount));
 								if(oolist.containsKey(outside.triplePattern.o))
 									oolist.get(outside.triplePattern.o).add(outsideDP.get(outCount));
 								else{
 									HashSet<ECSTuple> d = new HashSet<ECSTuple>();
 									d.add(outsideDP.get(outCount));
 									oolist.put(outside.triplePattern.o, d);
 								}
 								//System.out.println("outer adding " + outsideDP.get(outCount).toString()+" to oolist");
 								joinmap.add(outsideDP.get(outCount).ecs.objectCS);
 							}
 							for(ArrayList<ECSTuple> insideDP : queryAnswerListSet2.get(qqinner)){
 								//System.out.println("inside DP : " + insideDP.toString());
 								if(oolist.containsKey(inside.triplePattern.o))
 									oolist.get(inside.triplePattern.o).add(insideDP.get(inCount));
 								else{
 									HashSet<ECSTuple> d = new HashSet<ECSTuple>();
 									d.add(insideDP.get(inCount));
 									oolist.put(inside.triplePattern.o, d);
 								}
 								//System.out.println("inner adding " + insideDP.get(inCount).toString()+" to oolist");
	 							if(joinmap.contains(insideDP.get(inCount).ecs.objectCS)){
	 								//System.out.println("contains this (oo)");
	 								ookilllist.add(insideDP.get(inCount).ecs.objectCS);
	 								//System.out.println("added object " + insideDP.get(inCount).toString());
	 							}
	 						}
 						}
 						/*if(outside.triplePattern.o == inside.triplePattern.s)
 							System.out.println("object-subject join");
 						if(outside.triplePattern.s == inside.triplePattern.o)
 							System.out.println("subject-object join");*/
 						inCount++;
 					}
 				}
 				outCount++;
 			} 	
 			break;
 		}

 		 
 		
 		HashMap<BigInteger, int[]> binder = new HashMap<BigInteger, int[]>(10000);
 		
 		boolean addOrJoin = false;
 		
 		HashMap<Integer, HashSet<Integer>> outBindings = new HashMap<Integer, HashSet<Integer>>();
 		
 		totalresults = 0;
 		
 		HashMap<Long, Vector<Integer>> previous_res_vectors = null;
 		
 		tstart = System.nanoTime();
 		
 		for(int qi = 0; qi < f.size(); qi++){
 			
 			QueryPattern qp = f.get(qi);
 			int tot = 0;
 			HashMap<Long, Vector<Integer>> res_vectors = new HashMap<Long, Vector<Integer>>();	
 			for(ArrayList<ECSTuple> next : queryAnswerListSet2.get(qp)){ 				 				
 				//HashSet<Vector<Integer>> res = new HashSet<Vector<Integer>>();	
 				if(next.size() == 1){ 					
 					joinTwoECS(res_vectors, previous_res_vectors, next.get(0), null);
 				}
 				else
 				for(int ind = 0; ind < next.size()-1; ind++){
 					
 					joinTwoECS(res_vectors, previous_res_vectors, next.get(ind),next.get(ind+1));
 					
 				}
 				//res_vectors.addAll(res);
 				
 					
 			}
 			//System.out.println(qi+". tot: " + res.size());
 			
 		/*	if(qi == 1){
 				
 				HashMap<Long, Vector<Integer>> h = new HashMap<Long, Vector<Integer>>(); 		
 				
 				for(Vector<Integer> nextVector : previous_res_vectors){
 										
 					h.put(szudzik(nextVector.firstElement(), nextVector.lastElement()), nextVector);
 					
 				}

 				for(Vector<Integer> nextVector : res_vectors){
 					if(h.containsKey(szudzik(nextVector.firstElement(), nextVector.lastElement())))
						tot++;
 				}
 				
 			}*/
 			//previous_res = res;
 			previous_res_vectors = res_vectors;
 			//System.out.println(qi+". tot: " + tot);
 		}
 		tend = System.nanoTime();
 		
 		System.out.println("join " + previous_res_vectors.size() + "\t: " + (tend-tstart)); 		
 		times.add((tend-tstart));
 		/*tstart = System.nanoTime();
 		for(int qi = 0; qi < f.size(); qi++){
 			
 			QueryPattern queryPatterns = f.get(qi);
 			
 			//HashMap<int[], Boolean> paths = new HashMap<int[], Boolean>(100);
 			List<int[]> paths = new ArrayList<int[]>();
 			HashMap<Integer, HashSet<Integer>> singleBindings = new HashMap<Integer, HashSet<Integer>>();
 			
 			ArrayList<Integer> commonJoinVariables = new ArrayList<>();
 			ArrayList<Integer> commonHashVariables = new ArrayList<>();
 			if(qi < f.size()-1){
 				HashSet<Integer> vars = qpVarMap2.get(queryPatterns.queryPattern);
	 			HashSet<Integer> nextVars = qpVarMap2.get(f.get(qi+1).queryPattern);	 				 		
	 			for(Integer var : vars){
					if(nextVars.contains(var)){
						commonHashVariables.add(var);
					}					
	 			}
 			}
 			if(previousQueryPattern != null){
	 			HashSet<Integer> vars = qpVarMap2.get(queryPatterns.queryPattern);
	 			HashSet<Integer> previousVars = qpVarMap2.get(previousQueryPattern.queryPattern);	 				 		
	 			for(Integer var : vars){
					if(previousVars.contains(var)){
						commonJoinVariables.add(var);
					}					
	 			}
 			}
 			for(ECSTuple rootTuple : qans.get(queryPatterns).keySet()){
 				//System.out.println("root tuple: " + rootTuple.toString());
 				//System.out.println("root tuple: " + rootTuple.subjectBinds);
 				if(outerPathProbe.isEmpty())
 					oTheosVoithos4(rootTuple, 
						paths, commonHashVariables, projectVariables.size(), 
						outerPathProbe, singleBindings, outBindings,
						qans.get(queryPatterns),
						queryPatterns,commonVarsMap, binder, addOrJoin, joinedQueryPatterns);
 				else
 					oTheosVoithos5(rootTuple, 
						paths, commonJoinVariables, projectVariables.size(), 
						outerPathProbe, singleBindings, outBindings,
						qans.get(queryPatterns),
						queryPatterns,commonVarsMap, binder, addOrJoin, joinedQueryPatterns);
 			}
 			//System.out.println("ttt2: " + ttt2);
 			System.out.println("path size: " + paths.size());
 			for(Integer in : singleBindings.keySet()){
 				if(outBindings.containsKey(in))
 					outBindings.get(in).addAll(singleBindings.get(in));
 				else{
 					outBindings.put(in,singleBindings.get(in));
 					
 				}
 			}
 			outerPathProbe = paths;	 		
 			previousQueryPattern = queryPatterns;
 			
 		}

 		tend = System.nanoTime();
 		
 		System.out.println("yoga " + outerPathProbe.size() + "\t: " + (tend-tstart)); 		
 		times.add((tend-tstart));
 		
 		//reverse now
 		outBindings = new HashMap<Integer, HashSet<Integer>>();
 		//outerPathProbe = new HashMap<int[], Boolean>();
 		outerPathProbe = new ArrayList<int[]>();
 		binder = new HashMap<BigInteger, int[]>(10000);
 		tstart = System.nanoTime();
 		
 		for(int qi = 0; qi < f.size(); qi++){
 			
 			QueryPattern queryPatterns = f.get(qi);
 			
 			//HashMap<int[], Boolean> paths = new HashMap<int[], Boolean>(100);
 			List<int[]> paths = new ArrayList<int[]>();
 			
 			HashMap<Integer, HashSet<Integer>> singleBindings = new HashMap<Integer, HashSet<Integer>>();
 			
 			ArrayList<Integer> commonJoinVariables = new ArrayList<>();
 			ArrayList<Integer> commonHashVariables = new ArrayList<>();
 			if(qi < f.size()-1){
 				HashSet<Integer> vars = qpVarMap2.get(queryPatterns.queryPattern);
	 			HashSet<Integer> nextVars = qpVarMap2.get(f.get(qi+1).queryPattern);	 				 		
	 			for(Integer var : vars){
					if(nextVars.contains(var)){
						commonHashVariables.add(var);
					}					
	 			}
 			}
 			if(previousQueryPattern != null){
	 			HashSet<Integer> vars = qpVarMap2.get(queryPatterns.queryPattern);
	 			HashSet<Integer> previousVars = qpVarMap2.get(previousQueryPattern.queryPattern);	 				 		
	 			for(Integer var : vars){
					if(previousVars.contains(var)){
						commonJoinVariables.add(var);
					}					
	 			}
 			}
 			for(ECSTuple rootTuple : qansReverse.get(queryPatterns).keySet()){
 				//System.out.println("root tuple: " + rootTuple.toString());
 				//System.out.println("root tuple: " + rootTuple.subjectBinds);
 				if(outerPathProbe.isEmpty())
 					oTheosVoithos4(rootTuple, 
						paths, commonHashVariables, projectVariables.size(), 
						outerPathProbe, singleBindings, outBindings,
						qansReverse.get(queryPatterns),
						queryPatterns,commonVarsMap, binder, addOrJoin, joinedQueryPatterns, true);
 				else
 					oTheosVoithos5(rootTuple, 
						paths, commonJoinVariables, projectVariables.size(), 
						outerPathProbe, singleBindings, outBindings,
						qansReverse.get(queryPatterns),
						queryPatterns,commonVarsMap, binder, addOrJoin, joinedQueryPatterns, true);
 			}
 			//System.out.println("ttt2: " + ttt2);
 			for(Integer in : singleBindings.keySet()){
 				if(outBindings.containsKey(in))
 					outBindings.get(in).addAll(singleBindings.get(in));
 				else{
 					outBindings.put(in,singleBindings.get(in));
 					
 				}
 			} 		
 			 
 			outerPathProbe = paths;
 			previousQueryPattern = queryPatterns;
 			//iteration++;
 		}

 		tend = System.nanoTime();
 		
 		System.out.println("reverse " + outerPathProbe.size() + "\t: " + (tend-tstart)); 	
 		times.add((tend-tstart));*/
	}
 		/*for(int[] result : outerPathProbe.keySet()){
 			
 			for(int i = 0; i < result.length; i++){
 				System.out.print(reverseIntMap.get(result[i])+", ");
			}
 			System.out.println();
 			
 		}*/
 		Collections.sort(times);
 		
 		System.out.println("best time: " + times.get(0));
 		}
		inmem.close();
		//executorService.shutdown();
		db.close();
	}
		
	
	
	
	static public HashSet<LinkedHashSet<ExtendedCharacteristicSet>> queryAnswerListSet = new HashSet<LinkedHashSet<ExtendedCharacteristicSet>>();
	static public HashMap<ArrayList<ExtendedCharacteristicSet>, ArrayList<Integer>> pAnswerListSet = new HashMap<>();
	static public HashMap<ArrayList<ExtendedCharacteristicSet>, ArrayList<TripleAsInt>> tAnswerListSet = new HashMap<>();
	static public HashMap<ArrayList<ExtendedCharacteristicSet>, ArrayList<TripleAsInt>> tqAnswerListSet = new HashMap<>();
	static public HashMap<Node, Integer> varMap = new HashMap<Node, Integer>();
	static public HashMap<Integer, Node> reverseVarMap = new HashMap<Integer, Node>();
	static public HashMap<ArrayList<ExtendedCharacteristicSet>, ArrayList<ExtendedCharacteristicSet>> qdMap = new HashMap<ArrayList<ExtendedCharacteristicSet>, ArrayList<ExtendedCharacteristicSet>>();
	static public HashMap<ArrayList<ExtendedCharacteristicSet>, HashSet<ArrayList<ExtendedCharacteristicSet>>> reverseQdMap = new HashMap<ArrayList<ExtendedCharacteristicSet>, HashSet<ArrayList<ExtendedCharacteristicSet>>>();
	static public int nextVar = -1;
	
	static public boolean dfsOnECSLinks(ExtendedCharacteristicSet ecs, 
			HashMap<ExtendedCharacteristicSet, HashSet<ExtendedCharacteristicSet>> links, 
			ArrayList<ExtendedCharacteristicSet> queryLinks, 
			ArrayList<ExtendedCharacteristicSet> originalQueryLinks, 
			LinkedHashSet<ExtendedCharacteristicSet> list, 
			ArrayList<Integer> plist, ArrayList<TripleAsInt> tlist){
		
		//System.out.println("current ECS: " + ecs.properties.toString());
		if(queryLinks.size() == 0) {
			
			queryAnswerListSet.add(list);			
			pAnswerListSet.put(new ArrayList<>(list), plist);
			tAnswerListSet.put(new ArrayList<>(list), tlist);
			tqAnswerListSet.put(originalQueryLinks, tlist);
			qdMap.put(new ArrayList<>(list), originalQueryLinks);
			if(reverseQdMap.containsKey(originalQueryLinks))
				reverseQdMap.get(originalQueryLinks).add(new ArrayList<>(list));
			else{
				HashSet<ArrayList<ExtendedCharacteristicSet>> dummy = new HashSet<ArrayList<ExtendedCharacteristicSet>>();
				dummy.add(new ArrayList<>(list));
				reverseQdMap.put(originalQueryLinks, dummy);
			}
			return true;
		}
		if((queryLinks.get(0).subjectCS.longRep & ecs.subjectCS.longRep) != queryLinks.get(0).subjectCS.longRep){
			
			return false;
		}
		if(queryLinks.get(0).objectCS != null && ecs.objectCS != null)
			if((queryLinks.get(0).objectCS.longRep & ecs.objectCS.longRep) != queryLinks.get(0).objectCS.longRep){
				
				return false;
			}
		if(queryLinks.get(0).objectCS == null && ecs.objectCS != null) return false;
		if(queryLinks.get(0).objectCS != null && ecs.objectCS == null) return false;
		if(visited.contains(ecs)){
			queryAnswerListSet.add(list);		
			pAnswerListSet.put(new ArrayList<>(list), plist);
			tAnswerListSet.put(new ArrayList<>(list), tlist);
			tqAnswerListSet.put(originalQueryLinks, tlist);
			qdMap.put(new ArrayList<>(list), originalQueryLinks);
			if(reverseQdMap.containsKey(originalQueryLinks))
				reverseQdMap.get(originalQueryLinks).add(new ArrayList<>(list));
			else{
				HashSet<ArrayList<ExtendedCharacteristicSet>> dummy = new HashSet<ArrayList<ExtendedCharacteristicSet>>();
				dummy.add(new ArrayList<>(list));
				reverseQdMap.put(originalQueryLinks, dummy);
			}
			return true;
		}
		visited.add(ecs);
		list.add(ecs);
		plist.add(propertiesSet.get(queryLinks.get(0).predicate.getURI()));
		//System.out.println("prop: " + queryLinks.get(0).predicate.getURI());
		//System.out.println("int: " + propertiesSet.get(queryLinks.get(0).predicate.getURI()));
		
		int s = -1, p = -1, o = -1;
		if(!queryLinks.get(0).subject.isVariable()){
			if(intMap.containsKey(queryLinks.get(0).subject))
				s = intMap.get(queryLinks.get(0).subject);			
		}
		else{
			if(!varMap.containsKey(queryLinks.get(0).subject)){
				reverseVarMap.put(nextVar, queryLinks.get(0).subject);
				varMap.put(queryLinks.get(0).subject, nextVar--);
			}
			s = varMap.get(queryLinks.get(0).subject);
		}
		if(!queryLinks.get(0).predicate.isVariable()){
			if(intMap.containsKey(queryLinks.get(0).predicate))
				p = propertiesSet.get(queryLinks.get(0).predicate.getURI());			
		}
		else{
			if(!varMap.containsKey(queryLinks.get(0).predicate)){
				reverseVarMap.put(nextVar, queryLinks.get(0).predicate);
				varMap.put(queryLinks.get(0).predicate, nextVar--);
			}
			p = varMap.get(queryLinks.get(0).predicate);
		}
		if(!queryLinks.get(0).object.isVariable()){
			if(intMap.containsKey(queryLinks.get(0).object))
				o = intMap.get(queryLinks.get(0).object);			
		}
		else{
			if(!varMap.containsKey(queryLinks.get(0).object)){
				reverseVarMap.put(nextVar, queryLinks.get(0).object);
				varMap.put(queryLinks.get(0).object, nextVar--);
			}
			o = varMap.get(queryLinks.get(0).object);
		}
		
		
		tlist.add(new TripleAsInt(s, p, o));
		
		if(!links.containsKey(ecs)){
				
			queryAnswerListSet.add(list);			
			pAnswerListSet.put(new ArrayList<>(list), plist);
			tAnswerListSet.put(new ArrayList<>(list), tlist);
			tqAnswerListSet.put(originalQueryLinks, tlist);
			qdMap.put(new ArrayList<>(list), originalQueryLinks);
			if(reverseQdMap.containsKey(originalQueryLinks))
				reverseQdMap.get(originalQueryLinks).add(new ArrayList<>(list));
			else{
				HashSet<ArrayList<ExtendedCharacteristicSet>> dummy = new HashSet<ArrayList<ExtendedCharacteristicSet>>();
				dummy.add(new ArrayList<>(list));
				reverseQdMap.put(originalQueryLinks, dummy);
			}
			return true;
		}
		else{
			//System.out.println("children: " + links.get(ecs).size());
			for(ExtendedCharacteristicSet child : links.get(ecs)){
				//if(!visited.contains(child)){
				
					if(queryLinks.size()>1){
						//System.out.println("size: " + queryLinks.size());
						LinkedHashSet<ExtendedCharacteristicSet> dummy = new LinkedHashSet<ExtendedCharacteristicSet>();
						dummy.addAll(list);
						ArrayList<Integer> pdummy = new ArrayList<Integer>();
						pdummy.addAll(plist);
						ArrayList<TripleAsInt> tdummy = new ArrayList<TripleAsInt>();
						tdummy.addAll(tlist);
						dfsOnECSLinks(child, links, new ArrayList<>(queryLinks.subList(1, queryLinks.size())), originalQueryLinks, dummy, pdummy, tdummy);
					}
					else {
						queryAnswerListSet.add(list);						
						pAnswerListSet.put(new ArrayList<>(list), plist);
						tAnswerListSet.put(new ArrayList<>(list), tlist);
						tqAnswerListSet.put(originalQueryLinks, tlist);
						qdMap.put(new ArrayList<>(list), originalQueryLinks);
						if(reverseQdMap.containsKey(originalQueryLinks))
							reverseQdMap.get(originalQueryLinks).add(new ArrayList<>(list));
						else{
							HashSet<ArrayList<ExtendedCharacteristicSet>> dummy = new HashSet<ArrayList<ExtendedCharacteristicSet>>();
							dummy.add(new ArrayList<>(list));
							reverseQdMap.put(originalQueryLinks, dummy);
						}
						//return true;
						}
				//}
			}
		}
		
		return false;
									
	}
	
	public static HashMap<QueryPattern, HashSet<ArrayList<ECSTuple>>> queryAnswerListSet2 ;
	
	static public boolean dfsOnECSLinks2(ExtendedCharacteristicSet ecs, 
			HashMap<ExtendedCharacteristicSet, HashSet<ExtendedCharacteristicSet>> links, 
			ArrayList<ECSTuple> queryLinks, 
			ArrayList<ECSTuple> originalQueryLinks, 
			ArrayList<ECSTuple> list){
				
		//System.out.println("ecs: " + ecs.toString());
		if(queryLinks.size() == 0) {
			if(queryAnswerListSet2.containsKey(new QueryPattern(originalQueryLinks)))
				queryAnswerListSet2.get(new QueryPattern(originalQueryLinks)).add(list);			
			else{
				HashSet<ArrayList<ECSTuple>> d = new HashSet<ArrayList<ECSTuple>>();
				d.add(list);
				queryAnswerListSet2.put(new QueryPattern(originalQueryLinks),d);
			}
			return true;
		}
		/*if(ecs.subjectCS == null);
		if(ecs.subjectCS.longRep == null);
		if(queryLinks.get(0) == null);
		if(queryLinks.get(0).ecs == null);
		if(queryLinks.get(0).ecs.subjectCS == null);
		if(queryLinks.get(0).ecs.subjectCS.longRep == null);*/
		
		if((queryLinks.get(0).ecs.subjectCS.longRep & ecs.subjectCS.longRep) != queryLinks.get(0).ecs.subjectCS.longRep){
			
			return false;
		}
		if(queryLinks.get(0).ecs.objectCS != null && ecs.objectCS != null)
			if((queryLinks.get(0).ecs.objectCS.longRep & ecs.objectCS.longRep) != queryLinks.get(0).ecs.objectCS.longRep){
				
				return false;
			}
		if(queryLinks.get(0).ecs.objectCS == null && ecs.objectCS != null) return false;
		if(queryLinks.get(0).ecs.objectCS != null && ecs.objectCS == null) return false;
		//int ind = indexOfProperty(dbECSMap.get(ecsIntegerMap.get(ecs)), propertiesSet.get(queryLinks.get(0).ecs.predicate.getURI()));
		
		
		if(!propIndexMap.get(ecsIntegerMap.get(ecs)).containsKey(propertiesSet.get(queryLinks.get(0).ecs.predicate.getURI())))
			return false;
		int ind = propIndexMap.get(ecsIntegerMap.get(ecs)).get(propertiesSet.get(queryLinks.get(0).ecs.predicate.getURI()));
		//if(ind < 0) return false;
		if(visited.contains(ecs)){
			if(queryAnswerListSet2.containsKey(new QueryPattern(originalQueryLinks)))
				queryAnswerListSet2.get(new QueryPattern(originalQueryLinks)).add(list);			
			else{
				HashSet<ArrayList<ECSTuple>> d = new HashSet<ArrayList<ECSTuple>>();
				d.add(list);
				queryAnswerListSet2.put(new QueryPattern(originalQueryLinks),d);
			}			
		}
		visited.add(ecs);
		
		ECSTuple ecsTuple = new ECSTuple(ecs, propertiesSet.get(queryLinks.get(0).ecs.predicate.getURI()), getQueryTriplePattern(queryLinks.get(0).ecs));
		ecsTuple.subjectBinds = queryLinks.get(0).ecs.subjectBinds;
		ecsTuple.objectBinds = queryLinks.get(0).ecs.objectBinds;
		if(!ecsTupleIntegerMap.containsKey(ecsTuple)){
			reverseECSTupleIntegerMap.put(nextECSTuple, ecsTuple);
			ecsTupleIntegerMap.put(ecsTuple, nextECSTuple++);
			
		}
		list.add(ecsTuple);
		
		if(!links.containsKey(ecs)){
				
			if(queryAnswerListSet2.containsKey(new QueryPattern(originalQueryLinks)))
				queryAnswerListSet2.get(new QueryPattern(originalQueryLinks)).add(list);			
			else{
				HashSet<ArrayList<ECSTuple>> d = new HashSet<ArrayList<ECSTuple>>();
				d.add(list);
				queryAnswerListSet2.put(new QueryPattern(originalQueryLinks),d);
			}		
			
			return true;
		}
		else{			
			for(ExtendedCharacteristicSet child : links.get(ecs)){				
				
					if(queryLinks.size()>1){
						//System.out.println("size: " + queryLinks.size());
						ArrayList<ECSTuple> dummy = new ArrayList<ECSTuple>();
						dummy.addAll(list);
						
						dfsOnECSLinks2(child, links, new ArrayList<>(queryLinks.subList(1, queryLinks.size())), originalQueryLinks, dummy);
					}
					else {
						if(queryAnswerListSet2.containsKey(new QueryPattern(originalQueryLinks)))
							queryAnswerListSet2.get(new QueryPattern(originalQueryLinks)).add(list);			
						else{
							HashSet<ArrayList<ECSTuple>> d = new HashSet<ArrayList<ECSTuple>>();
							d.add(list);
							queryAnswerListSet2.put(new QueryPattern(originalQueryLinks),d);
						}					
						
					}			
			}
		}
		
		return false;
									
	}
	
	static public boolean dfsOnECSLinks3(ExtendedCharacteristicSet ecs, 
			HashMap<ExtendedCharacteristicSet, HashSet<ExtendedCharacteristicSet>> links, 
			ArrayList<ECSTuple> queryLinks, 
			ArrayList<ECSTuple> originalQueryLinks, 
			ArrayList<ECSTuple> list){
				
		//System.out.println("ecs: " + ecs.toString());
		if(queryLinks.size() == 0) {
			if(queryAnswerListSet2.containsKey(new QueryPattern(originalQueryLinks)))
				queryAnswerListSet2.get(new QueryPattern(originalQueryLinks)).add(list);			
			else{
				HashSet<ArrayList<ECSTuple>> d = new HashSet<ArrayList<ECSTuple>>();
				d.add(list);
				queryAnswerListSet2.put(new QueryPattern(originalQueryLinks),d);
			}
			return true;
		}
		if((queryLinks.get(0).ecs.subjectCS.longRep & ecs.subjectCS.longRep) != queryLinks.get(0).ecs.subjectCS.longRep){
			
			return false;
		}
		if(queryLinks.get(0).ecs.objectCS != null && ecs.objectCS != null)
			if((queryLinks.get(0).ecs.objectCS.longRep & ecs.objectCS.longRep) != queryLinks.get(0).ecs.objectCS.longRep){
				
				return false;
			}
		/*if(queryLinks.get(0).ecs.objectCS == null && ecs.objectCS != null) return false;
		if(queryLinks.get(0).ecs.objectCS != null && ecs.objectCS == null) return false;*/
		int ind = indexOfProperty(ecsLongArrayMap.get(ecs), propertiesSet.get(queryLinks.get(0).ecs.predicate.getURI()));
		if(ind < 0) return false;
		if(visited.contains(ecs)){
			if(queryAnswerListSet2.containsKey(new QueryPattern(originalQueryLinks)))
				queryAnswerListSet2.get(new QueryPattern(originalQueryLinks)).add(list);			
			else{
				HashSet<ArrayList<ECSTuple>> d = new HashSet<ArrayList<ECSTuple>>();
				d.add(list);
				queryAnswerListSet2.put(new QueryPattern(originalQueryLinks),d);
			}			
		}
		visited.add(ecs);
		//System.out.println("maeded ");
		ECSTuple ecsTuple = new ECSTuple(ecs, propertiesSet.get(queryLinks.get(0).ecs.predicate.getURI()), getQueryTriplePattern(queryLinks.get(0).ecs));
		ecsTuple.subjectBinds = queryLinks.get(0).ecs.subjectBinds;
		ecsTuple.objectBinds = queryLinks.get(0).ecs.objectBinds;
		if(!ecsTupleIntegerMap.containsKey(ecsTuple)){
			reverseECSTupleIntegerMap.put(nextECSTuple, ecsTuple);
			ecsTupleIntegerMap.put(ecsTuple, nextECSTuple++);
			
		}
		list.add(ecsTuple);
		
		if(!links.containsKey(ecs)){
				
			if(queryAnswerListSet2.containsKey(new QueryPattern(originalQueryLinks)))
				queryAnswerListSet2.get(new QueryPattern(originalQueryLinks)).add(list);			
			else{
				HashSet<ArrayList<ECSTuple>> d = new HashSet<ArrayList<ECSTuple>>();
				d.add(list);
				queryAnswerListSet2.put(new QueryPattern(originalQueryLinks),d);
			}		
			
			return true;
		}
		else{			
			for(ExtendedCharacteristicSet child : links.get(ecs)){				
				
					if(queryLinks.size()>1){
						//System.out.println("size: " + queryLinks.size());
						ArrayList<ECSTuple> dummy = new ArrayList<ECSTuple>();
						dummy.addAll(list);
						
						dfsOnECSLinks2(child, links, new ArrayList<>(queryLinks.subList(1, queryLinks.size())), originalQueryLinks, dummy);
					}
					else {
						if(queryAnswerListSet2.containsKey(new QueryPattern(originalQueryLinks)))
							queryAnswerListSet2.get(new QueryPattern(originalQueryLinks)).add(list);			
						else{
							HashSet<ArrayList<ECSTuple>> d = new HashSet<ArrayList<ECSTuple>>();
							d.add(list);
							queryAnswerListSet2.put(new QueryPattern(originalQueryLinks),d);
						}					
						
					}			
			}
		}
		
		return false;
									
	}
	
	public static long convert(BitSet bitset) {
        long value = 0L;
        for (int i = 0; i < bitset.length(); ++i) {
          value += bitset.get(i) ? (1L << i) : 0L;
        }
        return value;
      }


	public static boolean debug = false;
		public static void oTheosVoithos4(
			  ECSTuple ecsTuple,			  			  
			  //ConcurrentHashMap<int[], Boolean> paths,									  									   									  							
			  //HashMap<int[], Boolean> paths,
			  List<int[]> paths,
			  ArrayList<Integer> commonHashVariables,
			  int projectVars, 									 
			  //ConcurrentHashMap<int[], Boolean> outerPathProbe, 									  
			  //HashMap<int[], Boolean> outerPathProbe,
			  List<int[]> outerPathProbe,
			  HashMap<Integer, HashSet<Integer>> singleBindings,
			  HashMap<Integer, HashSet<Integer>> outerBindings,  
			  HashMap<ECSTuple, AnswerPattern> qans,
			  QueryPattern queryPattern,
			  HashMap<QueryPattern, HashSet<ArrayList<Integer>>> commonVarsMap,
			  HashMap<BigInteger, int[]> binder, boolean add, 
			  HashMap<QueryPattern, HashSet<QueryPattern>> joinedQueryPatterns
				){
				 
					
			if(sslist.containsKey(ecsTuple.triplePattern.s) 
					&& sslist.get(ecsTuple.triplePattern.s).contains(ecsTuple) 
					&& !sskilllist.contains(ecsTuple.ecs.subjectCS) ) {				
				return;
			}
			if(oolist.containsKey(ecsTuple.triplePattern.o) 
					&& oolist.get(ecsTuple.triplePattern.o).contains(ecsTuple) 
					&& !ookilllist.contains(ecsTuple.ecs.objectCS) ) {				
				return;
			}
			//debug = false;
				if(qans.get(ecsTuple).children.size() == 0){// == 1){	
					
						ECSTuple tuple = ecsTuple;
						long[] toIter = ecsLongArrayMap.get(tuple.ecs);
						//long[] toIter = dbECSMap.get(ecsIntegerMap.get(tuple.ecs));
						/*long[] toIter;
						if(cacheMap.containsKey(ecsIntegerMap.get(tuple.ecs)))
							toIter = cacheMap.get(ecsIntegerMap.get(tuple.ecs));
						else{
							toIter = dbECSMap.get(ecsIntegerMap.get(tuple.ecs));
							cacheMap.put(ecsIntegerMap.get(tuple.ecs), toIter);	
						}*/
						int start ;												

						int end = toIter.length;

						if(tuple.triplePattern.s >= 0){
							
							long ps = (long)(((long)tuple.property << 27) | (long)tuple.triplePattern.s) & 0x1FFFFFFFFFl;
							
							start = indexOfPS(toIter, ps, 0);							

						}
						else{
							//start = indexOfProperty(toIter, tuple.property);
							start = propIndexMap.get(tuple.ecs).get(tuple.property);
						}
						
						if(start < 0) return;
						long triple;
						int tripleS, tripleO;
						boolean binds;
						int[] row;
						for(int k = start; k < end; k++){
														
								triple = toIter[k];
								
								tripleS = (int)((triple >> 27) & 0x7FFFFFF);
								
								tripleO = (int)((triple & 0x7FFFFFF));
								
								if(outerBindings.containsKey(tuple.triplePattern.s) && 
										!outerBindings.get(tuple.triplePattern.s).contains(tripleS)) 
									continue;
								
								if(outerBindings.containsKey(tuple.triplePattern.o) && 
										!outerBindings.get(tuple.triplePattern.o).contains(tripleO)) 
									continue;
								
								if((int)((triple >> 54)  & 0x3ff) != tuple.property)
									break;
								if(tuple.triplePattern.s >= 0){
									//System.out.println("hello");
									long ps = (long)(((long)tuple.property << 27) | (long)tuple.triplePattern.s) & 0x1FFFFFFFFFl;
									if((long)((long)(triple >> 27) & 0x1FFFFFFFFFl) != ps)
										break;
									
								}
								binds = true;
								
								if(tuple.subjectBinds != null){
									 
									
									for(Integer prop : tuple.subjectBinds.keySet()){
										binds = false;
										int obj = tuple.subjectBinds.get(prop);
										long tripleSPOLong = ((long)tripleS << 37 | (long)prop << 27 | (long)obj);
										if(indexOfTriple(spoIndex, tripleSPOLong) < 0) {											
											break;
										}
										binds = true;
									
									}				

								}
								if(!binds) continue;
								
								binds = true;
								if(tuple.objectBinds != null){
									
									
									for(Integer prop : tuple.objectBinds.keySet()){
										binds = false;
										int obj = tuple.objectBinds.get(prop);																			
										long tripleSPOLong = ((long)tripleO << 37 | (long)prop << 27 | (long)obj);
										if(indexOfTriple(spoIndex, tripleSPOLong) < 0) {											
											break;
										}
										binds = true;
										
										
									}				

								}
								if(!binds) continue;
								
								row = new int[projectVars];								
								row[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.s))] = tripleS;
								//if (tuple.triplePattern.o < 0)
								row[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.o))] = tripleO;
								
								BigInteger sum  = BigInteger.valueOf(0);
								for(Integer hashIndex : commonHashVariables){												
																																			
									sum = sum.add(BigInteger.valueOf(row[hashIndex]));
									sum = sum.shiftLeft(32);									
																							
								}
								binder.put(sum, row);
								if(tuple.triplePattern.s < 0){
									if(singleBindings.containsKey(tuple.triplePattern.s))
										singleBindings.get(tuple.triplePattern.s).add(tripleS);
									else{
										HashSet<Integer> d = new HashSet<Integer>();
										d.add(tripleS);
										singleBindings.put(tuple.triplePattern.s, d);
									}
								}
								if(tuple.triplePattern.o < 0){
									if(singleBindings.containsKey(tuple.triplePattern.o))
										singleBindings.get(tuple.triplePattern.o).add(tripleO);
									else{
										HashSet<Integer> d = new HashSet<Integer>();
										d.add(tripleO);
										singleBindings.put(tuple.triplePattern.o, d);
									}
								}
								//if(add)
									//paths.put(row, true);
								paths.add(row);
													
								
						}
				}
				else {
										
				//long[] toIter = dbECSMap.get(ecsIntegerMap.get(ecsTuple.ecs));
				/*long[] toIter;
				if(cacheMap.containsKey(ecsIntegerMap.get(ecsTuple.ecs)))
					toIter = cacheMap.get(ecsIntegerMap.get(ecsTuple.ecs));
				else{
					toIter = dbECSMap.get(ecsIntegerMap.get(ecsTuple.ecs));
					cacheMap.put(ecsIntegerMap.get(ecsTuple.ecs), toIter);
				}*/
				//HashMap<ExtendedCharacteristicSet, long[]> ecsLongArrayMap = ecsLongArrayMapOS; //reverse order
				
				long[] toIter = ecsLongArrayMap.get(ecsTuple.ecs);
				
				//int start = indexOfProperty(toIter, ecsTuple.property);
				int start = propIndexMap.get(ecsIntegerMap.get(ecsTuple.ecs)).get(ecsTuple.property);
				if(start < 0) return;	
				StackRow stackRow ;
				long triple;
				ArrayDeque<StackRow> stack;
				int[] row;
				HashSet<Long> visited;
				boolean binds = true;
				for(int k = start; k < toIter.length; k++){

				
						//long triple = toIter[k];
						triple = toIter[k];
						if((int)((triple >> 54)  & 0x3ff) != ecsTuple.property) {	 				
							break;
						}	 					
						
						//Stack<StackRow> stack = new Stack<StackRow>();
						stack = new ArrayDeque<StackRow>();
						//Stack<int[][]> stack = new Stack<int[][]>();
						
						row = new int[projectVars];
						row[varIndexMap.get(reverseVarMap.get(ecsTuple.triplePattern.s))] = (int)((triple >> 27) & 0x7FFFFFF);
						row[varIndexMap.get(reverseVarMap.get(ecsTuple.triplePattern.o))] = (int)(triple & 0x7FFFFFF);
						/*row[varIndexMap.get(reverseVarMap.get(ecsTuple.triplePattern.o))] = (int)((triple >> 27) & 0x7FFFFFF);
						row[varIndexMap.get(reverseVarMap.get(ecsTuple.triplePattern.s))] = (int)(triple & 0x7FFFFFF);*/
						
						stackRow = new StackRow();
						stackRow.row = row;
						stackRow.ecsTuple = ecsTuple;
						stack.push(stackRow);
						/*int[][] drow = new int[2][row.length];
						drow[0] = row;
						drow[1] = new int[] {ecsTupleIntegerMap.get(ecsTuple)};
						stack.push(drow);*/
						/*if(!killlist.contains(ecsTuple.ecs.subjectCS) || !killlist.contains(ecsTuple.ecs.objectCS)) 
							continue;*/
						
						visited  = new HashSet<Long>();						
						
						//while(!stack.empty()){
						StackRow stackRowSoFar;
						int[] rowSoFar;
						ECSTuple tuple;
						while(!stack.isEmpty()){
														
							stackRowSoFar = stack.pop();
							//int[][] drowSoFar = stack.pop();
							rowSoFar = stackRowSoFar.row;
							//int[] rowSoFar = drowSoFar[0];
							tuple = stackRowSoFar.ecsTuple;
							if(tuple.subjectBinds != null){
								 								
								for(Integer prop : tuple.subjectBinds.keySet()){
									binds = false;
									int obj = tuple.subjectBinds.get(prop);
									long tripleSPOLong = ((long)rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.s))] << 37 | (long)prop << 27 | (long)obj);
									if(indexOfTriple(spoIndex, tripleSPOLong) < 0) {											
										break;
									}
									binds = true;
								
								}				

							}
							if(!binds) continue;
							
							binds = true;
							if(tuple.objectBinds != null){
								
								
								for(Integer prop : tuple.objectBinds.keySet()){
									binds = false;
									int obj = tuple.objectBinds.get(prop);																			
									long tripleSPOLong = ((long)rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.o))] << 37 | (long)prop << 27 | (long)obj);
									if(indexOfTriple(spoIndex, tripleSPOLong) < 0) {											
										break;
									}
									binds = true;
									
									
								}				

							}
							if(!binds) continue;
							//ECSTuple tuple = reverseECSTupleIntegerMap.get(drowSoFar[1][0]);
							//if(debug) System.out.println(Arrays.toString(rowSoFar));
							//System.out.println("next; " + tuple.toString());
							triple = ((long)tuple.property << 54 | 
									(long)rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.s))] << 27 
									| (long)rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.o))]);
							/*triple = ((long)tuple.property << 54 | 
									(long)rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.o))] << 27 
									| (long)rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.s))]);*/
													 												
							visited.add(triple);
													
							if(qans.get(tuple) == null || qans.get(tuple).children == null){
												
								
								//paths.put(rowSoFar, true);
								paths.add(rowSoFar);
								BigInteger sum  = BigInteger.valueOf(0);
								for(Integer hashIndex : commonHashVariables){												
																																			
									sum = sum.add(BigInteger.valueOf(rowSoFar[hashIndex]));
									sum = sum.shiftLeft(32);									
																							
								}
								binder.put(sum, row);
								/*for(int i = 0; i < rowSoFar.length; i++){
									if(singleBindings.containsKey(i))
										singleBindings.get(i).add(rowSoFar[i]);
									else{
										HashSet<Integer> d = new HashSet<Integer>();
										d.add(rowSoFar[i]);
										singleBindings.put(i, d);
									}
								}*/
								continue;
							}
							
							Set<AnswerPattern> nextSet = qans.get(tuple).children;
							
							for(AnswerPattern nextAnswer : nextSet){
								
								ECSTuple nextTuple = nextAnswer.root;
								/*if(debug){
									System.out.println("next answer: " + nextAnswer.children);
									System.out.println("next tuple: " + nextTuple.toString());
								}*/
								if(sslist.containsKey(nextTuple.triplePattern.s) 
										&& sslist.get(nextTuple.triplePattern.s).contains(nextTuple) 
										&& !sskilllist.contains(nextTuple.ecs.subjectCS) ) {
									
									/*if(debug)
										System.out.println("i breaking subject");*/
									continue;
								}
								if(oolist.containsKey(nextTuple.triplePattern.o) 
										&& oolist.get(nextTuple.triplePattern.o).contains(nextTuple) 
										&& !ookilllist.contains(nextTuple.ecs.objectCS) ) {
									/*if(debug)
										System.out.println("i breaking object");*/
									continue;
								}
								long[] children = ecsLongArrayMap.get(nextTuple.ecs);
								//long[] children = dbECSMap.get(ecsIntegerMap.get(nextTuple.ecs));
								/*long[] children;
								if(cacheMap.containsKey(ecsIntegerMap.get(ecsTuple.ecs)))
									children = cacheMap.get(ecsIntegerMap.get(ecsTuple.ecs));
								else{
									children = dbECSMap.get(ecsIntegerMap.get(ecsTuple.ecs));
									cacheMap.put(ecsIntegerMap.get(ecsTuple.ecs), children);
								}*/
								
								
								if(children == null) continue;
								
								int lastObject = rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.o))];
								//int lastObject = rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.s))];
								
								//int pstart = indexOfProperty(children, nextTuple.property); 
								//int pstart = propIndexMap.get(nextTuple.ecs).get(nextTuple.property);
								
								//int startingIndex = indexOfSubject(children, lastObject, pstart);
								if(!subjectIndexMap.get(nextTuple.ecs).containsKey(lastObject)) continue;
								int startingIndex = subjectIndexMap.get(nextTuple.ecs).get(lastObject).get(nextTuple.property);
								
								//if(startingIndex < 0) continue;							
								
								for(int i = startingIndex; i < children.length; i++){
									long nextTriple = children[i];
									
									int sub = (int)((nextTriple >> 27) & 0x7FFFFFF);
									
									if(sub > lastObject) {									
										break;
									}
									
									if(!visited.contains(nextTriple)){
										//System.out.println("here3");
										rowSoFar[varIndexMap.get(reverseVarMap.get(nextTuple.triplePattern.s))] = sub;
										rowSoFar[varIndexMap.get(reverseVarMap.get(nextTuple.triplePattern.o))] = (int)(nextTriple & 0x7FFFFFF);
										/*rowSoFar[varIndexMap.get(reverseVarMap.get(nextTuple.triplePattern.o))] = sub;
										rowSoFar[varIndexMap.get(reverseVarMap.get(nextTuple.triplePattern.s))] = (int)(nextTriple & 0x7FFFFFF);*/
										
										StackRow pushRow = new StackRow();
										//pushRow.row = rowSoFar.clone();
										pushRow.row = new int[rowSoFar.length];
										System.arraycopy(rowSoFar,0, pushRow.row,0,rowSoFar.length);
										pushRow.ecsTuple = nextTuple;
										stack.push(pushRow);
										/*int[][] pushdrow = new int[2][rowSoFar.length];
										System.arraycopy(rowSoFar,0, pushdrow[0],0,rowSoFar.length);
										drow[1] = new int[] {ecsTupleIntegerMap.get(nextTuple)};
										stack.push(pushdrow);*/
									}
								}																													
							}
						}
					} 										
				}	
		}
	
	
		public static void oTheosVoithos5(
				  ECSTuple ecsTuple,			  			  
				  //ConcurrentHashMap<int[], Boolean> paths,									  									   									  							
				  //HashMap<int[], Boolean> paths,
				  List<int[]> paths,
				  ArrayList<Integer> commonVars,
				  int projectVars, 									 
				  //ConcurrentHashMap<int[], Boolean> outerPathProbe, 									  
				  //HashMap<int[], Boolean> outerPathProbe,
				  List<int[]> outerPathProbe,
				  HashMap<Integer, HashSet<Integer>> singleBindings,
				  HashMap<Integer, HashSet<Integer>> outerBindings,  
				  HashMap<ECSTuple, AnswerPattern> qans,
				  QueryPattern queryPattern,
				  HashMap<QueryPattern, HashSet<ArrayList<Integer>>> commonVarsMap,
				  HashMap<BigInteger, int[]> binder, boolean add, 
				  HashMap<QueryPattern, HashSet<QueryPattern>> joinedQueryPatterns
				  
					){
					 
						
				if(sslist.containsKey(ecsTuple.triplePattern.s) 
						&& sslist.get(ecsTuple.triplePattern.s).contains(ecsTuple) 
						&& !sskilllist.contains(ecsTuple.ecs.subjectCS) ) {				
					return;
				}
				if(oolist.containsKey(ecsTuple.triplePattern.o) 
						&& oolist.get(ecsTuple.triplePattern.o).contains(ecsTuple) 
						&& !ookilllist.contains(ecsTuple.ecs.objectCS) ) {				
					return;
				}
				//debug = false;
					if(qans.get(ecsTuple).children.size() == 0){// == 1){	
						
							ECSTuple tuple = ecsTuple;						
							long[] toIter = ecsLongArrayMap.get(tuple.ecs);
							//long[] toIter = dbECSMap.get(ecsIntegerMap.get(tuple.ecs));
							/*long[] toIter;
							if(cacheMap.containsKey(ecsIntegerMap.get(tuple.ecs)))
								toIter = cacheMap.get(ecsIntegerMap.get(tuple.ecs));
							else{
								toIter = dbECSMap.get(ecsIntegerMap.get(tuple.ecs));
								cacheMap.put(ecsIntegerMap.get(tuple.ecs), toIter);
							}	*/				
							int start ;												

							int end = toIter.length;

							if(tuple.triplePattern.s >= 0){
								
								long ps = (long)(((long)tuple.property << 27) | (long)tuple.triplePattern.s) & 0x1FFFFFFFFFl;
								
								start = indexOfPS(toIter, ps, 0);							

							}
							else{
								//start = indexOfProperty(toIter, tuple.property);
								start = propIndexMap.get(ecsIntegerMap.get(tuple.ecs)).get(tuple.property);
							}
							
							if(start < 0) return;
							long triple;
							int tripleS, tripleO;
							boolean binds;
							int[] row;
							for(int k = start; k < end; k++){
															
									triple = toIter[k];
									
									tripleS = (int)((triple >> 27) & 0x7FFFFFF);
									
									tripleO = (int)((triple & 0x7FFFFFF));
									
									if(outerBindings.containsKey(tuple.triplePattern.s) && 
											!outerBindings.get(tuple.triplePattern.s).contains(tripleS)) 
										continue;
									
									if(outerBindings.containsKey(tuple.triplePattern.o) && 
											!outerBindings.get(tuple.triplePattern.o).contains(tripleO)) 
										continue;
									
									if((int)((triple >> 54)  & 0x3ff) != tuple.property)
										break;
									if(tuple.triplePattern.s >= 0){
										//System.out.println("hello");
										long ps = (long)(((long)tuple.property << 27) | (long)tuple.triplePattern.s) & 0x1FFFFFFFFFl;
										if((long)((long)(triple >> 27) & 0x1FFFFFFFFFl) != ps)
											break;
										//System.out.println("hello1");
									}
									binds = true;								
									
									if(tuple.subjectBinds != null){
										//TODO			
										//int sind = indexOfSubjectSO(spoIndex, tripleS, startS);
										//if(sind < 0) break;
										for(Integer prop : tuple.subjectBinds.keySet()){
											binds = false;
											int obj = tuple.subjectBinds.get(prop);
											long tripleSPOLong = ((long)tripleS << 37 | (long)prop << 27 | (long)obj);
											if(indexOfTriple(spoIndex, tripleSPOLong) < 0) {											
												break;
											}
											binds = true;
											
										}				

									}
									if(!binds) continue;
									
									binds = true;
									if(tuple.objectBinds != null){
										//TODO						
										/*int oind = indexOfSubjectSO(spoIndex, tripleO, startO);
										if(oind < 0) break;*/
										
										for(Integer prop : tuple.objectBinds.keySet()){
											binds = false;
											int obj = tuple.objectBinds.get(prop);																			
											long tripleSPOLong = ((long)tripleO << 37 | (long)prop << 27 | (long)obj);
											if(indexOfTriple(spoIndex, tripleSPOLong) < 0) {											
												break;
											}
											binds = true;
											
										
										}				
	
									}
									if(!binds) continue;
									
									row = new int[projectVars];
									row[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.s))] = tripleS;
									row[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.o))] = tripleO;
									
									BigInteger sum  = BigInteger.valueOf(0);
									for(Integer hashIndex : commonVars){												
																																				
										sum = sum.add(BigInteger.valueOf(row[hashIndex]));
										sum = sum.shiftLeft(32);									
																								
									}
									if(!binder.containsKey(sum)) continue;
									if(tuple.triplePattern.s < 0){
										if(singleBindings.containsKey(tuple.triplePattern.s))
											singleBindings.get(tuple.triplePattern.s).add(tripleS);
										else{
											HashSet<Integer> d = new HashSet<Integer>();
											d.add(tripleS);
											singleBindings.put(tuple.triplePattern.s, d);
										}
									}
									if(tuple.triplePattern.o < 0){
										if(singleBindings.containsKey(tuple.triplePattern.o))
											singleBindings.get(tuple.triplePattern.o).add(tripleO);
										else{
											HashSet<Integer> d = new HashSet<Integer>();
											d.add(tripleO);
											singleBindings.put(tuple.triplePattern.o, d);
										}
									}
									//if(add)
									//paths.put(row, true);
									paths.add(row);
									
									/*else{
										
										for(ArrayList<Integer> nextVarList : commonVarsMap.get(queryPattern)){
											BigInteger sum  = BigInteger.valueOf(0);
											for(Integer var : nextVarList){																
												sum = sum.add(BigInteger.valueOf(row[var]));
												sum = sum.shiftLeft(32);
											}																				
											binder.get(nextVarList).put(sum, row);
										}									
									}		*/						
									
							}
					}
					else {
											
				
					//HashMap<ExtendedCharacteristicSet, long[]> ecsLongArrayMap = ecsLongArrayMapOS; //reverse order
					
					long[] toIter = ecsLongArrayMap.get(ecsTuple.ecs);
					//long[] toIter = dbECSMap.get(ecsIntegerMap.get(ecsTuple.ecs));
						/*long[] toIter;
						if(cacheMap.containsKey(ecsIntegerMap.get(ecsTuple.ecs)))
							toIter = cacheMap.get(ecsIntegerMap.get(ecsTuple.ecs));
						else{
							toIter = dbECSMap.get(ecsIntegerMap.get(ecsTuple.ecs));
							cacheMap.put(ecsIntegerMap.get(ecsTuple.ecs), toIter);
						}*/
					//int start = indexOfProperty(toIter, ecsTuple.property);
					int start = propIndexMap.get(ecsIntegerMap.get(ecsTuple.ecs)).get(ecsTuple.property);
					if(start < 0) return;	
					StackRow stackRow ;
					long triple;
					ArrayDeque<StackRow> stack;
					int[] row;
					HashSet<Long> visited;
					boolean binds = true;
					for(int k = start; k < toIter.length; k++){

											
							triple = toIter[k];
							if((int)((triple >> 54)  & 0x3ff) != ecsTuple.property) {	 				
								break;
							}
							
							//Stack<StackRow> stack = new Stack<StackRow>();
							stack = new ArrayDeque<StackRow>();
							
							row = new int[projectVars];
							row[varIndexMap.get(reverseVarMap.get(ecsTuple.triplePattern.s))] = (int)((triple >> 27) & 0x7FFFFFF);
							row[varIndexMap.get(reverseVarMap.get(ecsTuple.triplePattern.o))] = (int)(triple & 0x7FFFFFF);
							/*row[varIndexMap.get(reverseVarMap.get(ecsTuple.triplePattern.o))] = (int)((triple >> 27) & 0x7FFFFFF);
							row[varIndexMap.get(reverseVarMap.get(ecsTuple.triplePattern.s))] = (int)(triple & 0x7FFFFFF);*/
							
							stackRow = new StackRow();
							stackRow.row = row;
							stackRow.ecsTuple = ecsTuple;
							stack.push(stackRow);
							/*if(!killlist.contains(ecsTuple.ecs.subjectCS) || !killlist.contains(ecsTuple.ecs.objectCS)) 
								continue;*/
							
							visited  = new HashSet<Long>();						
							
							//while(!stack.empty()){
							while(!stack.isEmpty()){
															
								StackRow stackRowSoFar = stack.pop(); 
								int[] rowSoFar = stackRowSoFar.row;
								ECSTuple tuple = stackRowSoFar.ecsTuple;
								if(tuple.subjectBinds != null){
									 
									
									for(Integer prop : tuple.subjectBinds.keySet()){
										binds = false;
										int obj = tuple.subjectBinds.get(prop);
										long tripleSPOLong = ((long)rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.s))] << 37 | (long)prop << 27 | (long)obj);
										if(indexOfTriple(spoIndex, tripleSPOLong) < 0) {											
											break;
										}
										binds = true;
									
									}				

								}
								if(!binds) continue;
								
								binds = true;
								if(tuple.objectBinds != null){
									
									
									for(Integer prop : tuple.objectBinds.keySet()){
										binds = false;
										int obj = tuple.objectBinds.get(prop);																			
										long tripleSPOLong = ((long)rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.o))] << 37 | (long)prop << 27 | (long)obj);
										if(indexOfTriple(spoIndex, tripleSPOLong) < 0) {											
											break;
										}
										binds = true;
										
										
									}				

								}
								if(!binds) continue;
								//if(debug) System.out.println(Arrays.toString(rowSoFar));
								//System.out.println("next; " + tuple.toString());
								triple = ((long)tuple.property << 54 | 
										(long)rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.s))] << 27 
										| (long)rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.o))]);
								/*triple = ((long)tuple.property << 54 | 
										(long)rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.o))] << 27 
										| (long)rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.s))]);*/
														 												
								visited.add(triple);
														
								if(qans.get(tuple) == null || qans.get(tuple).children == null){
													
									
									
									BigInteger sum  = BigInteger.valueOf(0);
									for(Integer hashIndex : commonVars){												
																																				
										sum = sum.add(BigInteger.valueOf(rowSoFar[hashIndex]));
										sum = sum.shiftLeft(32);									
																								
									}
									if(!binder.containsKey(sum)) continue;
									//paths.put(rowSoFar, true);
									paths.add(rowSoFar);
									
									/*for(int i = 0; i < rowSoFar.length; i++){
										if(singleBindings.containsKey(i))
											singleBindings.get(i).add(rowSoFar[i]);
										else{
											HashSet<Integer> d = new HashSet<Integer>();
											d.add(rowSoFar[i]);
											singleBindings.put(i, d);
										}
									}*/
									continue;
								}
								
								Set<AnswerPattern> nextSet = qans.get(tuple).children;
								
								for(AnswerPattern nextAnswer : nextSet){
									
									ECSTuple nextTuple = nextAnswer.root;
									/*if(debug){
										System.out.println("next answer: " + nextAnswer.children);
										System.out.println("next tuple: " + nextTuple.toString());
									}*/
									if(sslist.containsKey(nextTuple.triplePattern.s) 
											&& sslist.get(nextTuple.triplePattern.s).contains(nextTuple) 
											&& !sskilllist.contains(nextTuple.ecs.subjectCS) ) {
										
										continue;
									}
									if(oolist.containsKey(nextTuple.triplePattern.o) 
											&& oolist.get(nextTuple.triplePattern.o).contains(nextTuple) 
											&& !ookilllist.contains(nextTuple.ecs.objectCS) ) {
										
										continue;
									}
									long[] children = ecsLongArrayMap.get(nextTuple.ecs);
									//long[] children = dbECSMap.get(ecsIntegerMap.get(nextTuple.ecs));
									/*long[] children;
									if(cacheMap.containsKey(ecsIntegerMap.get(ecsTuple.ecs)))
										children = cacheMap.get(ecsIntegerMap.get(ecsTuple.ecs));
									else{
										children = dbECSMap.get(ecsIntegerMap.get(ecsTuple.ecs));
										cacheMap.put(ecsIntegerMap.get(ecsTuple.ecs), children);
									}*/
									if(children == null ) continue;
									
									int lastObject = rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.o))];
									//int lastObject = rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.s))];
									
									//int pstart = indexOfProperty(children, nextTuple.property); 
									//int pstart = propIndexMap.get(nextTuple.ecs).get(nextTuple.property);
									
									//int startingIndex = indexOfSubject(children, lastObject, pstart);
									if(!subjectIndexMap.get(nextTuple.ecs).containsKey(lastObject)) continue;
									int startingIndex = subjectIndexMap.get(nextTuple.ecs).get(lastObject).get(nextTuple.property);
									
									//if(startingIndex < 0) continue;							
									
									for(int i = startingIndex; i < children.length; i++){
										long nextTriple = children[i];
										
										int sub = (int)((nextTriple >> 27) & 0x7FFFFFF);
										
										if(sub > lastObject) {									
											break;
										}
										
										if(!visited.contains(nextTriple)){
											//System.out.println("here3");
											rowSoFar[varIndexMap.get(reverseVarMap.get(nextTuple.triplePattern.s))] = sub;
											rowSoFar[varIndexMap.get(reverseVarMap.get(nextTuple.triplePattern.o))] = (int)(nextTriple & 0x7FFFFFF);
											/*rowSoFar[varIndexMap.get(reverseVarMap.get(nextTuple.triplePattern.o))] = sub;
											rowSoFar[varIndexMap.get(reverseVarMap.get(nextTuple.triplePattern.s))] = (int)(nextTriple & 0x7FFFFFF);*/
											
											StackRow pushRow = new StackRow();
											//pushRow.row = rowSoFar.clone();
											pushRow.row = new int[rowSoFar.length];
											System.arraycopy(rowSoFar,0, pushRow.row,0,rowSoFar.length);
											pushRow.ecsTuple = nextTuple;
											stack.push(pushRow);
										}
									}																													
								}
							}					
						} 										
					}	
			}
		
		public static void oTheosVoithos4(
				  ECSTuple ecsTuple,			  			  
				  //ConcurrentHashMap<int[], Boolean> paths,									  									   									  							
				  //HashMap<int[], Boolean> paths,
				  List<int[]> paths,
				  ArrayList<Integer> commonHashVariables,
				  int projectVars, 									 
				  //ConcurrentHashMap<int[], Boolean> outerPathProbe, 									  
				  //HashMap<int[], Boolean> outerPathProbe,
				  List<int[]> outerPathProbe,
				  HashMap<Integer, HashSet<Integer>> singleBindings,
				  HashMap<Integer, HashSet<Integer>> outerBindings,  
				  HashMap<ECSTuple, AnswerPattern> qans,
				  QueryPattern queryPattern,
				  HashMap<QueryPattern, HashSet<ArrayList<Integer>>> commonVarsMap,
				  HashMap<BigInteger, int[]> binder, boolean add, 
				  HashMap<QueryPattern, HashSet<QueryPattern>> joinedQueryPatterns, boolean reverse			  
					){
					 
						
				if(sslist.containsKey(ecsTuple.triplePattern.s) 
						&& sslist.get(ecsTuple.triplePattern.s).contains(ecsTuple) 
						&& !sskilllist.contains(ecsTuple.ecs.subjectCS) ) {				
					return;
				}
				if(oolist.containsKey(ecsTuple.triplePattern.o) 
						&& oolist.get(ecsTuple.triplePattern.o).contains(ecsTuple) 
						&& !ookilllist.contains(ecsTuple.ecs.objectCS) ) {				
					return;
				}
				//debug = false;
					if(qans.get(ecsTuple).children.size() == 0){// == 1){	
						
							ECSTuple tuple = ecsTuple;						
							long[] toIter = ecsLongArrayMap.get(tuple.ecs);
							//long[] toIter = dbECSMap.get(ecsIntegerMap.get(tuple.ecs));
							/*long[] toIter;
							if(cacheMap.containsKey(ecsIntegerMap.get(tuple.ecs)))
								toIter = cacheMap.get(ecsIntegerMap.get(tuple.ecs));
							else{
								toIter = dbECSMap.get(ecsIntegerMap.get(tuple.ecs));
								cacheMap.put(ecsIntegerMap.get(tuple.ecs), toIter);
							}*/
							int start ;												

							int end = toIter.length;

							if(tuple.triplePattern.s >= 0){
								
								long ps = (long)(((long)tuple.property << 27) | (long)tuple.triplePattern.s) & 0x1FFFFFFFFFl;
								
								start = indexOfPS(toIter, ps, 0);							

							}
							else{
								//start = indexOfProperty(toIter, tuple.property);
								start = propIndexMap.get(ecsIntegerMap.get(tuple.ecs)).get(tuple.property);
							}
							
							if(start < 0) return;
							long triple;
							int tripleS, tripleO;
							boolean binds;
							int[] row;
							for(int k = start; k < end; k++){
															
									triple = toIter[k];
									
									tripleS = (int)((triple >> 27) & 0x7FFFFFF);
									
									tripleO = (int)((triple & 0x7FFFFFF));
									
									if(outerBindings.containsKey(tuple.triplePattern.s) && 
											!outerBindings.get(tuple.triplePattern.s).contains(tripleS)) 
										continue;
									
									if(outerBindings.containsKey(tuple.triplePattern.o) && 
											!outerBindings.get(tuple.triplePattern.o).contains(tripleO)) 
										continue;
									
									if((int)((triple >> 54)  & 0x3ff) != tuple.property)
										break;
									if(tuple.triplePattern.s >= 0){
										//System.out.println("hello");
										long ps = (long)(((long)tuple.property << 27) | (long)tuple.triplePattern.s) & 0x1FFFFFFFFFl;
										if((long)((long)(triple >> 27) & 0x1FFFFFFFFFl) != ps)
											break;
										//System.out.println("hello1");
									}
									binds = true;								
									
									if(tuple.subjectBinds != null){
										 
										
										for(Integer prop : tuple.subjectBinds.keySet()){
											binds = false;
											int obj = tuple.subjectBinds.get(prop);
											long tripleSPOLong = ((long)tripleS << 37 | (long)prop << 27 | (long)obj);
											if(indexOfTriple(spoIndex, tripleSPOLong) < 0) {											
												break;
											}
											binds = true;
										
										}				

									}
									if(!binds) continue;
									
									binds = true;
									if(tuple.objectBinds != null){
										
										
										for(Integer prop : tuple.objectBinds.keySet()){
											binds = false;
											int obj = tuple.objectBinds.get(prop);																			
											long tripleSPOLong = ((long)tripleO << 37 | (long)prop << 27 | (long)obj);
											if(indexOfTriple(spoIndex, tripleSPOLong) < 0) {											
												break;
											}
											binds = true;
											
											
										}				

									}
									if(!binds) continue;
									
									row = new int[projectVars];
									row[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.s))] = tripleS;
									//if(tuple.triplePattern.o < 0)
									row[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.o))] = tripleO;
									
									BigInteger sum  = BigInteger.valueOf(0);
									for(Integer hashIndex : commonHashVariables){												
																																				
										sum = sum.add(BigInteger.valueOf(row[hashIndex]));
										sum = sum.shiftLeft(32);									
																								
									}
									binder.put(sum, row);
									if(tuple.triplePattern.s < 0){
										if(singleBindings.containsKey(tuple.triplePattern.s))
											singleBindings.get(tuple.triplePattern.s).add(tripleS);
										else{
											HashSet<Integer> d = new HashSet<Integer>();
											d.add(tripleS);
											singleBindings.put(tuple.triplePattern.s, d);
										}
									}
									if(tuple.triplePattern.o < 0){
										if(singleBindings.containsKey(tuple.triplePattern.o))
											singleBindings.get(tuple.triplePattern.o).add(tripleO);
										else{
											HashSet<Integer> d = new HashSet<Integer>();
											d.add(tripleO);
											singleBindings.put(tuple.triplePattern.o, d);
										}
									}
									//if(add)
										//paths.put(row, true);
									paths.add(row);
									
									/*else{
										
										for(ArrayList<Integer> nextVarList : commonVarsMap.get(queryPattern)){
											BigInteger sum  = BigInteger.valueOf(0);
											for(Integer var : nextVarList){																
												sum = sum.add(BigInteger.valueOf(row[var]));
												sum = sum.shiftLeft(32);
											}																				
											binder.get(nextVarList).put(sum, row);
										}									
									}		*/						
									
							}
					}
					else {
											
					//long[] toIter = dbECSMap.get(ecsIntegerMap.get(asList.get(0)));
					//HashMap<ExtendedCharacteristicSet, long[]> ecsLongArrayMap = ecsLongArrayMapOS; //reverse order
					
					long[] toIter = ecsLongArrayMapOS.get(ecsTuple.ecs);
					//long[] toIter = dbECSMapOS.get(ecsIntegerMap.get(ecsTuple.ecs));
						/*long[] toIter;
						if(cacheMapOS.containsKey(ecsIntegerMap.get(ecsTuple.ecs)))
							toIter = cacheMapOS.get(ecsIntegerMap.get(ecsTuple.ecs));
						else{
							toIter = dbECSMapOS.get(ecsIntegerMap.get(ecsTuple.ecs));
							cacheMapOS.put(ecsIntegerMap.get(ecsTuple.ecs), toIter);
						}*/
					//int start = indexOfProperty(toIter, ecsTuple.property);
					int start = propIndexMapReverse.get(ecsTuple.ecs).get(ecsTuple.property);
					if(start < 0) return;	
					StackRow stackRow ;
					long triple;
					ArrayDeque<StackRow> stack;
					int[] row;
					HashSet<Long> visited;
					boolean binds = true;
					for(int k = start; k < toIter.length; k++){

					
							//long triple = toIter[k];
							triple = toIter[k];
							if((int)((triple >> 54)  & 0x3ff) != ecsTuple.property) {	 				
								break;
							}	 					
							
							//Stack<StackRow> stack = new Stack<StackRow>();
							stack = new ArrayDeque<StackRow>();
							//Stack<int[][]> stack = new Stack<int[][]>();
							
							row = new int[projectVars];
							/*row[varIndexMap.get(reverseVarMap.get(ecsTuple.triplePattern.s))] = (int)((triple >> 27) & 0x7FFFFFF);
							row[varIndexMap.get(reverseVarMap.get(ecsTuple.triplePattern.o))] = (int)(triple & 0x7FFFFFF);*/
							row[varIndexMap.get(reverseVarMap.get(ecsTuple.triplePattern.o))] = (int)((triple >> 27) & 0x7FFFFFF);
							row[varIndexMap.get(reverseVarMap.get(ecsTuple.triplePattern.s))] = (int)(triple & 0x7FFFFFF);
							
							stackRow = new StackRow();
							stackRow.row = row;
							stackRow.ecsTuple = ecsTuple;
							stack.push(stackRow);
							/*int[][] drow = new int[2][row.length];
							drow[0] = row;
							drow[1] = new int[] {ecsTupleIntegerMap.get(ecsTuple)};
							stack.push(drow);*/
							/*if(!killlist.contains(ecsTuple.ecs.subjectCS) || !killlist.contains(ecsTuple.ecs.objectCS)) 
								continue;*/
							
							visited  = new HashSet<Long>();						
							
							//while(!stack.empty()){
							while(!stack.isEmpty()){
															
								StackRow stackRowSoFar = stack.pop();
								//int[][] drowSoFar = stack.pop();
								int[] rowSoFar = stackRowSoFar.row;
								//int[] rowSoFar = drowSoFar[0];
								ECSTuple tuple = stackRowSoFar.ecsTuple;
								if(tuple.subjectBinds != null){
									 
									
									for(Integer prop : tuple.subjectBinds.keySet()){
										binds = false;
										int obj = tuple.subjectBinds.get(prop);
										long tripleSPOLong = ((long)rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.s))] << 37 | (long)prop << 27 | (long)obj);
										if(indexOfTriple(spoIndex, tripleSPOLong) < 0) {											
											break;
										}
										binds = true;
									
									}				

								}
								if(!binds) continue;
								
								binds = true;
								if(tuple.objectBinds != null){
									
									
									for(Integer prop : tuple.objectBinds.keySet()){
										binds = false;
										int obj = tuple.objectBinds.get(prop);																			
										long tripleSPOLong = ((long)rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.o))] << 37 | (long)prop << 27 | (long)obj);
										if(indexOfTriple(spoIndex, tripleSPOLong) < 0) {											
											break;
										}
										binds = true;
										
										
									}				

								}
								if(!binds) continue;
								//ECSTuple tuple = reverseECSTupleIntegerMap.get(drowSoFar[1][0]);
								//if(debug) System.out.println(Arrays.toString(rowSoFar));
								//System.out.println("next; " + tuple.toString());
								/*triple = ((long)tuple.property << 54 | 
										(long)rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.s))] << 27 
										| (long)rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.o))]);*/
								triple = ((long)tuple.property << 54 | 
										(long)rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.o))] << 27 
										| (long)rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.s))]);
														 												
								visited.add(triple);
														
								if(qans.get(tuple) == null || qans.get(tuple).children == null){
													
									
									//paths.put(rowSoFar, true);
									paths.add(rowSoFar);
									BigInteger sum  = BigInteger.valueOf(0);
									for(Integer hashIndex : commonHashVariables){												
																																				
										sum = sum.add(BigInteger.valueOf(rowSoFar[hashIndex]));
										sum = sum.shiftLeft(32);									
																								
									}
									binder.put(sum, row);
									/*for(int i = 0; i < rowSoFar.length; i++){
										if(singleBindings.containsKey(i))
											singleBindings.get(i).add(rowSoFar[i]);
										else{
											HashSet<Integer> d = new HashSet<Integer>();
											d.add(rowSoFar[i]);
											singleBindings.put(i, d);
										}
									}*/
									continue;
								}
								
								Set<AnswerPattern> nextSet = qans.get(tuple).children;
								
								for(AnswerPattern nextAnswer : nextSet){
									
									ECSTuple nextTuple = nextAnswer.root;
									/*if(debug){
										System.out.println("next answer: " + nextAnswer.children);
										System.out.println("next tuple: " + nextTuple.toString());
									}*/
									if(sslist.containsKey(nextTuple.triplePattern.s) 
											&& sslist.get(nextTuple.triplePattern.s).contains(nextTuple) 
											&& !sskilllist.contains(nextTuple.ecs.subjectCS) ) {
										
										/*if(debug)
											System.out.println("i breaking subject");*/
										continue;
									}
									if(oolist.containsKey(nextTuple.triplePattern.o) 
											&& oolist.get(nextTuple.triplePattern.o).contains(nextTuple) 
											&& !ookilllist.contains(nextTuple.ecs.objectCS) ) {
										/*if(debug)
											System.out.println("i breaking object");*/
										continue;
									}
									long[] children = ecsLongArrayMapOS.get(nextTuple.ecs);
									//long[] children = dbECSMapOS.get(ecsIntegerMap.get(nextTuple.ecs));
									/*long[] children;
									if(cacheMapOS.containsKey(ecsIntegerMap.get(ecsTuple.ecs)))
										children = cacheMapOS.get(ecsIntegerMap.get(ecsTuple.ecs));
									else{
										children = dbECSMap.get(ecsIntegerMap.get(ecsTuple.ecs));
										cacheMapOS.put(ecsIntegerMap.get(ecsTuple.ecs), children);
									}*/
									if(children == null) continue;
									
									//int lastObject = rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.o))];
									int lastObject = rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.s))];
									
									//int pstart = indexOfProperty(children, nextTuple.property); 
									//int pstart = propIndexMap.get(nextTuple.ecs).get(nextTuple.property);
									
									//int startingIndex = indexOfSubject(children, lastObject, pstart);
									if(!subjectIndexMapReverse.get(nextTuple.ecs).containsKey(lastObject)) continue;
									int startingIndex = subjectIndexMapReverse.get(nextTuple.ecs).get(lastObject).get(nextTuple.property);
									
									//if(startingIndex < 0) continue;							
									
									for(int i = startingIndex; i < children.length; i++){
										long nextTriple = children[i];
										
										int sub = (int)((nextTriple >> 27) & 0x7FFFFFF);
										
										if(sub > lastObject) {									
											break;
										}
										
										if(!visited.contains(nextTriple)){
											//System.out.println("here3");
											/*rowSoFar[varIndexMap.get(reverseVarMap.get(nextTuple.triplePattern.s))] = sub;
											rowSoFar[varIndexMap.get(reverseVarMap.get(nextTuple.triplePattern.o))] = (int)(nextTriple & 0x7FFFFFF);*/
											rowSoFar[varIndexMap.get(reverseVarMap.get(nextTuple.triplePattern.o))] = sub;
											rowSoFar[varIndexMap.get(reverseVarMap.get(nextTuple.triplePattern.s))] = (int)(nextTriple & 0x7FFFFFF);
											
											StackRow pushRow = new StackRow();
											//pushRow.row = rowSoFar.clone();
											pushRow.row = new int[rowSoFar.length];
											System.arraycopy(rowSoFar,0, pushRow.row,0,rowSoFar.length);
											pushRow.ecsTuple = nextTuple;
											stack.push(pushRow);
											/*int[][] pushdrow = new int[2][rowSoFar.length];
											System.arraycopy(rowSoFar,0, pushdrow[0],0,rowSoFar.length);
											drow[1] = new int[] {ecsTupleIntegerMap.get(nextTuple)};
											stack.push(pushdrow);*/
										}
									}																													
								}
							}
						} 										
					}	
			}
		
		
			public static void oTheosVoithos5(
					  ECSTuple ecsTuple,			  			  
					  //ConcurrentHashMap<int[], Boolean> paths,									  									   									  							
					  //HashMap<int[], Boolean> paths,
					  List<int[]> paths,
					  ArrayList<Integer> commonVars,
					  int projectVars, 									 
					  //ConcurrentHashMap<int[], Boolean> outerPathProbe, 									  
					 // HashMap<int[], Boolean> outerPathProbe,
					  List<int[]> outerPathProbe,
					  HashMap<Integer, HashSet<Integer>> singleBindings,
					  HashMap<Integer, HashSet<Integer>> outerBindings,  
					  HashMap<ECSTuple, AnswerPattern> qans,
					  QueryPattern queryPattern,
					  HashMap<QueryPattern, HashSet<ArrayList<Integer>>> commonVarsMap,
					  HashMap<BigInteger, int[]> binder, boolean add, 
					  HashMap<QueryPattern, HashSet<QueryPattern>> joinedQueryPatterns,
					  boolean reverse
						){
						 
							
					if(sslist.containsKey(ecsTuple.triplePattern.s) 
							&& sslist.get(ecsTuple.triplePattern.s).contains(ecsTuple) 
							&& !sskilllist.contains(ecsTuple.ecs.subjectCS) ) {				
						return;
					}
					if(oolist.containsKey(ecsTuple.triplePattern.o) 
							&& oolist.get(ecsTuple.triplePattern.o).contains(ecsTuple) 
							&& !ookilllist.contains(ecsTuple.ecs.objectCS) ) {				
						return;
					}
					//debug = false;
						if(qans.get(ecsTuple).children.size() == 0){// == 1){	
							
								ECSTuple tuple = ecsTuple;						
								long[] toIter = ecsLongArrayMap.get(tuple.ecs);
								//long[] toIter = dbECSMap.get(ecsIntegerMap.get(tuple.ecs));
								/*long[] toIter;
								if(cacheMap.containsKey(ecsIntegerMap.get(tuple.ecs)))
									toIter = cacheMap.get(ecsIntegerMap.get(tuple.ecs));
								else{
									toIter = dbECSMap.get(ecsIntegerMap.get(tuple.ecs));
									cacheMap.put(ecsIntegerMap.get(tuple.ecs), toIter);	
								}*/
								int start ;												

								int end = toIter.length;

								if(tuple.triplePattern.s >= 0){
									
									long ps = (long)(((long)tuple.property << 27) | (long)tuple.triplePattern.s) & 0x1FFFFFFFFFl;
									
									start = indexOfPS(toIter, ps, 0);							

								}
								else{
									//start = indexOfProperty(toIter, tuple.property);
									start = propIndexMap.get(ecsIntegerMap.get(tuple.ecs)).get(tuple.property);
								}
								
								if(start < 0) return;
								long triple;
								int tripleS, tripleO;
								boolean binds;
								int[] row;
								for(int k = start; k < end; k++){
																
										triple = toIter[k];
										
										tripleS = (int)((triple >> 27) & 0x7FFFFFF);
										
										tripleO = (int)((triple & 0x7FFFFFF));
										
										if(outerBindings.containsKey(tuple.triplePattern.s) && 
												!outerBindings.get(tuple.triplePattern.s).contains(tripleS)) 
											continue;
										
										if(outerBindings.containsKey(tuple.triplePattern.o) && 
												!outerBindings.get(tuple.triplePattern.o).contains(tripleO)) 
											continue;
										
										if((int)((triple >> 54)  & 0x3ff) != tuple.property)
											break;
										if(tuple.triplePattern.s >= 0){
											//System.out.println("hello");
											long ps = (long)(((long)tuple.property << 27) | (long)tuple.triplePattern.s) & 0x1FFFFFFFFFl;
											if((long)((long)(triple >> 27) & 0x1FFFFFFFFFl) != ps)
												break;
											//System.out.println("hello1");
										}
										binds = true;								
										
										if(tuple.subjectBinds != null){
											//TODO			
											//int sind = indexOfSubjectSO(spoIndex, tripleS, startS);
											//if(sind < 0) break;
											for(Integer prop : tuple.subjectBinds.keySet()){
												binds = false;
												int obj = tuple.subjectBinds.get(prop);
												long tripleSPOLong = ((long)tripleS << 37 | (long)prop << 27 | (long)obj);
												if(indexOfTriple(spoIndex, tripleSPOLong) < 0) {											
													break;
												}
												binds = true;
												
											}				

										}
										if(!binds) continue;
										
										binds = true;
										if(tuple.objectBinds != null){
											//TODO						
											/*int oind = indexOfSubjectSO(spoIndex, tripleO, startO);
											if(oind < 0) break;*/
											
											for(Integer prop : tuple.objectBinds.keySet()){
												binds = false;
												int obj = tuple.objectBinds.get(prop);																			
												long tripleSPOLong = ((long)tripleO << 37 | (long)prop << 27 | (long)obj);
												if(indexOfTriple(spoIndex, tripleSPOLong) < 0) {											
													break;
												}
												binds = true;
												
											
											}				
		
										}
										if(!binds) continue;
										
										row = new int[projectVars];
										row[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.s))] = tripleS;
										row[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.o))] = tripleO;
										
										BigInteger sum  = BigInteger.valueOf(0);
										for(Integer hashIndex : commonVars){												
																																					
											sum = sum.add(BigInteger.valueOf(row[hashIndex]));
											sum = sum.shiftLeft(32);									
																									
										}
										if(!binder.containsKey(sum)) continue;
										if(tuple.triplePattern.s < 0){
											if(singleBindings.containsKey(tuple.triplePattern.s))
												singleBindings.get(tuple.triplePattern.s).add(tripleS);
											else{
												HashSet<Integer> d = new HashSet<Integer>();
												d.add(tripleS);
												singleBindings.put(tuple.triplePattern.s, d);
											}
										}
										if(tuple.triplePattern.o < 0){
											if(singleBindings.containsKey(tuple.triplePattern.o))
												singleBindings.get(tuple.triplePattern.o).add(tripleO);
											else{
												HashSet<Integer> d = new HashSet<Integer>();
												d.add(tripleO);
												singleBindings.put(tuple.triplePattern.o, d);
											}
										}
										//if(add)
										//paths.put(row, true);
										paths.add(row);
										
										/*else{
											
											for(ArrayList<Integer> nextVarList : commonVarsMap.get(queryPattern)){
												BigInteger sum  = BigInteger.valueOf(0);
												for(Integer var : nextVarList){																
													sum = sum.add(BigInteger.valueOf(row[var]));
													sum = sum.shiftLeft(32);
												}																				
												binder.get(nextVarList).put(sum, row);
											}									
										}		*/						
										
								}
						}
						else {
												
						//long[] toIter = dbECSMap.get(ecsIntegerMap.get(asList.get(0)));
						//HashMap<ExtendedCharacteristicSet, long[]> ecsLongArrayMap = ecsLongArrayMapOS; //reverse order
						
						long[] toIter = ecsLongArrayMapOS.get(ecsTuple.ecs);
						//long[] toIter = dbECSMapOS.get(ecsIntegerMap.get(ecsTuple.ecs));
							/*long[] toIter;
							if(cacheMapOS.containsKey(ecsIntegerMap.get(ecsTuple.ecs)))
								toIter = cacheMapOS.get(ecsIntegerMap.get(ecsTuple.ecs));
							else{
								toIter = dbECSMapOS.get(ecsIntegerMap.get(ecsTuple.ecs));
								cacheMapOS.put(ecsIntegerMap.get(ecsTuple.ecs), toIter);			
							}*/
						//int start = indexOfProperty(toIter, ecsTuple.property);
						int start = propIndexMap.get(ecsIntegerMap.get(ecsTuple.ecs)).get(ecsTuple.property);
						if(start < 0) return;	
						StackRow stackRow ;
						long triple;
						ArrayDeque<StackRow> stack;
						int[] row;
						HashSet<Long> visited;
						boolean binds = true;
						for(int k = start; k < toIter.length; k++){

												
								triple = toIter[k];
								if((int)((triple >> 54)  & 0x3ff) != ecsTuple.property) {	 				
									break;
								}
								
								//Stack<StackRow> stack = new Stack<StackRow>();
								stack = new ArrayDeque<StackRow>();
								
								row = new int[projectVars];
								/*row[varIndexMap.get(reverseVarMap.get(ecsTuple.triplePattern.s))] = (int)((triple >> 27) & 0x7FFFFFF);
								row[varIndexMap.get(reverseVarMap.get(ecsTuple.triplePattern.o))] = (int)(triple & 0x7FFFFFF);*/
								row[varIndexMap.get(reverseVarMap.get(ecsTuple.triplePattern.o))] = (int)((triple >> 27) & 0x7FFFFFF);
								row[varIndexMap.get(reverseVarMap.get(ecsTuple.triplePattern.s))] = (int)(triple & 0x7FFFFFF);
								
								stackRow = new StackRow();
								stackRow.row = row;
								stackRow.ecsTuple = ecsTuple;
								stack.push(stackRow);
								/*if(!killlist.contains(ecsTuple.ecs.subjectCS) || !killlist.contains(ecsTuple.ecs.objectCS)) 
									continue;*/
								
								visited  = new HashSet<Long>();						
								
								//while(!stack.empty()){
								while(!stack.isEmpty()){
																
									StackRow stackRowSoFar = stack.pop(); 
									int[] rowSoFar = stackRowSoFar.row;
									ECSTuple tuple = stackRowSoFar.ecsTuple;
									if(tuple.subjectBinds != null){
										 
										
										for(Integer prop : tuple.subjectBinds.keySet()){
											binds = false;
											int obj = tuple.subjectBinds.get(prop);
											long tripleSPOLong = ((long)rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.s))] << 37 | (long)prop << 27 | (long)obj);
											if(indexOfTriple(spoIndex, tripleSPOLong) < 0) {											
												break;
											}
											binds = true;
										
										}				

									}
									if(!binds) continue;
									
									binds = true;
									if(tuple.objectBinds != null){
										
										
										for(Integer prop : tuple.objectBinds.keySet()){
											binds = false;
											int obj = tuple.objectBinds.get(prop);																			
											long tripleSPOLong = ((long)rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.o))] << 37 | (long)prop << 27 | (long)obj);
											if(indexOfTriple(spoIndex, tripleSPOLong) < 0) {											
												break;
											}
											binds = true;
											
											
										}				

									}
									if(!binds) continue;
									//if(debug) System.out.println(Arrays.toString(rowSoFar));
									//System.out.println("next; " + tuple.toString());
									/*triple = ((long)tuple.property << 54 | 
											(long)rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.s))] << 27 
											| (long)rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.o))]);*/
									triple = ((long)tuple.property << 54 | 
											(long)rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.o))] << 27 
											| (long)rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.s))]);
															 												
									visited.add(triple);
															
									if(qans.get(tuple) == null || qans.get(tuple).children == null){
														
										
										
										BigInteger sum  = BigInteger.valueOf(0);
										for(Integer hashIndex : commonVars){												
																																					
											sum = sum.add(BigInteger.valueOf(rowSoFar[hashIndex]));
											sum = sum.shiftLeft(32);									
																									
										}
										if(!binder.containsKey(sum)) continue;
										//paths.put(rowSoFar, true);
										paths.add(rowSoFar);
										
										/*for(int i = 0; i < rowSoFar.length; i++){
											if(singleBindings.containsKey(i))
												singleBindings.get(i).add(rowSoFar[i]);
											else{
												HashSet<Integer> d = new HashSet<Integer>();
												d.add(rowSoFar[i]);
												singleBindings.put(i, d);
											}
										}*/
										continue;
									}
									
									Set<AnswerPattern> nextSet = qans.get(tuple).children;
									
									for(AnswerPattern nextAnswer : nextSet){
										
										ECSTuple nextTuple = nextAnswer.root;
										/*if(debug){
											System.out.println("next answer: " + nextAnswer.children);
											System.out.println("next tuple: " + nextTuple.toString());
										}*/
										if(sslist.containsKey(nextTuple.triplePattern.s) 
												&& sslist.get(nextTuple.triplePattern.s).contains(nextTuple) 
												&& !sskilllist.contains(nextTuple.ecs.subjectCS) ) {
											
											continue;
										}
										if(oolist.containsKey(nextTuple.triplePattern.o) 
												&& oolist.get(nextTuple.triplePattern.o).contains(nextTuple) 
												&& !ookilllist.contains(nextTuple.ecs.objectCS) ) {
											
											continue;
										}//96724008
										 //88226025
										long[] children = ecsLongArrayMapOS.get(nextTuple.ecs);
										//long[] children = dbECSMapOS.get(ecsIntegerMap.get(nextTuple.ecs));
										/*long[] children;
										if(cacheMapOS.containsKey(ecsIntegerMap.get(ecsTuple.ecs)))
											children = cacheMapOS.get(ecsIntegerMap.get(ecsTuple.ecs));
										else{
											children = dbECSMap.get(ecsIntegerMap.get(ecsTuple.ecs));
											cacheMapOS.put(ecsIntegerMap.get(ecsTuple.ecs), children);
										}*/
										if(children == null ) continue;
										
										//int lastObject = rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.o))];
										int lastObject = rowSoFar[varIndexMap.get(reverseVarMap.get(tuple.triplePattern.s))];
										
										//int pstart = indexOfProperty(children, nextTuple.property); 
										//int pstart = propIndexMap.get(nextTuple.ecs).get(nextTuple.property);
										
										//int startingIndex = indexOfSubject(children, lastObject, pstart);
										if(!subjectIndexMapReverse.get(nextTuple.ecs).containsKey(lastObject)) continue;
										int startingIndex = subjectIndexMapReverse.get(nextTuple.ecs).get(lastObject).get(nextTuple.property);
										
										//if(startingIndex < 0) continue;							
										
										for(int i = startingIndex; i < children.length; i++){
											long nextTriple = children[i];
											
											int sub = (int)((nextTriple >> 27) & 0x7FFFFFF);
											
											if(sub > lastObject) {									
												break;
											}
											
											if(!visited.contains(nextTriple)){
												//System.out.println("here3");
												/*rowSoFar[varIndexMap.get(reverseVarMap.get(nextTuple.triplePattern.s))] = sub;
												rowSoFar[varIndexMap.get(reverseVarMap.get(nextTuple.triplePattern.o))] = (int)(nextTriple & 0x7FFFFFF);*/
												rowSoFar[varIndexMap.get(reverseVarMap.get(nextTuple.triplePattern.o))] = sub;
												rowSoFar[varIndexMap.get(reverseVarMap.get(nextTuple.triplePattern.s))] = (int)(nextTriple & 0x7FFFFFF);
												
												StackRow pushRow = new StackRow();
												//pushRow.row = rowSoFar.clone();
												pushRow.row = new int[rowSoFar.length];
												System.arraycopy(rowSoFar,0, pushRow.row,0,rowSoFar.length);
												pushRow.ecsTuple = nextTuple;
												stack.push(pushRow);
											}
										}																													
									}
								}					
							} 										
						}	
				}
		
	
	
	public static int indexOfProperty(long[] a, int key) {
		 int lo = 0;
	        int hi = a.length - 1;
	        int firstOccurrence = Integer.MIN_VALUE;
	        while (lo <= hi) {
	        	int mid = lo + (hi - lo) / 2;
	        	
	            int s = (int)((a[mid] >> 54)  & 0x3ff);
	          
	        	 if (s == key) {
	                 // key found and we want to search an earlier occurrence
	                 firstOccurrence = mid;	                 
	                 hi = mid - 1;	                 
	             } else if (s < key) {
	            	 lo = mid + 1;
	             } else {
	            	 hi = mid - 1;
	             }
	            
	        }
	        if (firstOccurrence != Integer.MIN_VALUE) {
	            return firstOccurrence;
	        }

	        return -1;
    }
	
	public static int indexOfProperty(ArrayList<Long> a, int key) {
        int lo = 0;
        int hi = a.size() - 1;   
        int firstOccurrence = Integer.MIN_VALUE;
        while (lo <= hi) {
        	int mid = lo + (hi - lo) / 2;
            int s = (int)((a.get(mid) >> 54)  & 0x3ff);
        	 if (s == key) {
                 // key found and we want to search an earlier occurrence
                 firstOccurrence = mid;
                 hi = mid - 1;
             } else if (s < key) {
            	 lo = mid + 1;
             } else {
            	 hi = mid - 1;
             }
            
            
        }
        if (firstOccurrence != Integer.MIN_VALUE) {
            return firstOccurrence;
        }

        return -1;
    }
	
	public static int indexOfSubject(long[] a, int key) {
        int lo = 0;
        int hi = a.length - 1;        
        while (lo <= hi) {
            // Key is in a[lo..hi] or not present.
            int mid = lo + (hi - lo) / 2;
            int s = (int)((a[mid] >> 27) & 0x7FFFFFF);
            if      (key < s ) hi = mid - 1;
            else if (key > s) lo = mid + 1;
            else return mid;
        }
        return -1;
    }
	
	public static int indexOfSubject(long[] a, int key, int start) {
        int lo = start;
        int hi = a.length - 1; 
        int firstOccurrence = Integer.MIN_VALUE;
        while (lo <= hi) {
        	int mid = lo + (hi - lo) / 2;
        	int s = (int)((a[mid] >> 27) & 0x7FFFFFF);
        	//System.out.println("s: " + s);
        	if (s == key) {
                 // key found and we want to search an earlier occurrence
                 firstOccurrence = mid;
                 hi = mid - 1;
             } else if (s < key) {
            	 lo = mid + 1;
             } else {
            	 hi = mid - 1;
             }                        
        }
        if (firstOccurrence != Integer.MIN_VALUE) {
            return firstOccurrence;
        }
        
        return -1;
       
    }
	
	public static int indexOfPS(long[] a, long key, int start) {
        int lo = start;
        int hi = a.length - 1;  
        int firstOccurrence = Integer.MIN_VALUE;
        while (lo <= hi) {
        	int mid = lo + (hi - lo) / 2;
        	long s = (long)((a[mid] >> 27) & 0x1FFFFFFFFFl);
        	//System.out.println("s: " + s);
        	if (s == key) {
                 // key found and we want to search an earlier occurrence
                 firstOccurrence = mid;
                 hi = mid - 1;
             } else if (s < key) {
            	 lo = mid + 1;
             } else {
            	 hi = mid - 1;
             }                        
        }
        if (firstOccurrence != Integer.MIN_VALUE) {
            return firstOccurrence;
        }

        return -1;
        
    }
	
	public static int indexOfSubjectSO(long[] a, int key) {
        
        int lo = 0;
        int hi = a.length - 1;   
        int firstOccurrence = Integer.MIN_VALUE;
        while (lo <= hi) {
        	int mid = lo + (hi - lo) / 2;
        	int s = (int)((a[mid] >> 37) & 0x7FFFFFF); //mask 27 bits
        	 if (s == key) {
                 // key found and we want to search an earlier occurrence
                 firstOccurrence = mid;
                 hi = mid - 1;
             } else if (s < key) {
            	 lo = mid + 1;
             } else {
            	 hi = mid - 1;
             }                        
        }
        if (firstOccurrence != Integer.MIN_VALUE) {
            return firstOccurrence;
        }

        return -1;
    }
	
	public static int indexOfSubjectSO(long[] a, int key, int start) {
        
        int lo = start;
        int hi = a.length - 1;   
        int firstOccurrence = Integer.MIN_VALUE;
        while (lo <= hi) {
        	int mid = lo + (hi - lo) / 2;
        	int s = (int)((a[mid] >> 37) & 0x7FFFFFF); //mask 27 bits
        	 if (s == key) {
                 // key found and we want to search an earlier occurrence
                 firstOccurrence = mid;
                 hi = mid - 1;
             } else if (s < key) {
            	 lo = mid + 1;
             } else {
            	 hi = mid - 1;
             }                        
        }
        if (firstOccurrence != Integer.MIN_VALUE) {
            return firstOccurrence;
        }

        return -1;
    }
	
	public static int indexOfPropertySO(long[] a, int key, int start) {
        int lo = start;
        int hi = a.length - 1;        
                     
        int firstOccurrence = Integer.MIN_VALUE;
        while (lo <= hi) {
          	int mid = lo + (hi - lo) / 2;
           	int s = (int)((a[mid] >> 27) & 0x3FF); //mask 10 bits
           	 if (s == key) {
                    // key found and we want to search an earlier occurrence
                    firstOccurrence = mid;
                    hi = mid - 1;
             } else if (s < key) {
               	 lo = mid + 1;
             } else {
               	 hi = mid - 1;
             }                        
         }
         if (firstOccurrence != Integer.MIN_VALUE) {
             return firstOccurrence;
         }

         return -1;
        
    }
	
	
	public static int indexOfSubject(ArrayList<Long> a, int key, int start) {
        int lo = start;
        int hi = a.size() - 1;        
        while (lo <= hi) {
            // Key is in a[lo..hi] or not present.
            int mid = lo + (hi - lo) / 2;
            int s = (int)((a.get(mid) >> 27) & 0x7FFFFFF);
            if      (key < s ) hi = mid - 1;
            else if (key > s) lo = mid + 1;
            else return mid;
        }
        return -1;
    }
	
	public static int indexOfObject(long[] a, int key) {
        int lo = 0;
        int hi = a.length - 1;        
        while (lo <= hi) {
            // Key is in a[lo..hi] or not present.
            int mid = lo + (hi - lo) / 2;
            int s = (int)(a[mid] & 0x7FFFFFF);
            if      (key < s ) hi = mid - 1;
            else if (key > s) lo = mid + 1;
            else return mid;
        }
        return -1;
    }
	
	public static int indexOfTriple(long[] a, long key) {
        int lo = 0;
        int hi = a.length - 1;        
        while (lo <= hi) {
            // Key is in a[lo..hi] or not present.
            int mid = lo + (hi - lo) / 2;
            long s = a[mid];
            if      (key < s ) hi = mid - 1;
            else if (key > s) lo = mid + 1;
            else return mid;
        }
        return -1;
    }
	
	public static void printLong(long l){
		for(int i = 0; i < Long.numberOfLeadingZeros((long)l); i++) {
		      System.out.print('0');
		}
		System.out.println(Long.toBinaryString((long)l));
	}
	
	static class ECSComparator implements Comparator<ArrayList<ExtendedCharacteristicSet>>
	 {
	     public int compare(ArrayList<ExtendedCharacteristicSet> c1, ArrayList<ExtendedCharacteristicSet> c2)
	     {
	         return -1*(new Integer(c1.size())).compareTo(c2.size());
	     }
	 }
	
	static class ECSTupleComparator implements Comparator<QueryPattern>
	 {
	     public int compare(QueryPattern c1, QueryPattern c2)
	     {
	    	 if(c1.queryPattern.size() == c2.queryPattern.size()){
	    		 if(c1.boundVars > 0 && c2.boundVars > 0)
	    			 return (new Integer(c1.boundVars).compareTo(c2.boundVars));
	    		 else if(c1.boundVars > 0 )
	    			 return -1;
	    		 else if(c2.boundVars > 0 )
	    			 return +1;
	    		 else 
	    			 return 0;
	    	 }
	    	 
	    	 else
	    		 return -1*(new Integer(c1.queryPattern.size())).compareTo(c2.queryPattern.size());
	     }
	 }
	
	/*class Exec extends Thread {
		
		public Exec(ArrayList<QueryPattern> f, HashMap<QueryPattern, 
				HashMap<ECSTuple, AnswerPattern>> qans, 
				HashMap<QueryPattern, HashSet<ArrayList<Integer>>> commonVarsMap, 
				HashMap<int[], Boolean> outerPathProbe, 
				List<Var> projectVariables, 
				HashMap<QueryPattern, HashSet<QueryPattern>> joinedQueryPatterns){
			this.f = f;
			this.qans = qans;
			this.commonVarsMap = commonVarsMap;
			this.outerPathProbe = outerPathProbe;
			this.projectVariables = projectVariables;
			this.joinedQueryPatterns = joinedQueryPatterns;
		}
		
		ArrayList<QueryPattern> f ;
		HashMap<QueryPattern, HashMap<ECSTuple, AnswerPattern>> qans ;
		HashMap<QueryPattern, HashSet<ArrayList<Integer>>> commonVarsMap ;		
		HashMap<int[], Boolean> outerPathProbe ;
		List<Var> projectVariables ;
		HashMap<QueryPattern, HashSet<QueryPattern>> joinedQueryPatterns ;
		
		@Override		
		public void run(){
			
			//System.out.println("Starting thread...");
			ThreadMXBean thMxB = ManagementFactory.getThreadMXBean();
			HashMap<BigInteger, int[]> binder = new HashMap<BigInteger, int[]>();
	 		boolean addOrJoin = false;
	 		//int iteration = 0;
	 		HashMap<Integer, HashSet<Integer>> outBindings = new HashMap<Integer, HashSet<Integer>>();
	 		QueryPattern previousQueryPattern = null;
	 		
	 		for(int qi = 0; qi < f.size(); qi++){
	 			
	 			QueryPattern queryPatterns = f.get(qi);
	 			
	 			HashMap<int[], Boolean> paths = new HashMap<int[], Boolean>();
	 			
	 			HashMap<Integer, HashSet<Integer>> singleBindings = new HashMap<Integer, HashSet<Integer>>();
	 			
	 			ArrayList<Integer> commonJoinVariables = new ArrayList<>();
	 			ArrayList<Integer> commonHashVariables = new ArrayList<>();
	 			if(qi < f.size()-1){
	 				HashSet<Integer> vars = qpVarMap2.get(queryPatterns.queryPattern);
		 			HashSet<Integer> nextVars = qpVarMap2.get(f.get(qi+1).queryPattern);	 				 		
		 			for(Integer var : vars){
						if(nextVars.contains(var)){
							commonHashVariables.add(var);
						}					
		 			}
	 			}
	 			if(previousQueryPattern != null){
		 			HashSet<Integer> vars = qpVarMap2.get(queryPatterns.queryPattern);
		 			HashSet<Integer> previousVars = qpVarMap2.get(previousQueryPattern.queryPattern);	 				 		
		 			for(Integer var : vars){
						if(previousVars.contains(var)){
							commonJoinVariables.add(var);
						}					
		 			}
	 			}
	 			for(ECSTuple rootTuple : qans.get(queryPatterns).keySet()){
	 				//System.out.println("root tuple: " + rootTuple.toString());
	 				//System.out.println("root tuple: " + rootTuple.subjectBinds);
	 				if(outerPathProbe.isEmpty())
	 					oTheosVoithos4(rootTuple, 
							paths, commonHashVariables, projectVariables.size(), 
							outerPathProbe, singleBindings, outBindings,
							qans.get(queryPatterns),
							queryPatterns,commonVarsMap, binder, addOrJoin, joinedQueryPatterns);
	 				else
	 					oTheosVoithos5(rootTuple, 
							paths, commonJoinVariables, projectVariables.size(), 
							outerPathProbe, singleBindings, outBindings,
							qans.get(queryPatterns),
							queryPatterns,commonVarsMap, binder, addOrJoin, joinedQueryPatterns);
	 			}
	 			//System.out.println("ttt2: " + ttt2);
	 			for(Integer in : singleBindings.keySet()){
	 				if(outBindings.containsKey(in))
	 					outBindings.get(in).addAll(singleBindings.get(in));
	 				else{
	 					outBindings.put(in,singleBindings.get(in));
	 					
	 				}
	 			}
	 			 				 			
	 			outerPathProbe = paths;
		 		
	 			previousQueryPattern = queryPatterns;
	 			
	 		}
	 			 	
	 		
	 		System.out.println("CurrentThreadUserTime:"+thMxB.getCurrentThreadUserTime());
	 		System.out.println("CurrentThreadCpuTime:"+thMxB.getCurrentThreadCpuTime());
	 		System.out.println("yoga " + outerPathProbe.size() );//+ "\t: " + (tend-tstart)); 		
			inmem.close();
		
			db.close();
	 		
	 		
	 		
		}
	}
	*/
	/*public class DataPatternProcessor implements Runnable{
		
		private ArrayList<TripleAsInt> tasList;
		private ConcurrentHashMap<int[], Boolean> paths;
		private ArrayList<ExtendedCharacteristicSet> asList;
		private ConcurrentHashMap<int[], Boolean> outerPathProbe;		
		private HashMap<Integer, HashSet<Integer>> singleBindings;
		private ArrayList<Integer> pasList;
		private int projectVars;

		@Override
		public void run(){
			oTheosVoithos2(tasList, asList, pasList, 
						paths, projectVars, 
						outerPathProbe, singleBindings);
			//System.out.println("thread done");
		}
		
		public DataPatternProcessor(ArrayList<TripleAsInt> tasList, 									  
									  ArrayList<ExtendedCharacteristicSet> asList,									  
									  ArrayList<Integer> pasList,
									  ConcurrentHashMap<int[], Boolean> paths2,									 									   									  							
									  int projectVars, 									 				 
									  ConcurrentHashMap<int[], Boolean> outerPathProbe2, 									  
									  HashMap<Integer, HashSet<Integer>> singleBindings){
			this.tasList = tasList;
			this.pasList = pasList;
			this.asList = asList;
			this.paths = paths2;
			this.projectVars = projectVars;
			this.outerPathProbe = outerPathProbe2;			
			this.singleBindings = singleBindings;
		}

		public DataPatternProcessor(ArrayList<TripleAsInt> tasList2,
				ArrayList<ExtendedCharacteristicSet> dataPattern,
				ArrayList<Integer> pasList2, HashSet<int[]> paths2, int size,
				ConcurrentHashMap<int[], Boolean> outerPathProbe2,
				HashMap<Integer, HashSet<Integer>> singleBindings2) {
			// TODO Auto-generated constructor stub
		}
	}
	
	private static Callable<Void> toCallable(final Runnable runnable) {
	    return new Callable<Void>() {
	        @Override
	        public Void call() {
	            runnable.run();
	            return null;
	        }
	    };
	}*/
	
	public static TripleAsInt getQueryTriplePattern(ExtendedCharacteristicSet queryLinks){
		int s = -1, p = -1, o = -1;
		if(!queryLinks.subject.isVariable()){
			if(intMap.containsKey(queryLinks.subject))
				s = intMap.get(queryLinks.subject);			
		}
		else{
			if(!varMap.containsKey(queryLinks.subject)){
				reverseVarMap.put(nextVar, queryLinks.subject);
				varMap.put(queryLinks.subject, nextVar--);
			}
			s = varMap.get(queryLinks.subject);
		}
		if(!queryLinks.predicate.isVariable()){
			if(intMap.containsKey(queryLinks.predicate))
				p = propertiesSet.get(queryLinks.predicate.getURI());			
		}
		else{
			if(!varMap.containsKey(queryLinks.predicate)){
				reverseVarMap.put(nextVar, queryLinks.predicate);
				varMap.put(queryLinks.predicate, nextVar--);
			}
			p = varMap.get(queryLinks.predicate);
		}
		if(!queryLinks.object.isVariable()){
			if(intMap.containsKey(queryLinks.object))
				o = intMap.get(queryLinks.object);			
		}
		else{
			if(!varMap.containsKey(queryLinks.object)){
				reverseVarMap.put(nextVar, queryLinks.object);
				varMap.put(queryLinks.object, nextVar--);
			}
			o = varMap.get(queryLinks.object);
		}
		
		return new TripleAsInt(s, p, o);
	}
	
	public static HashMap<Long, Vector<Integer>> joinTwoECS(HashMap<Long, Vector<Integer>> res,
														HashMap<Long, Vector<Integer>> previous_res,
														ECSTuple e1, ECSTuple e2){
		
		if(previous_res == null){
			if(e2 == null){
				
				int p1 = propIndexMap.get(e1.ecs).get(e1.property);
				long[] e1_array = dbECSMap.get(ecsIntegerMap.get(e1.ecs));
				for(int i = p1; i < e1_array.length; i++){
					Vector<Integer> v = new Vector<Integer>();
					long t = e1_array[i];
					if((int)((t >> 54)  & 0x3ff) != e1.property)
						break;
					v.add((int)((t >> 27) & 0x7FFFFFF));
					v.add((int)((t & 0x7FFFFFF)));
					//res.add(v);
					res.put(szudzik(v.firstElement(), v.lastElement()), v);
				}			
			}
			else{
				
				HashMap<Integer, ArrayList<Vector<Integer>>> h = new HashMap<Integer, ArrayList<Vector<Integer>>>();			
				
				int p1 = propIndexMap.get(ecsIntegerMap.get(e1.ecs)).get(e1.property);
				long[] e1_array = dbECSMap.get(ecsIntegerMap.get(e1.ecs));
				for(int i = p1; i < e1_array.length; i++){				
					Vector<Integer> v = new Vector<Integer>();				
					long t = e1_array[i];				
					if((int)((t >> 54)  & 0x3ff) != e1.property) 
						break;				
					v.add((int)((t >> 27) & 0x7FFFFFF));				
					v.add((int)((t & 0x7FFFFFF)));
					ArrayList<Vector<Integer>> l = h.getOrDefault((int)((t & 0x7FFFFFF)), new ArrayList<Vector<Integer>>());
					l.add(v);
					h.put((int)((t & 0x7FFFFFF)), l);
				}	
				
				int p2 = propIndexMap.get(ecsIntegerMap.get(e2.ecs)).get(e2.property);
				long[] e2_array = dbECSMap.get(ecsIntegerMap.get(e2.ecs));
				for(int i = p2; i < e2_array.length; i++){				

					long t = e2_array[i];				
					if((int)((t >> 54)  & 0x3ff) != e2.property) 
						break;				
					if(h.containsKey((int)((t >> 27) & 0x7FFFFFF))){
						ArrayList<Vector<Integer>> l = h.get((int)((t >> 27) & 0x7FFFFFF));					
						for(Vector<Integer> v : l){
							
							Vector<Integer> nv = new Vector<Integer>(v);
							nv.add((int)((t & 0x7FFFFFF)));
							//res.add(nv);
							res.put(szudzik(nv.firstElement(), nv.lastElement()), nv);
							
						}
						
					}
					
				}						
			}
		}
		else{
			if(e2 == null){
				
				//int p1 = propIndexMap.get(e1.ecs).get(e1.property);
				int p1 = propIndexMap.get(ecsIntegerMap.get(e1.ecs)).get(e1.property);
				long[] e1_array = dbECSMap.get(ecsIntegerMap.get(e1.ecs));
				for(int i = p1; i < e1_array.length; i++){				
									
					long t = e1_array[i];				
					if((int)((t >> 54)  & 0x3ff) != e1.property) 
						break;				
					Vector<Integer> v = new Vector<Integer>();
					v.add((int)((t >> 27) & 0x7FFFFFF));				
					v.add((int)((t & 0x7FFFFFF)));				
					//res.add(v);
					long hash = szudzik(v.firstElement(), v.lastElement());
					if(previous_res.containsKey(hash)){
						res.put(hash, v);
					}
					
					
				}			
			}
			else{
				
				HashMap<Integer, ArrayList<Vector<Integer>>> h = new HashMap<Integer, ArrayList<Vector<Integer>>>();			
				
				//int p1 = propIndexMap.get(e1.ecs).get(e1.property);
				int p1 = propIndexMap.get(ecsIntegerMap.get(e1.ecs)).get(e1.property);
				long[] e1_array = dbECSMap.get(ecsIntegerMap.get(e1.ecs));
				for(int i = p1; i < e1_array.length; i++){				
					Vector<Integer> v = new Vector<Integer>();				
					long t = e1_array[i];				
					if((int)((t >> 54)  & 0x3ff) != e1.property) 
						break;				
					v.add((int)((t >> 27) & 0x7FFFFFF));				
					v.add((int)((t & 0x7FFFFFF)));
					ArrayList<Vector<Integer>> l = h.getOrDefault((int)((t & 0x7FFFFFF)), new ArrayList<Vector<Integer>>());
					l.add(v);
					h.put((int)((t & 0x7FFFFFF)), l);				
				}	
				
				//int p2 = propIndexMap.get(e2.ecs).get(e2.property);
				int p2 = propIndexMap.get(ecsIntegerMap.get(e2.ecs)).get(e2.property);
				long[] e2_array = dbECSMap.get(ecsIntegerMap.get(e2.ecs));
				for(int i = p2; i < e2_array.length; i++){				
									
					long t = e2_array[i];				
					if((int)((t >> 54)  & 0x3ff) != e2.property) 
						break;				
					if(h.containsKey((int)((t >> 27) & 0x7FFFFFF))){
						ArrayList<Vector<Integer>> l = h.get((int)((t >> 27) & 0x7FFFFFF));					
						for(Vector<Integer> v : l){
							
							Vector<Integer> nv = new Vector<Integer>(v);
							nv.add((int)((t & 0x7FFFFFF)));
							//res.add(nv);
							res.put(szudzik(nv.firstElement(), nv.lastElement()), nv);
						}
						
					}
					
				}						
			}
		}
		return res;
	}
	
	public static long szudzik(int a, int b){
				
		return a >= b ? a * a + a + b : a + b * b;
		
	}
	
	public static ExtendedCharacteristicSet getTripleECS(Triple triple){
		
		CharacteristicSet subjectCS = characteristicSetMap.get(triple.getSubject());
			
		CharacteristicSet objectCS = null;
			
		if(characteristicSetMap.containsKey(triple.getObject())){
				objectCS = characteristicSetMap.get(triple.getObject());
		}
			
		return new ExtendedCharacteristicSet(subjectCS, objectCS);
		
	}
	
}