package com.athena.imis.tests;

import java.io.File;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.Vector;

import org.apache.jena.graph.Node;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import com.athena.imis.models.BigAnswerPattern;
import com.athena.imis.models.BigCharacteristicSet;
import com.athena.imis.models.BigECSQuery;
import com.athena.imis.models.BigECSTuple;
import com.athena.imis.models.BigExtendedCharacteristicSet;
import com.athena.imis.models.BigQueryPattern;
import com.athena.imis.models.QueryPattern;
import com.athena.imis.models.TripleAsInt;


/**
 * This class is helpful in order to initialize an exisiting blinkDB instance and run a series of queries. 
 * The args[0] param contains the path to the blinkDB instance. 
 * The args[1] param contains the query set to test (0=LUBM, 1=Reactome, 2=Geonames), you can change this in order 
 * to define a custom SPARQL query.
 * 
 * @author Marios Meimaris
 *
 */

public class BigQueryTests {

	
	public static HashMap<BigQueryPattern, HashSet<ArrayList<BigECSTuple>>> queryAnswerListSet2 ;
	
	public static HashSet<BigExtendedCharacteristicSet> visited = new HashSet<BigExtendedCharacteristicSet>();
	
	static public HashMap<Node, Integer> varMap = new HashMap<Node, Integer>();
	
	static public HashMap<Integer, Node> reverseVarMap = new HashMap<Integer, Node>();
	
	public static Map<String, Integer> propertiesSet ;
	
	public static Map<Integer, String> reversePropertiesSet ; 
	
	public static int nextVar = -1;
	
	public static Map<Integer, HashMap<Integer, Integer>> propIndexMap ;
	
	public static Map<BigExtendedCharacteristicSet, Integer> ecsIntegerMap ;
	
	public static HashMap<Node, Integer> varIndexMap;
	
	public static Map<Integer, long[]> ecsLongArrayMap ;
	
	public static Map<Integer, long[]> csMap ;
	
	public static Map<Integer, BigCharacteristicSet> rucs ;
	
	public static Map<BigCharacteristicSet, Integer> ucs ;
	
	public static Map<String, Integer> intMap ;
	
	public static Map<Integer, String> reverseIntMap ;
	
	public static Map<String, Integer> prefixMap ;
	
	public static void main(String[] args) {
		
		System.out.println("Loading database file.");
		DB db = DBMaker.newFileDB(new File(args[0]))
		//DB db = DBMaker.newFileDB(new File("C:/temp/reactome_rose.bin"))
		//DB db = DBMaker.newFileDB(new File("C:/temp/temp"))
				.transactionDisable()
 				.fileChannelEnable() 			
 				.fileMmapEnable()
 				.cacheSize(1000000) 				
 				.closeOnJvmShutdown()
 				.make();
		System.out.println("Loading ecs map.");
		ecsLongArrayMap = db.hashMapCreate("ecsLongArrays")
 				.keySerializer(Serializer.INTEGER)
 				.valueSerializer(Serializer.LONG_ARRAY)
 				.makeOrGet();
		
		prefixMap = db.hashMapCreate("prefixMap")
 				.keySerializer(Serializer.STRING)
 				.valueSerializer(Serializer.INTEGER)
 				.makeOrGet();
		System.out.println("Loading prefix map.");
		
		Map<Integer, BigExtendedCharacteristicSet> ruecs = db.hashMapCreate("ruecsMap")
 				.keySerializer(Serializer.INTEGER)
 				//.valueSerializer(Serializer.)
 				.makeOrGet();
		System.out.println("Loading ruecs map.");
		
		intMap = db.treeMapCreate("intMap")
 				.keySerializer(Serializer.STRING)
 				.valueSerializer(Serializer.INTEGER)
 				.makeOrGet();
//		System.out.println("intMap: " + intMap.toString());
		System.out.println("Loading int map.");
		
		reverseIntMap = db.hashMapCreate("reverseIntMap")
 				.keySerializer(Serializer.INTEGER)
 				.valueSerializer(Serializer.STRING)
 				.makeOrGet();
		System.out.println("Loading reverse int map.");
		
		csMap = db.hashMapCreate("csMap")
 				.keySerializer(Serializer.INTEGER)
 				.valueSerializer(Serializer.LONG_ARRAY)
 				.makeOrGet();
		System.out.println("Loading cs map.");
		/*int totalInCS = 0;
		long one = 0 ;
		for(Integer c : csMap.keySet()){
			totalInCS += csMap.get(c).length;
			one = csMap.get(c)[csMap.get(c).length/5];
			break;
		}
		for(Integer e : ecsLongArrayMap.keySet()){
			long[] dff = ecsLongArrayMap.get(e);
			if(indexOfTriple(dff, one)>=0)
				System.out.println("found");
		}*/
		rucs = db.hashMapCreate("rucsMap")
 				.keySerializer(Serializer.INTEGER)
 				//.valueSerializer(Serializer.)
 				.makeOrGet();
		System.out.println("Loading rucs map.");
		
		ucs = new HashMap<BigCharacteristicSet, Integer>();
		for(Integer ci : rucs.keySet()){
			ucs.put(rucs.get(ci), ci);
		}
		
		ecsIntegerMap = db.hashMapCreate("uecsMap")
 				.valueSerializer(Serializer.INTEGER)
 				//.valueSerializer(Serializer.)
 				.makeOrGet();
		System.out.println("Loading uecs map.");
		
		Map<BigExtendedCharacteristicSet, HashSet<BigExtendedCharacteristicSet>> ecsLinks = db.hashMapCreate("ecsLinks")
 				//.keySerializer(Serializer.INTEGER)	 			
 				.makeOrGet(); 
		System.out.println("Loading ecs links map.");
		
		int tot = 0;
		for(Integer ecs : ruecs.keySet()){
			tot += ecsLongArrayMap.get(ecs).length;
		}
		
		System.out.println("total mapped triples: " + tot);
		tot = 0;
		for(BigExtendedCharacteristicSet e : ecsLinks.keySet()){
			tot += ecsLinks.get(e).size();
		}
		System.out.println("total ecs links: " + tot);
		System.out.println("ECS Links size: " + ecsLinks.size()); 	
		
		Map<Integer, int[]> properIndexMap = db.hashMapCreate("propIndexMap")
 				.keySerializer(Serializer.INTEGER)	
 				.valueSerializer(Serializer.INT_ARRAY)
 				.makeOrGet();  
		System.out.println("Loading property index map.");
		
		propIndexMap = new HashMap<Integer, HashMap<Integer,Integer>>();
		for(Integer e : properIndexMap.keySet()){
			HashMap<Integer, Integer> d = propIndexMap.getOrDefault(e, new HashMap<Integer, Integer>());
			for(int i = 0; i < properIndexMap.get(e).length; i++){				
				if(properIndexMap.get(e)[i] >= 0){
					d.put(i, properIndexMap.get(e)[i]);
				}
			}
			propIndexMap.put(e, d);
		}
		
		propertiesSet = db.hashMapCreate("propertiesSet")
 				.keySerializer(Serializer.STRING)	
 				.valueSerializer(Serializer.INTEGER)
 				.makeOrGet();
		System.out.println("Loading properties set.");
		
		reversePropertiesSet = new HashMap<Integer, String>();
		
		System.out.println(propertiesSet.toString());
	
		
		HashMap<BigExtendedCharacteristicSet, HashSet<Vector<BigExtendedCharacteristicSet>>> ecsVectorMap = new HashMap<BigExtendedCharacteristicSet, HashSet<Vector<BigExtendedCharacteristicSet>>>();
 		
 		HashSet<Vector<BigExtendedCharacteristicSet>> ecsVectors = new HashSet<Vector<BigExtendedCharacteristicSet>>(); 
 		
 		for(BigExtendedCharacteristicSet ecs : ecsLinks.keySet()){
 			
// 			if(true) break;
 			
 			HashSet<BigExtendedCharacteristicSet> visited = new HashSet<BigExtendedCharacteristicSet>();
 			
 			Stack<Vector<BigExtendedCharacteristicSet>> stack = new Stack<>();
 			
 			Vector<BigExtendedCharacteristicSet> v = new Vector<BigExtendedCharacteristicSet>();
 			
 			v.add(ecs);
 			
 			stack.push(v);
 			
 			while(!stack.empty()){
 				
 				v = stack.pop();
 				
 				BigExtendedCharacteristicSet current = v.lastElement();
 				
 				visited.add(current);
 				
 				if(!ecsLinks.containsKey(current)){
 				
 					if(ecsVectorMap.containsKey(current))
 						ecsVectorMap.get(current).add(v);
 					else{
 						HashSet<Vector<BigExtendedCharacteristicSet>> d = new HashSet<Vector<BigExtendedCharacteristicSet>>();
 						d.add(v);
 						ecsVectorMap.put(current, d); 						
 					}
 					ecsVectors.add(v);
 					continue;
 					
 				}
 				
 				for(BigExtendedCharacteristicSet child : ecsLinks.get(current)){
 					if(!visited.contains(child)){
 						Vector<BigExtendedCharacteristicSet> _v = new Vector<BigExtendedCharacteristicSet>();
 						_v.addAll(v);
 						_v.add(child);
 						stack.push(_v);
 					}
 				}
 				
 				
 			}
 		}
 		
 		System.out.println("total patterns: " + ecsVectors.size());
		
		String queryString;
 		Queries lubm = new Queries();
 		ArrayList<Long> times;
 		
 			
 		//args[1] contains the query set to test (0=LUBM, 1=Reactome, 2=Geonames)
 		for(String qs : lubm.getQueries(Integer.parseInt(args[1]))){
 		
 			
 			try{
 			queryString = qs;
 			
 			times = new ArrayList<Long>(); 
 			
 			//Prints the next query string
		 		
 				System.out.println(queryString);
		 		Query q=QueryFactory.create(queryString);
		 		//List<Var> projectVariables = q.getProjectVars();
		 		long tstart = System.nanoTime();
		 		//int totalresults = 0;
		 		long tend ;
		 		 		
		 		BigECSQuery ecsq = new BigECSQuery(q);
		 		
		 		ecsq.findJoins();
		 		//ECSTree queryTree = ecsq.getEcsTree();
		 		
		 		
		 		HashSet<LinkedHashSet<BigExtendedCharacteristicSet>> queryListSet = BigECSQuery.getListSet();
		 		
		 		queryAnswerListSet2 = new HashMap<BigQueryPattern, HashSet<ArrayList<BigECSTuple>>>();
		 		
		 		for(LinkedHashSet<BigExtendedCharacteristicSet> thisQueryList : queryListSet){ 			
		 			  	
		 				ArrayList<BigExtendedCharacteristicSet> qlist = new ArrayList<>(thisQueryList);
		 				
		 				ArrayList<BigECSTuple> qlistTuple = new ArrayList<BigECSTuple>();
		 				for(BigExtendedCharacteristicSet qe : qlist){
		 					//String pr = qe.predicate.getURI();
		 					/*if(pr.contains("swat"))				
		 						pr = pr.replaceAll("#", "##");*/
		 					BigECSTuple nt ;
			 				if(qe.predicate.isURI())
			 					nt = new BigECSTuple(qe, propertiesSet.get(qe.predicate.toString()), getQueryTriplePattern(qe));
			 				else{
			 					TripleAsInt tai = getQueryTriplePattern(qe);
			 					nt = new BigECSTuple(qe, tai.p, tai);
			 				}
		 					//BigECSTuple nt = new BigECSTuple(qe, propertiesSet.get(pr), getQueryTriplePattern(qe));
		 					
		 					nt.subjectBinds = qe.subjectBinds;
		 					nt.objectBinds = qe.objectBinds;
		 					qlistTuple.add(nt);
		 				}
		 				
		 				boolean fl = false;
		 				
		 				for(BigExtendedCharacteristicSet ecs1 : ecsLinks.keySet()){ 			
		 						
		 					visited = new HashSet<BigExtendedCharacteristicSet>();							
		 					if(findDataPatterns(ecs1, ecsLinks, qlistTuple, qlistTuple, 
									new ArrayList<BigECSTuple>())){
		 						fl = true;
							}						
		 			
		 				}
		 		}
		 		System.out.println("size of query patterns " + queryAnswerListSet2.size());
		 		
		 		HashSet<BigExtendedCharacteristicSet> queryECSs = ecsq.ecsSet;
		 		HashMap<BigECSTuple, HashSet<BigECSTuple>> queryECStoData = new HashMap<BigECSTuple, HashSet<BigECSTuple>>();
		 		for(BigExtendedCharacteristicSet qECS : queryECSs){
		 			for(BigExtendedCharacteristicSet ecs : ecsIntegerMap.keySet()){
		 				BigECSTuple qt ;
		 				if(qECS.predicate.isURI())
		 					qt = new BigECSTuple(qECS, propertiesSet.get(qECS.predicate.toString()), getQueryTriplePattern(qECS));
		 				else{
		 					TripleAsInt tai = getQueryTriplePattern(qECS);
		 					qt = new BigECSTuple(qECS, tai.p, tai);
		 				}
		 				BitSet b1 = (BitSet) qt.ecs.subjectCS.longRep.clone();
			 			BitSet b2 = (BitSet) ecs.subjectCS.longRep.clone();
			 			BitSet b3 = (BitSet) qt.ecs.subjectCS.longRep.clone();
			 			b1.and(b2);
			 			if(!b1.equals(b3)) continue;
			 			if(qt.ecs.objectCS != null && ecs.objectCS != null){
			 				BitSet b4 = (BitSet) qt.ecs.objectCS.longRep.clone();
			 				BitSet b5 = (BitSet) ecs.objectCS.longRep.clone();
			 				BitSet b6 = (BitSet) qt.ecs.objectCS.longRep.clone();
			 				b4.and(b5);
			 				if(!b4.equals(b6)) continue;
			 			}
			 					 			
			 			if(qt.ecs.objectCS == null && ecs.objectCS != null) continue;
			 			if(qt.ecs.objectCS != null && ecs.objectCS == null) continue;
			 			
			 			if(!propIndexMap.get(ecsIntegerMap.get(ecs)).containsKey(propertiesSet.get(qt.ecs.predicate.toString())))
			 				continue;
			 			HashSet<BigECSTuple> dset = queryECStoData.getOrDefault(qt, new HashSet<BigECSTuple>());
			 			BigECSTuple ecsT = new BigECSTuple(ecs, propertiesSet.get(qt.ecs.predicate.toString()), getQueryTriplePattern(qt.ecs));
			 			ecsT.subjectBinds = qECS.subjectBinds;
			 			//System.out.println(ecsT.subjectBinds.toString());
			 			dset.add(ecsT);
			 			queryECStoData.put(qt, dset);
		 			}		 			
		 		}
		 		//System.out.println(queryECStoData.toString());
		 		/*ArrayList<BigECSTuple> planList = new ArrayList<BigECSTuple>();
		 		
		 		for(BigECSTuple queryTuple : queryECStoData.keySet()){
		 					 					 			
		 			for(BigECSTuple queryTupleInner : queryECStoData.keySet()){
			 						 		
		 				if(queryTuple==queryTupleInner) continue;
		 				
		 				queryTuple.card += ecsLongArrayMap.get(ecsIntegerMap.get(queryTupleInner.ecs)).length;
		 				if(queryTuple.triplePattern.s == queryTupleInner.triplePattern.s){
		 					
		 				}
			 			
			 		}
		 			
		 			planList.add(queryTuple);
		 			
		 		}
		 		Collections.sort(planList, new BigECSTupleComparator2());
		 		
		 		BigECSTuple qt = planList.get(0);
		 		
		 		planList.remove(qt);
		 		
		 		
		 		
		 		for(BigECSTuple dt : queryECStoData.get(qt)){
		 			
		 		}*/
		 		
	
		 		
		 		varIndexMap = new HashMap<Node, Integer>();
		 		int varIndex = 0;
		 		
		 		for(BigQueryPattern key : queryAnswerListSet2.keySet()){
		 			for(ArrayList<BigECSTuple> dataPattern : queryAnswerListSet2.get(key)){
		 				for(BigECSTuple BigECSTuple : dataPattern){
		 					TripleAsInt tai = BigECSTuple.triplePattern;
		 					/*if(!outerBindings.containsKey(reverseVarMap.get(tai.s)))
		 	 					outerBindings.put(reverseVarMap.get(tai.s), new HashSet<Long>());
		 						if(!outerBindings.containsKey(reverseVarMap.get(tai.o)))
		 							outerBindings.put(reverseVarMap.get(tai.o), new HashSet<Long>());*/
		 	 				if(!varIndexMap.containsKey(reverseVarMap.get(tai.s)))
		 	 					varIndexMap.put(reverseVarMap.get(tai.s), varIndex++);
		 	 				if(!varIndexMap.containsKey(reverseVarMap.get(tai.o)))
		 	 					varIndexMap.put(reverseVarMap.get(tai.o), varIndex++);
		 				}
		 			} 			
		 		}
		 		HashMap<ArrayList<BigExtendedCharacteristicSet>, HashSet<Integer>> qpVarMap = new HashMap<ArrayList<BigExtendedCharacteristicSet>, HashSet<Integer>>();
		 		HashMap<ArrayList<BigECSTuple>, HashSet<Integer>> qpVarMap2 = new HashMap<ArrayList<BigECSTuple>, HashSet<Integer>>();
		 		//HashMap<ArrayList<BigExtendedCharacteristicSet>, HashSet<ArrayList<BigExtendedCharacteristicSet>>> skipping = new HashMap<ArrayList<BigExtendedCharacteristicSet>, HashSet<ArrayList<BigExtendedCharacteristicSet>>>();
		 		HashMap<BigQueryPattern, HashSet<ArrayList<BigExtendedCharacteristicSet>>> skipping = new HashMap<BigQueryPattern, HashSet<ArrayList<BigExtendedCharacteristicSet>>>();
		 		HashMap<BigQueryPattern, HashSet<ArrayList<BigECSTuple>>> skipping2 = new HashMap<BigQueryPattern, HashSet<ArrayList<BigECSTuple>>>();
		 		for(BigQueryPattern BigQueryPattern : queryAnswerListSet2.keySet()){
		 			int count = 0;
		 			
		 			skipping.put(BigQueryPattern, new HashSet<ArrayList<BigExtendedCharacteristicSet>>());
		 			skipping2.put(BigQueryPattern, new HashSet<ArrayList<BigECSTuple>>());
		 			for(ArrayList<BigECSTuple> dataPattern : queryAnswerListSet2.get(BigQueryPattern)){
		 				if(dataPattern.size() > 1){
		 					//System.out.println("data pattern: " + dataPattern.toString());
		 	 				boolean cont = false;
		 	 				ArrayList<BigExtendedCharacteristicSet> dataPatECS = new ArrayList<BigExtendedCharacteristicSet>();
		 	 				for(BigECSTuple et : dataPattern)
		 	 					dataPatECS.add(et.ecs);
		 	 				for(Vector<BigExtendedCharacteristicSet> vector : ecsVectors){
		 	 					if(vector.containsAll(dataPatECS)){
		 	 						cont = true;
		 	 						break;
		 	 					}
		 	 				}
		 	 				if(!cont){
		 	 					count++;
		 	 					skipping.get(BigQueryPattern).add(dataPatECS);
		 	 					skipping2.get(BigQueryPattern).add(dataPattern);
		 	 				}
		 				}
		 				 					 				
		 				HashSet<Integer> vars = new HashSet<Integer>();
		 				for(BigECSTuple et : dataPattern){
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
		 				qpVarMap2.put(BigQueryPattern.queryPattern, vars);
		 				
		 			}
		 			
		 			System.out.println("not contains all: " + count);
		 			System.out.println("from total: " + queryAnswerListSet2.get(BigQueryPattern).size());
		 			
		 		}
		 		for(int i = 0; i < 10; i++){
			 		ArrayList<BigQueryPattern> f = new ArrayList<>(queryAnswerListSet2.keySet());
			 		
			 		Collections.sort(f, new BigECSTupleComparator());
			 		
			 		HashMap<BigQueryPattern, HashMap<BigECSTuple, BigAnswerPattern>> qans = new HashMap<BigQueryPattern, HashMap<BigECSTuple,BigAnswerPattern>>();
			 		
			 		HashMap<BigQueryPattern, HashMap<BigECSTuple, BigAnswerPattern>> qansReverse = new HashMap<BigQueryPattern, HashMap<BigECSTuple,BigAnswerPattern>>();
			 		
			 		for(BigQueryPattern BigQueryPattern : f){
			 			
			 			qans.put(BigQueryPattern, new HashMap<BigECSTuple, BigAnswerPattern>());
			 			
			 			qansReverse.put(BigQueryPattern, new HashMap<BigECSTuple, BigAnswerPattern>());
			 			
			 			if(BigQueryPattern.queryPattern.size() == 1 )
			 				{
			 				
			 				for(ArrayList<BigECSTuple> dataPattern1 : queryAnswerListSet2.get(BigQueryPattern)){
			 					BigAnswerPattern ans = new BigAnswerPattern(dataPattern1.get(0), BigQueryPattern);
			 					if(qans.get(BigQueryPattern).containsKey(ans.root)){
			 						ans = qans.get(BigQueryPattern).get(ans.root); 						
			 					} 
			 					qans.get(BigQueryPattern).put(ans.root, ans);
			 					qansReverse.get(BigQueryPattern).put(ans.root, ans); 		
			 				
			 				}
			 			
			 				continue;
			 			}
			 			 			
			 			
			 			for(ArrayList<BigECSTuple> dataPattern1 : queryAnswerListSet2.get(BigQueryPattern)){
			 			 				 				
			 				for(int i1 = 0 ; i1 < dataPattern1.size()-1; i1++){
			 					
			 					BigAnswerPattern ans = new BigAnswerPattern(dataPattern1.get(i1), BigQueryPattern);
			 					if(qans.get(BigQueryPattern).containsKey(ans.root)){
			 						ans = qans.get(BigQueryPattern).get(ans.root);
			 					} 					
			 					BigAnswerPattern nextAns = new BigAnswerPattern(dataPattern1.get(i1+1), BigQueryPattern);
			 	 				if(qans.get(BigQueryPattern).containsKey(nextAns.root)){
			 	 					nextAns = qans.get(BigQueryPattern).get(nextAns.root);
			 	 				}
			 	 				ans.addChild(nextAns); 					
			 					
			 					qans.get(BigQueryPattern).put(ans.root, ans); 	
			 					//qans.get(BigQueryPattern).put(nextAns.root, nextAns);
			 					
			 				}
			 				for(int i1 = dataPattern1.size()-1 ; i1 >= 1; i1--){
			 					
			 					BigAnswerPattern ans = new BigAnswerPattern(dataPattern1.get(i1), BigQueryPattern);
			 					if(qansReverse.get(BigQueryPattern).containsKey(ans.root)){
			 						ans = qansReverse.get(BigQueryPattern).get(ans.root);
			 					} 					
			 					BigAnswerPattern nextAns = new BigAnswerPattern(dataPattern1.get(i1-1), BigQueryPattern);
			 	 				if(qansReverse.get(BigQueryPattern).containsKey(nextAns.root)){
			 	 					nextAns = qansReverse.get(BigQueryPattern).get(nextAns.root);
			 	 				}
			 	 				ans.addChild(nextAns); 					
			 					
			 	 				qansReverse.get(BigQueryPattern).put(ans.root, ans); 	
			 					//qans.get(BigQueryPattern).put(nextAns.root, nextAns);
			 					
			 				}
			 			
			 			}
			 			
			 		
			 		}
			 		
			 		HashMap<BigQueryPattern, HashSet<BigQueryPattern>> joinedBigQueryPatterns = new HashMap<BigQueryPattern, HashSet<BigQueryPattern>>();
			 		HashMap<BigQueryPattern, HashSet<ArrayList<Integer>>> commonVarsMap = new HashMap<BigQueryPattern, HashSet<ArrayList<Integer>>>();
			 		for(BigQueryPattern BigQueryPatterns1 : f){
			 			commonVarsMap.put(BigQueryPatterns1, new HashSet<ArrayList<Integer>>());
			 			joinedBigQueryPatterns.put(BigQueryPatterns1, new HashSet<BigQueryPattern>());
			 		}
			 		for(BigQueryPattern BigQueryPatterns1 : f){
			 			for(BigQueryPattern BigQueryPatterns2 : f){
			 				if(BigQueryPatterns1 == BigQueryPatterns2) continue;
			 				
			 				HashSet<Integer> vars = qpVarMap2.get(BigQueryPatterns1.queryPattern);
			 	 			HashSet<Integer> previousVars = qpVarMap2.get(BigQueryPatterns2.queryPattern); 			
			 	 			ArrayList<Integer> commonVars = new ArrayList<Integer>(); 			
			 	 			for(Integer var : vars){
			 					if(previousVars.contains(var)){
			 						commonVars.add(var);
			 					} 					
			 	 			}
			 	 			if(commonVars.isEmpty()) continue;
			 	 			commonVarsMap.get(BigQueryPatterns1).add(commonVars);
			 	 			commonVarsMap.get(BigQueryPatterns2).add(commonVars);
			 	 			joinedBigQueryPatterns.get(BigQueryPatterns1).add(BigQueryPatterns2);
			 	 			joinedBigQueryPatterns.get(BigQueryPatterns2).add(BigQueryPatterns1);
			 			}
			 			
			 		}
			 		
			 		HashMap<Long, Vector<Integer>> previous_res_vectors = null;
			 		
			 		HashMap<Long, ArrayList<Vector<Integer>>> previous_res_vectors_new = new HashMap<Long, ArrayList<Vector<Integer>>>();
			 		
			 		tstart = System.nanoTime();
			 		
			 		Map<BigQueryPattern, HashMap<Integer, Integer>> qpVarIndexMap = new HashMap<BigQueryPattern, HashMap<Integer,Integer>>();
			 		 		
				 	for(int qi = 0; qi < f.size(); qi++){
				 	
				 		BigQueryPattern qp = f.get(qi);
				 	
				 		int nextIndex = 0;
				 		HashMap<Integer, Integer> varIndexes = new HashMap<Integer, Integer>();
				 		for(BigECSTuple nextECSPattern : qp.queryPattern){
				 			
				 			if(!varIndexes.containsKey(nextECSPattern.triplePattern.s))
				 				varIndexes.put(nextECSPattern.triplePattern.s, nextIndex++);
				 			varIndexes.put(nextECSPattern.triplePattern.o, nextIndex++);	 				
				 			
				 		}
				 		qpVarIndexMap.put(qp, varIndexes);
				 		//System.out.println(varIndexes.toString());
				 	
			 		}
			 		//HashMap<String, Vector>
			 		for(int qi = 0; qi < f.size(); qi++){
			 			
			 			BigQueryPattern qp = f.get(qi);
			 			
			 			tot = 0;
			 			
			 			HashMap<Long, Vector<Integer>> res_vectors = new HashMap<Long, Vector<Integer>>();	
			 			
			 			HashMap<Integer, Integer> varIndexes = qpVarIndexMap.get(qp);
			 			List<Integer> indexesOfCommonVarsToHash = new ArrayList<Integer>();
			 			
			 			if(qi < f.size()-1){
			 				BigQueryPattern nextQp = f.get(qi+1);
			 				HashMap<Integer, Integer> nextVarIndexes = qpVarIndexMap.get(nextQp);
			 				
			 				for(Integer nextVarIndex : varIndexes.keySet()){
			 					if(nextVarIndexes.containsKey(nextVarIndex)){
			 						indexesOfCommonVarsToHash.add(varIndexes.get(nextVarIndex));
			 					}
			 				}
			 			}
			 		
			 			List<Integer> indexesOfCommonVarsToProbe = new ArrayList<Integer>();
			 			
			 			if(qi > 0){
			 				BigQueryPattern previousQp = f.get(qi-1);
			 				HashMap<Integer, Integer> previousVarIndexes = qpVarIndexMap.get(previousQp);
			 				
			 				for(Integer nextVarIndex : varIndexes.keySet()){
			 					if(previousVarIndexes.containsKey(nextVarIndex)){
			 						indexesOfCommonVarsToProbe.add(varIndexes.get(nextVarIndex));
			 					}
			 				}
			 			}
			 			//System.out.println("indexes of common vars to hash: " + indexesOfCommonVarsToHash.toString());
			 			//System.out.println("indexes of common vars to probe: " + indexesOfCommonVarsToProbe.toString());
			 			HashSet<Vector<Integer>> results = null ;
			 			HashMap<Long, ArrayList<Vector<Integer>>> res_vectors_new = new HashMap<Long, ArrayList<Vector<Integer>>>();	
			 			if(qp.queryPattern.size() > 1)
			 			for(int k = 0; k < qp.queryPattern.size()-1; k++){
			 				results = new HashSet<Vector<Integer>>();
			 				boolean isLast = false;
			 				if((k+1) == qp.queryPattern.size()-1) isLast = true;
			 				BigECSTuple leftQueryECS = qp.queryPattern.get(k);
			 				BigECSTuple rightQueryECS = qp.queryPattern.get(k+1);
			 				HashSet<BigECSTuple> leftDataSet = queryECStoData.get(leftQueryECS);
			 				HashSet<BigECSTuple> rightDataSet = queryECStoData.get(rightQueryECS);
			 				
			 				if(previous_res_vectors_new != null){
			 					res_vectors_new = previous_res_vectors_new;
			 					
			 					joinTwoECSNew(res_vectors_new, leftDataSet, rightDataSet, indexesOfCommonVarsToHash, results, 
				 						indexesOfCommonVarsToProbe, isLast );
			 					
			 				}
			 				/*if(isLast){
			 					
			 					res_vectors_new = null;
			 				}*/
			 					
			 				/*if(indexesOfCommonVarsToHash.size() == 2){
			 					for(Vector<Integer> r : results){
			 						res_vectors.put(szudzik(r.get(indexesOfCommonVarsToHash.get(0)), r.get(indexesOfCommonVarsToHash.get(1))), r);
			 					}
			 				}
			 				else if(indexesOfCommonVarsToHash.size() == 1){
			 					for(Vector<Integer> r : results){
			 						res_vectors.put((long)r.get(indexesOfCommonVarsToHash.get(0)), r);
			 					}
			 				}*/
			 				//System.out.println(k+" : " + res_vectors_new.size());
			 				
			 			}
			 			else{
			 				results = new HashSet<Vector<Integer>>();
			 				BigECSTuple leftQueryECS = qp.queryPattern.get(0);
			 				HashSet<BigECSTuple> leftDataSet = queryECStoData.get(leftQueryECS);
			 				if(previous_res_vectors_new != null){
			 					res_vectors_new = previous_res_vectors_new;
			 					
			 					joinOneECSNew(res_vectors_new, leftDataSet, indexesOfCommonVarsToHash, indexesOfCommonVarsToProbe, results);
			 					//System.out.println(res_vectors_new.size());
			 				}
			 				
			 				/*if(indexesOfCommonVarsToHash.size() == 2){
			 					for(Vector<Integer> r : results){
			 						res_vectors.put(szudzik(r.get(indexesOfCommonVarsToHash.get(0)), r.get(indexesOfCommonVarsToHash.get(1))), r);
			 					}
			 				}
			 				else if(indexesOfCommonVarsToHash.size() == 1){
			 					for(Vector<Integer> r : results){
			 						res_vectors.put((long)r.get(indexesOfCommonVarsToHash.get(0)), r);
			 					}
			 				}*/
			 			}
			 			//HashSet<Vector<Integer>> result
			 			/*results.clear();
			 			for(Integer what : res_vectors_new.keySet()){
			 				results.addAll(res_vectors_new.get(what));
			 			}*/
			 			System.out.println("result size: " + results.size());
			 			/*for(Vector<Integer> v : results){
			 				System.out.println(v.toString());
			 			}*/
			 			previous_res_vectors_new = res_vectors_new;
			 			if(true)continue;
			 			for(ArrayList<BigECSTuple> next : queryAnswerListSet2.get(qp)){
			 				
			 				if(next.size() == 1){ 
			 					
			 					if(previous_res_vectors == null)
			 						joinTwoECS(res_vectors, next.get(0), null, indexesOfCommonVarsToHash);
			 					else
			 						joinTwoECS(res_vectors, previous_res_vectors, next.get(0), null,indexesOfCommonVarsToHash, indexesOfCommonVarsToProbe);
			 				}
			 				else
			 				
			 				for(int ind = 0; ind < next.size()-1; ind++){
			 					
			 					if(previous_res_vectors == null)
			 						joinTwoECS(res_vectors, next.get(ind),next.get(ind+1), indexesOfCommonVarsToHash);
			 					else
			 						joinTwoECS(res_vectors, previous_res_vectors, next.get(ind),next.get(ind+1),indexesOfCommonVarsToHash, indexesOfCommonVarsToProbe);
			 					
			 				}
			 				
			 			}
			 			
			 			previous_res_vectors = res_vectors; 			
			 		}
			 		tend = System.nanoTime();
			 		
			 		if(previous_res_vectors != null)
			 			System.out.println("join " + previous_res_vectors.size() + "\t: " + (tend-tstart));
			 		else
			 			System.out.println("join 0 \t: " + (tend-tstart));
			 		times.add((tend-tstart));
					
			 		
			 		}
		 		Collections.sort(times);
		 		
		 		System.out.println("best time: " + times.get(0));
 			}
 	 		catch(Exception e){e.printStackTrace();}
 			//if(true) break;
 		}
 		 		
 		db.close();
	}
	
	public static TripleAsInt getQueryTriplePattern(BigExtendedCharacteristicSet queryLinks){
		int s = -1, p = -1, o = -1;
		if(!queryLinks.subject.isVariable()){
			if(intMap.containsKey(queryLinks.subject.getURI()))
				s = intMap.get(queryLinks.subject.getURI());			
		}
		else{
			if(!varMap.containsKey(queryLinks.subject)){
				reverseVarMap.put(nextVar, queryLinks.subject);
				varMap.put(queryLinks.subject, nextVar--);
			}
			s = varMap.get(queryLinks.subject);
		}
		if(!queryLinks.predicate.isVariable()){
			if(intMap.containsKey(queryLinks.predicate.getURI()))
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
			if(intMap.containsKey(queryLinks.object.toString()))
				o = intMap.get(queryLinks.object.toString());			
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
	
	
	static public boolean findDataPatterns(BigExtendedCharacteristicSet ecs, 
			Map<BigExtendedCharacteristicSet, HashSet<BigExtendedCharacteristicSet>> links, 
			ArrayList<BigECSTuple> queryLinks, 
			ArrayList<BigECSTuple> originalQueryLinks, 
			ArrayList<BigECSTuple> list){
						
		if(queryLinks.size() == 0) {
			if(queryAnswerListSet2.containsKey(new BigQueryPattern(originalQueryLinks)))
				queryAnswerListSet2.get(new BigQueryPattern(originalQueryLinks)).add(list);			
			else{
				HashSet<ArrayList<BigECSTuple>> d = new HashSet<ArrayList<BigECSTuple>>();
				d.add(list);
				queryAnswerListSet2.put(new BigQueryPattern(originalQueryLinks),d);
			}
			return true;
		}
		
		BitSet b1 = (BitSet) queryLinks.get(0).ecs.subjectCS.longRep.clone();
		BitSet b2 = (BitSet) ecs.subjectCS.longRep.clone();
		BitSet b3 = (BitSet) queryLinks.get(0).ecs.subjectCS.longRep.clone();
		b1.and(b2);
		if(!b1.equals(b3)) return false;
		if(queryLinks.get(0).ecs.objectCS != null && ecs.objectCS != null){
			BitSet b4 = (BitSet) queryLinks.get(0).ecs.objectCS.longRep.clone();
			BitSet b5 = (BitSet) ecs.objectCS.longRep.clone();
			BitSet b6 = (BitSet)  queryLinks.get(0).ecs.objectCS.longRep.clone();
			b4.and(b5);
			if(!b4.equals(b6)) return false;
		}
		
		/*if((queryLinks.get(0).ecs.subjectCS.longRep & ecs.subjectCS.longRep) != queryLinks.get(0).ecs.subjectCS.longRep){
			
			return false;
		}
		if(queryLinks.get(0).ecs.objectCS != null && ecs.objectCS != null)
			if((queryLinks.get(0).ecs.objectCS.longRep & ecs.objectCS.longRep) != queryLinks.get(0).ecs.objectCS.longRep){
				
				return false;
			}*/
		if(queryLinks.get(0).ecs.objectCS == null && ecs.objectCS != null) return false;
		if(queryLinks.get(0).ecs.objectCS != null && ecs.objectCS == null) return false;
		
		if(!propIndexMap.get(ecsIntegerMap.get(ecs)).containsKey(propertiesSet.get(queryLinks.get(0).ecs.predicate.toString())))
			return false;
		
		if(visited.contains(ecs)){
			if(queryAnswerListSet2.containsKey(new BigQueryPattern(originalQueryLinks)))
				queryAnswerListSet2.get(new BigQueryPattern(originalQueryLinks)).add(list);			
			else{
				HashSet<ArrayList<BigECSTuple>> d = new HashSet<ArrayList<BigECSTuple>>();
				d.add(list);
				queryAnswerListSet2.put(new BigQueryPattern(originalQueryLinks),d);
			}			
		}
		visited.add(ecs);
		
		BigECSTuple BigECSTuple = new BigECSTuple(ecs, propertiesSet.get(queryLinks.get(0).ecs.predicate.toString()), getQueryTriplePattern(queryLinks.get(0).ecs));
		BigECSTuple.subjectBinds = queryLinks.get(0).ecs.subjectBinds;
		BigECSTuple.objectBinds = queryLinks.get(0).ecs.objectBinds;
		
		list.add(BigECSTuple);
		
		if(!links.containsKey(ecs)){
				
			if(queryAnswerListSet2.containsKey(new BigQueryPattern(originalQueryLinks)))
				queryAnswerListSet2.get(new BigQueryPattern(originalQueryLinks)).add(list);			
			else{
				HashSet<ArrayList<BigECSTuple>> d = new HashSet<ArrayList<BigECSTuple>>();
				d.add(list);
				queryAnswerListSet2.put(new BigQueryPattern(originalQueryLinks),d);
			}		
			
			return true;
		}
		else{			
			for(BigExtendedCharacteristicSet child : links.get(ecs)){				
				
					if(queryLinks.size()>1){
						
						ArrayList<BigECSTuple> dummy = new ArrayList<BigECSTuple>();
						dummy.addAll(list);
						
						findDataPatterns(child, links, new ArrayList<>(queryLinks.subList(1, queryLinks.size())), originalQueryLinks, dummy);
					}
					else {
						if(queryAnswerListSet2.containsKey(new BigQueryPattern(originalQueryLinks)))
							queryAnswerListSet2.get(new BigQueryPattern(originalQueryLinks)).add(list);			
						else{
							HashSet<ArrayList<BigECSTuple>> d = new HashSet<ArrayList<BigECSTuple>>();
							d.add(list);
							queryAnswerListSet2.put(new BigQueryPattern(originalQueryLinks),d);
						}					
						
					}			
			}
		}
		
		return false;
									
	}
	
	
	
	public static HashMap<Long, Vector<Integer>> joinTwoECS(HashMap<Long, Vector<Integer>> res,
			HashMap<Long, Vector<Integer>> previous_res,
			BigECSTuple e1, BigECSTuple e2, 
			List<Integer> hashIndexes,
			List<Integer> probeIndexes
			){

						
				if(e2 == null){
					long[] checkArr = csMap.get(ucs.get(e1.ecs.subjectCS));					
					int p1 = propIndexMap.get(ecsIntegerMap.get(e1.ecs)).get(e1.property);
					long[] e1_array = ecsLongArrayMap.get(ecsIntegerMap.get(e1.ecs));
					//System.out.println(Arrays.toString(e1_array));
					for(int i = p1; i < e1_array.length; i++){				
					
						long t = e1_array[i];
						if((int)((t >> 54)  & 0x3ff) != e1.property)
							break;
						if(!checkBinds(e1, (int)(t >> 27 & 0x7FFFFFF), checkArr))
							continue;
										
						Vector<Integer> v = new Vector<Integer>();
						v.add((int)((t >> 27) & 0x7FFFFFF));				
						v.add((int)((t & 0x7FFFFFF)));				
						//res.add(v);
						long hash = szudzik(v.get(probeIndexes.get(0)), v.get(probeIndexes.get(1)));
						if(previous_res.containsKey(hash)){
							if(!hashIndexes.isEmpty())
								res.put(szudzik(v.get(hashIndexes.get(0)), v.get(hashIndexes.get(1))), v);
							else
								res.put(hash, v);
						}
					}			
				}
				else{
				
					HashMap<Integer, ArrayList<Vector<Integer>>> h = new HashMap<Integer, ArrayList<Vector<Integer>>>();			
					
					//int p1 = propIndexMap.get(e1.ecs).get(e1.property);
					int p1 = propIndexMap.get(ecsIntegerMap.get(e1.ecs)).get(e1.property);
					long[] e1_array = ecsLongArrayMap.get(ecsIntegerMap.get(e1.ecs));
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
					long[] e2_array = ecsLongArrayMap.get(ecsIntegerMap.get(e2.ecs));
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
								if(hashIndexes.size() > 0)
									res.put(szudzik(nv.get(hashIndexes.get(0)), nv.get(hashIndexes.get(1))), nv);
								else
									res.put(szudzik(nv.firstElement(), nv.lastElement()), nv);
							}
						
						}
					
					}						
				}
			
			return res;
			}
	
	public static HashMap<Long, ArrayList<Vector<Integer>>> joinTwoECSNew(HashMap<Long, ArrayList<Vector<Integer>>> res,			
			HashSet<BigECSTuple> e1set, HashSet<BigECSTuple> e2set, 
			List<Integer> hashIndexes,
			HashSet<Vector<Integer>> results,
			List<Integer> probeIndexes,
			boolean isLast
			//,HashMap<String, Vector<Integer>> outerJoin
			
			){

			
				HashMap<Integer, ArrayList<Vector<Integer>>> h = new HashMap<Integer, ArrayList<Vector<Integer>>>();
				
					for(BigECSTuple e1 : e1set){						
																		
						int p1 = propIndexMap.get(ecsIntegerMap.get(e1.ecs)).get(e1.property);
						long[] e1_array = ecsLongArrayMap.get(ecsIntegerMap.get(e1.ecs));
						//System.out.println("e1: " + e1_array.length);
						/*long[] checkArr = csMap.get(ucs.get(e1.ecs.subjectCS));	
						System.out.println(checkArr.length);*/
						for(int i = p1; i < e1_array.length; i++){
											
							long t = e1_array[i];
							
							if((int)((t >> 54)  & 0x3ff) != e1.property && e1.property >= 0) 
								break;			
							
							/*if(!checkBinds(e1, (int)(t >> 27 & 0x7FFFFFF), checkArr))
								continue;*/
							if(!res.isEmpty()){
								
								if(!res.containsKey((int)((t & 0x7FFFFFF)))){
									//System.out.println("ASdasd");
									//TODO
									//an erxetai apo previous res, tote den prepei na koitaksei to object!!!
									continue;
								}
								else{
									ArrayList<Vector<Integer>> l = res.get((int)((t & 0x7FFFFFF)));
									//for(Vector<Integer> nv : l){
									ArrayList<Vector<Integer>> vg = new ArrayList<Vector<Integer>>();
									for(int j = 0; j < l.size(); j++){
										//Vector<Integer> v = new Vector<Integer>(nv);
										Vector<Integer> nv = new Vector<Integer>(l.get(j));
										//System.out.println(nv.size());
										nv.add((int)((t & 0x7FFFFFF)));
										//res.getOrDefault(v.lastElement(), new ArrayList<Vector<Integer>>());
										vg.add(nv);
										
									}
									h.put((int)((t & 0x7FFFFFF)), vg);
									
								}
							}
							else{
								Vector<Integer> v = new Vector<Integer>();
								v.add((int)((t >> 27) & 0x7FFFFFF));
								v.add((int)((t & 0x7FFFFFF)));
								ArrayList<Vector<Integer>> l = h.getOrDefault((int)((t & 0x7FFFFFF)), new ArrayList<Vector<Integer>>());
								l.add(v);
								h.put((int)((t & 0x7FFFFFF)), l);
							}
							
						}	
						
					}
					
					/*if(isLast){
						HashSet<Integer> sizes = new HashSet<Integer>();
						for(Integer r : res.keySet()){
							for(Vector<Integer> vv : res.get(r))
								sizes.add(vv.size());
						}
						System.out.println("mesh" + sizes.toString());
					}*/
					//res.clear();
					for(BigECSTuple e2 : e2set){
						int p2 = propIndexMap.get(ecsIntegerMap.get(e2.ecs)).get(e2.property);
						long[] e2_array = ecsLongArrayMap.get(ecsIntegerMap.get(e2.ecs));
						//System.out.println("e2: " + e2_array.length);
						for(int i = p2; i < e2_array.length; i++){				
						
							long t = e2_array[i];
							if((int)((t >> 54)  & 0x3ff) != e2.property) 
							break;
							
							if(h.containsKey((int)((t >> 27) & 0x7FFFFFF))){
							
								ArrayList<Vector<Integer>> l = h.get((int)((t >> 27) & 0x7FFFFFF));					
								for(Vector<Integer> v : l){
								
									Vector<Integer> nv = new Vector<Integer>(v);
									nv.add((int)((t & 0x7FFFFFF)));
									
									
									if(!isLast){
										ArrayList<Vector<Integer>> vg = res.getOrDefault(nv.lastElement(), 
												new ArrayList<Vector<Integer>>());
																			
										vg.add(nv);
										res.put((long)nv.lastElement(), vg);
									}
									else{
										//TODO
										//edw prepei na doume poia hasharoume an yparxei epomeno query pattern, alliws 
										//kateftheian sta results
										if(hashIndexes.size() == 0)
											results.add(nv);
										else{
											if(hashIndexes.size()==1){
												ArrayList<Vector<Integer>> vg = res.getOrDefault(nv.get(hashIndexes.get(0)), 
														new ArrayList<Vector<Integer>>());
																					
												vg.add(nv);
												res.put((long)nv.get(hashIndexes.get(0)), vg);
											}
											else if(hashIndexes.size()==2){
												//System.out.println("Dfsdfsdf");
												ArrayList<Vector<Integer>> vg = res.getOrDefault(szudzik(nv.get(hashIndexes.get(0)), nv.get(hashIndexes.get(1))), 
														new ArrayList<Vector<Integer>>());
												vg.add(nv);
												res.put( szudzik(nv.get(hashIndexes.get(0)), nv.get(hashIndexes.get(1))), vg);
											} 
										}
										
									}
								
								}
							
							}
						
						}			
					}					
					//System.out.println("ffff " + res.size());
			return res;
			}
	
	public static HashMap<Long, ArrayList<Vector<Integer>>> joinOneECSNew(HashMap<Long, ArrayList<Vector<Integer>>> res,			
			HashSet<BigECSTuple> e1set,  	
			List<Integer> hashIndexes,
			List<Integer> probeIndexes, HashSet<Vector<Integer>> results
			
			){

					int s, o;
					long probe;
					for(BigECSTuple e1 : e1set){
						int p1 = propIndexMap.get(ecsIntegerMap.get(e1.ecs)).get(e1.property);
						long[] e1_array = ecsLongArrayMap.get(ecsIntegerMap.get(e1.ecs));
						//System.out.println("e1: " + e1_array.length);
						long[] checkArr = csMap.get(ucs.get(e1.ecs.subjectCS));	
						//System.out.println(checkArr.length);
						for(int i = p1; i < e1_array.length; i++){
							
							long t = e1_array[i];				
							if((int)((t >> 54)  & 0x3ff) != e1.property) 
								break;
							
							s = (int)((t >> 27) & 0x7FFFFFF);
							if(!checkBinds(e1, s, checkArr))
								continue;
							o = (int)((t & 0x7FFFFFF));							
							if(probeIndexes.size() > 0){
								//System.out.println(probeIndexes.toString());
								//szudzik((int)((t >> 27) & 0x7FFFFFF), (int)((t & 0x7FFFFFF)))
								probe = -1l;
								if(probeIndexes.size() == 1){
									if(probeIndexes.get(0)==0)
										probe = s;
									else 
										probe = o;
								}
								else{
									probe = szudzik(s,o);
								}
								if(!res.containsKey(probe)){
									//System.out.println(res.toString());
									//System.out.println(szudzik((int)((t >> 27) & 0x7FFFFFF), (int)((t & 0x7FFFFFF))));
									continue;
								}
								for(Vector<Integer> vv : res.get(probe)){
									//vv.add((int)((t >> 27) & 0x7FFFFFF));
									//vv.add((int)((t & 0x7FFFFFF)));							
									//if(probeIndexes.size() == 0)
									//System.out.println(vv.toString());
									results.add(vv);
								}
								//System.out.println("-----");
								continue;
							}
							else{
								Vector<Integer> v = new Vector<Integer>();
								v.add(s);
								v.add(o);
								//v.add();
								//v.add((int)((t & 0x7FFFFFF)));							
								//if(probeIndexes.size() == 0)
								results.add(v);
							}
							
						}							
					}
										
					
			return res;
			}
	
	public static HashMap<Long, Vector<Integer>> joinTwoECS(HashMap<Long, Vector<Integer>> res,			
			BigECSTuple e1, BigECSTuple e2, 
			List<Integer> hashIndexes
			
			){

			
				if(e2 == null){
				
					int p1 = propIndexMap.get(ecsIntegerMap.get(e1.ecs)).get(e1.property);
					long[] e1_array = ecsLongArrayMap.get(ecsIntegerMap.get(e1.ecs));
					long[] checkArr = csMap.get(ucs.get(e1.ecs.subjectCS));		
					
					for(int i = p1; i < e1_array.length; i++){
						
						long t = e1_array[i];
						if((int)((t >> 54)  & 0x3ff) != e1.property)
							break;
						if(!checkBinds(e1, (int)(t >> 27 & 0x7FFFFFF), checkArr))
							continue;
						Vector<Integer> v = new Vector<Integer>();
						v.add((int)((t >> 27) & 0x7FFFFFF));
						v.add((int)((t & 0x7FFFFFF)));
						//res.add(v);
						if(!hashIndexes.isEmpty())
							res.put(szudzik(v.get(hashIndexes.get(0)), v.get(hashIndexes.get(1))), v);
						else
							res.put(szudzik(v.firstElement(), v.lastElement()), v);
					}			
				}
				else{
				
					HashMap<Integer, ArrayList<Vector<Integer>>> h = new HashMap<Integer, ArrayList<Vector<Integer>>>();			
					
					int p1 = propIndexMap.get(ecsIntegerMap.get(e1.ecs)).get(e1.property);
					long[] e1_array = ecsLongArrayMap.get(ecsIntegerMap.get(e1.ecs));
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
					long[] e2_array = ecsLongArrayMap.get(ecsIntegerMap.get(e2.ecs));
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
								if(hashIndexes.size() == 2)
									res.put(szudzik(nv.get(hashIndexes.get(0)), nv.get(hashIndexes.get(1))), nv);
								/*else if(hashIndexes.size() == 1)
									res.put(szudzik(nv.get(hashIndexes.get(0)), nv.get(hashIndexes.get(1))), nv);*/
								else
									res.put((long)nv.lastElement(), nv);
							
							}
						
						}
					
					}						
				}
			
			
			return res;
			}
	
	public static long szudzik(int a, int b){
		
		return a >= b ? a * a + a + b : a + b * b;
		
	}
	
	public static boolean checkBinds(BigECSTuple tuple, int subject, long[] array){
							
		if(tuple.subjectBinds != null){
			
			//System.out.println("asdasdasd");
			for(Integer prop : tuple.subjectBinds.keySet()){
				//System.out.println("sdfsdfs");
				int obj = tuple.subjectBinds.get(prop);
				long tripleSPOLong = ((long)prop << 54 | 
						(long)(subject & 0x7FFFFFF) << 27 | 
						(long)(obj & 0x7FFFFFF));				
				if(indexOfTriple(array, tripleSPOLong) < 0) {						
					//System.out.println(tripleSPOLong);
					//System.out.println(Arrays.toString(array));
					return false;
				}
				//System.out.println(reverseIntMap.get(subject) + ", " + reverseIntMap.get(obj));
			}

		}
		return true;
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
	static class BigECSTupleComparator implements Comparator<BigQueryPattern>
	{
	    public int compare(BigQueryPattern c1, BigQueryPattern c2)
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
	
	static class BigECSTupleComparator2 implements Comparator<BigECSTuple>
	{
	    public int compare(BigECSTuple c1, BigECSTuple c2)
	    {
	   	
	   		 return (new Integer(c1.card)).compareTo(c2.card);
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
	
}




