package com.athena.imis.tests;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.jena.sparql.core.Var;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import com.athena.imis.models.AnswerPattern;
import com.athena.imis.models.CharacteristicSet;
import com.athena.imis.models.ECSQuery;
import com.athena.imis.models.ECSTuple;
import com.athena.imis.models.ExtendedCharacteristicSet;
import com.athena.imis.models.QueryPattern;
import com.athena.imis.models.TripleAsInt;
import com.athena.imis.tests.BigQueryTests.ECSTupleComparator;

public class QueryTests {

	
	public static HashMap<QueryPattern, HashSet<ArrayList<ECSTuple>>> queryAnswerListSet2 ;
	
	public static HashSet<ExtendedCharacteristicSet> visited = new HashSet<ExtendedCharacteristicSet>();
	
	static public HashMap<Node, Integer> varMap = new HashMap<Node, Integer>();
	
	static public HashMap<Integer, Node> reverseVarMap = new HashMap<Integer, Node>();
	
	public static Map<String, Integer> propertiesSet ; 
	
	public static int nextVar = -1;
	
	public static Map<Integer, HashMap<Integer, Integer>> propIndexMap ;
	
	public static Map<ExtendedCharacteristicSet, Integer> ecsIntegerMap ;
	
	public static HashMap<Node, Integer> varIndexMap;
	
	public static Map<Integer, long[]> ecsLongArrayMap ;
	
	public static Map<Integer, long[]> csMap ;
	
	public static Map<Integer, CharacteristicSet> rucs ;
	
	public static Map<CharacteristicSet, Integer> ucs ;
	
	public static Map<String, Integer> intMap ;
	
	public static Map<Integer, String> reverseIntMap ;
	
	public static Map<String, Integer> prefixMap ;
	
	public static void main(String[] args) {
		
		//DB db = DBMaker.newFileDB(new File("C:/temp/tempor.bin"))
		//args[0] = "C:/temp/temp" ;
		DB db = DBMaker.newFileDB(new File("C:/temp/temp"))
		//DB db = DBMaker.newFileDB(new File(args[0]))
				.transactionDisable()
 				.fileChannelEnable() 			
 				.fileMmapEnable()
 				.cacheSize(1000000000) 				
 				.closeOnJvmShutdown()
 				.make();
		
		ecsLongArrayMap = db.hashMapCreate("ecsLongArrays")
 				.keySerializer(Serializer.INTEGER)
 				.valueSerializer(Serializer.LONG_ARRAY)
 				.makeOrGet();
		
		prefixMap = db.hashMapCreate("prefixMap")
 				.keySerializer(Serializer.STRING)
 				.valueSerializer(Serializer.INTEGER)
 				.makeOrGet();
		
		Map<Integer, ExtendedCharacteristicSet> ruecs = db.hashMapCreate("ruecsMap")
 				.keySerializer(Serializer.INTEGER)
 				//.valueSerializer(Serializer.)
 				.makeOrGet();
		
		intMap = db.hashMapCreate("intMap")
 				.keySerializer(Serializer.STRING)
 				.valueSerializer(Serializer.INTEGER)
 				.makeOrGet();
		
		reverseIntMap = db.hashMapCreate("reverseIntMap")
 				.keySerializer(Serializer.INTEGER)
 				.valueSerializer(Serializer.STRING)
 				.makeOrGet();
		
		csMap = db.hashMapCreate("csMap")
 				.keySerializer(Serializer.INTEGER)
 				.valueSerializer(Serializer.LONG_ARRAY)
 				.makeOrGet();
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
		
		ucs = new HashMap<CharacteristicSet, Integer>();
		for(Integer ci : rucs.keySet()){
			ucs.put(rucs.get(ci), ci);
		}
		
		ecsIntegerMap = db.hashMapCreate("uecsMap")
 				.valueSerializer(Serializer.INTEGER)
 				//.valueSerializer(Serializer.)
 				.makeOrGet();
		Map<ExtendedCharacteristicSet, HashSet<ExtendedCharacteristicSet>> ecsLinks = db.hashMapCreate("ecsLinks")
 				//.keySerializer(Serializer.INTEGER)	 			
 				.makeOrGet(); 
		int tot = 0;
		for(Integer ecs : ruecs.keySet()){
			tot += ecsLongArrayMap.get(ecs).length;
		}
		
		System.out.println("total mapped triples: " + tot);
		tot = 0;
		for(ExtendedCharacteristicSet e : ecsLinks.keySet()){
			tot += ecsLinks.get(e).size();
		}
		System.out.println("total ecs links: " + tot);
		System.out.println("ECS Links size: " + ecsLinks.size()); 	
		
		Map<Integer, int[]> properIndexMap = db.hashMapCreate("propIndexMap")
 				.keySerializer(Serializer.INTEGER)	
 				.valueSerializer(Serializer.INT_ARRAY)
 				.makeOrGet();  
		
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
		
		/*for(String prr : propertiesSet.keySet()){
			System.out.println(prr);
		}*/
		
		//APO EDW ARXIZEI TO PANHGYRI
		
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
		
		String queryString;
 		Queries lubm = new Queries();
 		ArrayList<Long> times;
 		
 			
 		
 		for(String qs : lubm.getQueries(1)){
 		
 			try{
 			queryString = qs;
 			//queryString = lubm.q2;
 			times = new ArrayList<Long>(); 
 			
		 		System.out.println(queryString);
		 		Query q=QueryFactory.create(queryString);
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
		 					String pr = qe.predicate.getURI();
		 					/*if(pr.contains("swat"))				
		 						pr = pr.replaceAll("#", "##");*/
		 					
		 					ECSTuple nt = new ECSTuple(qe, propertiesSet.get(pr), getQueryTriplePattern(qe));
		 					
		 					nt.subjectBinds = qe.subjectBinds;
		 					nt.objectBinds = qe.objectBinds;
		 					qlistTuple.add(nt);
		 				}
		 			
		 				boolean fl = false;
		 				for(ExtendedCharacteristicSet ecs1 : ecsLinks.keySet()){ 			
		 						
		 					visited = new HashSet<ExtendedCharacteristicSet>();							
		 					if(findDataPatterns(ecs1, ecsLinks, qlistTuple, qlistTuple, 
									new ArrayList<ECSTuple>())){
		 						fl = true;
							}						
		 			
		 				}
		 		}
		 		System.out.println("size of query patterns " + queryAnswerListSet2.size());
		 		
		 		varIndexMap = new HashMap<Node, Integer>();
		 		int varIndex = 0;
		 		
		 		for(QueryPattern key : queryAnswerListSet2.keySet()){
		 			for(ArrayList<ECSTuple> dataPattern : queryAnswerListSet2.get(key)){
		 				for(ECSTuple ecsTuple : dataPattern){
		 					TripleAsInt tai = ecsTuple.triplePattern;
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
		 		HashMap<ArrayList<ExtendedCharacteristicSet>, HashSet<Integer>> qpVarMap = new HashMap<ArrayList<ExtendedCharacteristicSet>, HashSet<Integer>>();
		 		HashMap<ArrayList<ECSTuple>, HashSet<Integer>> qpVarMap2 = new HashMap<ArrayList<ECSTuple>, HashSet<Integer>>();
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
		 		for(int i = 0; i < 10; i++){
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
			 			
			 		
			 		}
			 		
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
			 		
			 		HashMap<Long, Vector<Integer>> previous_res_vectors = null;
			 		
			 		tstart = System.nanoTime();
			 		
			 		Map<QueryPattern, HashMap<Integer, Integer>> qpVarIndexMap = new HashMap<QueryPattern, HashMap<Integer,Integer>>();
			 		 		
				 	for(int qi = 0; qi < f.size(); qi++){
				 	
				 		QueryPattern qp = f.get(qi);
				 	
				 		int nextIndex = 0;
				 		HashMap<Integer, Integer> varIndexes = new HashMap<Integer, Integer>();
				 		for(ECSTuple nextECSPattern : qp.queryPattern){
				 			
				 			if(!varIndexes.containsKey(nextECSPattern.triplePattern.s))
				 				varIndexes.put(nextECSPattern.triplePattern.s, nextIndex++);
				 			varIndexes.put(nextECSPattern.triplePattern.o, nextIndex++);	 				
				 			
				 		}
				 		qpVarIndexMap.put(qp, varIndexes);
				 		//System.out.println(varIndexes.toString());
				 	
			 		}
			 		
			 		for(int qi = 0; qi < f.size(); qi++){
			 			
			 			QueryPattern qp = f.get(qi);
			 			
			 			tot = 0;
			 			
			 			HashMap<Long, Vector<Integer>> res_vectors = new HashMap<Long, Vector<Integer>>();	
			 			
			 			HashMap<Integer, Integer> varIndexes = qpVarIndexMap.get(qp);
			 			List<Integer> indexesOfCommonVarsToHash = new ArrayList<Integer>();
			 			
			 			if(qi < f.size()-1){
			 				QueryPattern nextQp = f.get(qi+1);
			 				HashMap<Integer, Integer> nextVarIndexes = qpVarIndexMap.get(nextQp);
			 				
			 				for(Integer nextVarIndex : varIndexes.keySet()){
			 					if(nextVarIndexes.containsKey(nextVarIndex)){
			 						indexesOfCommonVarsToHash.add(varIndexes.get(nextVarIndex));
			 					}
			 				}
			 			}
			 		
			 			List<Integer> indexesOfCommonVarsToProbe = new ArrayList<Integer>();
			 			
			 			if(qi > 0){
			 				QueryPattern previousQp = f.get(qi-1);
			 				HashMap<Integer, Integer> previousVarIndexes = qpVarIndexMap.get(previousQp);
			 				
			 				for(Integer nextVarIndex : varIndexes.keySet()){
			 					if(previousVarIndexes.containsKey(nextVarIndex)){
			 						indexesOfCommonVarsToProbe.add(varIndexes.get(nextVarIndex));
			 					}
			 				}
			 			}
			 			/*System.out.println("indexes of common vars to hash: " + indexesOfCommonVarsToHash.toString());
			 			System.out.println("indexes of common vars to probe: " + indexesOfCommonVarsToProbe.toString());*/
			 			
			 			
			 			for(ArrayList<ECSTuple> next : queryAnswerListSet2.get(qp)){ 				 				
			 				
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
 		}
 		 		
 		db.close();
	}
	
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
	
	
	static public boolean findDataPatterns(ExtendedCharacteristicSet ecs, 
			Map<ExtendedCharacteristicSet, HashSet<ExtendedCharacteristicSet>> links, 
			ArrayList<ECSTuple> queryLinks, 
			ArrayList<ECSTuple> originalQueryLinks, 
			ArrayList<ECSTuple> list){
						
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
		if(queryLinks.get(0).ecs.objectCS == null && ecs.objectCS != null) return false;
		if(queryLinks.get(0).ecs.objectCS != null && ecs.objectCS == null) return false;
		
		if(!propIndexMap.get(ecsIntegerMap.get(ecs)).containsKey(propertiesSet.get(queryLinks.get(0).ecs.predicate.toString())))
			return false;
		
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
		
		ECSTuple ecsTuple = new ECSTuple(ecs, propertiesSet.get(queryLinks.get(0).ecs.predicate.toString()), getQueryTriplePattern(queryLinks.get(0).ecs));
		ecsTuple.subjectBinds = queryLinks.get(0).ecs.subjectBinds;
		ecsTuple.objectBinds = queryLinks.get(0).ecs.objectBinds;
		
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
						
						ArrayList<ECSTuple> dummy = new ArrayList<ECSTuple>();
						dummy.addAll(list);
						
						findDataPatterns(child, links, new ArrayList<>(queryLinks.subList(1, queryLinks.size())), originalQueryLinks, dummy);
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
	
	
	
	public static HashMap<Long, Vector<Integer>> joinTwoECS(HashMap<Long, Vector<Integer>> res,
			HashMap<Long, Vector<Integer>> previous_res,
			ECSTuple e1, ECSTuple e2, 
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
								res.put(szudzik(nv.get(hashIndexes.get(0)), nv.get(hashIndexes.get(1))), nv);
							}
						
						}
					
					}						
				}
			
			return res;
			}
	
	public static HashMap<Long, Vector<Integer>> joinTwoECS(HashMap<Long, Vector<Integer>> res,			
			ECSTuple e1, ECSTuple e2, 
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
								if(!hashIndexes.isEmpty())
									res.put(szudzik(nv.get(hashIndexes.get(0)), nv.get(hashIndexes.get(1))), nv);
								else
									res.put(szudzik(nv.firstElement(), nv.lastElement()), nv);
							
							}
						
						}
					
					}						
				}
			
			
			return res;
			}
	
	public static long szudzik(int a, int b){
		
		return a >= b ? a * a + a + b : a + b * b;
		
	}
	
	public static boolean checkBinds(ECSTuple tuple, int subject, long[] array){
					
		if(tuple.subjectBinds != null){
			
			for(Integer prop : tuple.subjectBinds.keySet()){				
				int obj = tuple.subjectBinds.get(prop);
				long tripleSPOLong = ((long)prop << 54 | 
						(long)(subject & 0x7FFFFFF) << 27 | 
						(long)(obj & 0x7FFFFFF));				
				if(indexOfTriple(array, tripleSPOLong) < 0) {						
					/*System.out.println(tripleSPOLong);
					System.out.println(Arrays.toString(array));*/
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
}


