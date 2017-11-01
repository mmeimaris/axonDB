package com.athena.imis.tests;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.jena.graph.Triple;

import com.athena.imis.models.DirectedGraph;
import com.athena.imis.models.MinHash;
import com.athena.imis.models.NewCS;
import com.athena.imis.models.SQLTranslator;

public class RelationalQuery {

	
	public static Connection c ;
	
	public static void main(String[] args) {
		
		
		 c = null;
	      Statement stmt = null;
	      try {
	         Class.forName("org.postgresql.Driver");
	         c = DriverManager
	        		 .getConnection("jdbc:postgresql://"+args[0]+":5432/"+args[1],
	        		            //"mmeimaris", "dirtymarios");
	        				 //"postgres", "postgres");
	        				 args[2], args[3]);
	            /*.getConnection("jdbc:postgresql://"+args[0]+":5432/testbatch",
	            "mmeimaris", "dirtymarios");*/
	         System.out.println("Opened database successfully");

	         
	      } catch ( Exception e ) {
	         System.err.println( e.getClass().getName()+": "+ e.getMessage() );
	         System.exit(0);
	      }	      
	     
		ResultSet rs;
		long time = 0;
		int res = 0;
		double execTime = 0d, planTime = 0d;
		
		HashMap<String, Integer> propMap = new HashMap<String, Integer>();
		try{
			Statement st = c.createStatement();
			/*CSMerger merger = new CSMerger(c);
			merger.mergeExistingRelationalDB();
			if(true) return ;*/
			st = c.createStatement();
			
			String propertiesSetQuery = " SELECT id, uri FROM propertiesset ;";
			ResultSet rsProps = st.executeQuery(propertiesSetQuery);
			
			while(rsProps.next()){
				propMap.put(rsProps.getString(2), rsProps.getInt(1));
			}
			rsProps.close();
			st.close();
			System.out.println("propMap: " + propMap.toString()) ;
			SQLTranslator sqlTranslator = new SQLTranslator();
			
			String sparql = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
					+ "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> "
					+ "SELECT ?X ?Y ?Z WHERE {"
					+ "?X rdf:type ub:UndergraduateStudent ."
					//+ "?W rdf:type ub:UndergraduateStudent ."
					+ "?Y rdf:type ub:Department ."
					+ "?X ub:memberOf ?Y . "
					//+ "?W ub:memberOf ?Y . "
					+ "?Y ub:subOrganizationOf ?p . "
					//+ "?p rdf:type ?p1 . "
					+ "?X ub:emailAddress ?Z}";
			sparql = "SELECT ?v0 WHERE {  ?v0 <http://schema.org/eligibleRegion> ?v122 . "
					+ " ?v0 <http://purl.org/goodrelations/includes> ?v1 .  "
					//+ "?v2 <http://purl.org/goodrelations/offers> ?v0 . "
					//+ " ?v0 <http://purl.org/goodrelations/price> ?v3 . "
					//+ " ?v0 <http://purl.org/goodrelations/serialNumber> ?v4 .  "
					//+ "?v0 <http://purl.org/goodrelations/validFrom> ?v5 . "
					+ " ?v0 <http://purl.org/goodrelations/validThrough> ?v6 .  "
					+ "?v0 <http://schema.org/eligibleQuantity> ?v8 .  "
					//+ "?v0 <http://schema.org/priceValidUntil> ?v10 .  "
					+ "?v1 <http://schema.org/author> ?v7 .  }";
//			sparql = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
//					+ "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> "
//					+ "SELECT ?X ?Y "
//					+ "WHERE "
//					+ "{?X rdf:type ub:UndergraduateStudent . "
//					+ "?Y rdf:type ub:GraduateCourse . "
//					+ "?X ub:takesCourse ?Y . "
//					+ "<http://www.Department0.University0.edu/AssociateProfessor0> ub:teacherOf ?Y }";
					//+ "<http://www.Department0.University0.edu/AssociateProfessor0> ub:teacherOf ?Y }";
//			String reactomePrefixes = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
//					+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
//					+ "PREFIX owl: <http://www.w3.org/2002/07/owl#> "
//					+ "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> "
//					+ "PREFIX dc: <http://purl.org/dc/elements/1.1/> "
//					+ "PREFIX dcterms: <http://purl.org/dc/terms/> "
//					+ "PREFIX foaf: <http://xmlns.com/foaf/0.1/> "
//					+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#> "
//					+ "PREFIX biopax3: <http://www.biopax.org/release/biopax-level3.owl#> " ;
//			sparql = reactomePrefixes 
//					+ "SELECT DISTINCT ?pathway ?reaction ?complex ?protein ?ref  "
//					+ "WHERE  "
//					+ "{?pathway rdf:type biopax3:Pathway .  "
//					+ "?pathway biopax3:displayName ?pathwayname ."
//					+ "?pathway biopax3:pathwayComponent ?reaction . "
//					+ "?reaction rdf:type biopax3:BiochemicalReaction .  "			
//					+ "?reaction biopax3:right ?complex ."
//					+ "?complex rdf:type biopax3:Complex .  "
//					+ "?complex biopax3:component ?protein . "
//					+ "?protein rdf:type biopax3:Protein . "
//					+ "?protein biopax3:entityReference ?ref ."
//					+ "?ref biopax3:id ?id ; rdf:type ?refType" 
//					+ "}";
			System.out.println("SPARQL QUERY: " + sparql);
			sqlTranslator.setSparql(sparql);
			
			sqlTranslator.setPropertyMap(propMap);
			
			sqlTranslator.parseSPARQL();
			
			Map<NewCS, List<NewCS>> csJoinMap = sqlTranslator.getCsJoinMap();
			
			Set<NewCS> csSet = sqlTranslator.getCsSet();
			
			String schemaQuery = " SELECT DISTINCT * ";
			String schemaWhere = " WHERE " ;
			int cs_index = 0, ecs_index = 0;
			
			Map<NewCS, String> csVarMap = new HashMap<NewCS, String>();
						
			Map<NewCS, Set<String>> csMatches = new HashMap<NewCS, Set<String>>();
			
			Map<NewCS, Set<String>> csQueryMatches = new HashMap<NewCS, Set<String>>();
			
			Set<NewCS> undangled = new HashSet<NewCS>();
			//get ecs from db for each pair of SO joins
			for(NewCS nextCSS : csJoinMap.keySet()){
				
				if(csJoinMap.get(nextCSS) == null) 
					continue;
				for(NewCS nextCSO : csJoinMap.get(nextCSS)){
					
					undangled.add(nextCSO);
					
					String schema = " SELECT DISTINCT * FROM ecs_schema as e "
							+ "WHERE e.css_properties @> ARRAY" + nextCSS.getAsList().toString() 
							+ " AND e.cso_properties @> ARRAY" + nextCSO.getAsList().toString();							
					st = c.createStatement();
					//System.out.println(schema);
					ResultSet rsS = st.executeQuery(schema);
					
					while(rsS.next()){
						Set<String> css_matches = csQueryMatches.getOrDefault(nextCSS, new HashSet<String>());
						css_matches.add(rsS.getString(2));
						//if(csMatches.containsKey(nextCSS))
						csQueryMatches.put(nextCSS, css_matches);
						
						Set<String> cso_matches = csQueryMatches.getOrDefault(nextCSO, new HashSet<String>());
						cso_matches.add(rsS.getString(3));
						csQueryMatches.put(nextCSO, cso_matches);
					}
					rsS.close();
					st.close();
					System.out.println("new round " + csMatches.toString());
					for(NewCS nextCS : csQueryMatches.keySet()){
						System.out.println("next cs " + nextCS.toString());
						if(!csMatches.containsKey(nextCS)){
							System.out.println("not contained");
							csMatches.put(nextCS, csQueryMatches.get(nextCS));
							System.out.println("cs matches thus far: "+ csMatches.toString());
						}
						else{
							System.out.println("contained");
							Set<String> c = csQueryMatches.get(nextCS);
							System.out.println("existing matches: "+ csMatches.toString());
							System.out.println("existing c: "+ c.toString());
							c.retainAll(csMatches.get(nextCS));
							System.out.println("after retain: "+ c.toString());
							csMatches.put(nextCS, c) ;
						}
						
					}
					csQueryMatches.clear();
				}				
				
			}			
			for(NewCS nextCSS : csSet){
				
				if(csJoinMap.get(nextCSS) != null || undangled.contains(nextCSS))
					continue;
				System.out.println("Dangling: " + nextCSS.toString());													
				String schema = " SELECT DISTINCT * FROM ecs_schema as e "
						+ "WHERE e.css_properties @> ARRAY" + nextCSS.toString() ;											
				st = c.createStatement();
				//System.out.println(schema);
				ResultSet rsS = st.executeQuery(schema);
				
				while(rsS.next()){
					Set<String> css_matches = csMatches.getOrDefault(nextCSS, new HashSet<String>());
					css_matches.add(rsS.getString(2));
					csMatches.put(nextCSS, css_matches);					
				}
				rsS.close();
				st.close();
			}
			
			System.out.println("CS Matches: " + csMatches.toString());
			for(NewCS nextCS : csMatches.keySet()){
				nextCS.setMatches(csMatches.get(nextCS));
			}
			
			//prepare WHERE clause
			String finalQuery = "";
			String where = " WHERE ";
			
			//sqlTranslator.getObjectMap() ;
			
			List<String> resList = new ArrayList<String>();
			
			Map<NewCS, List<Triple>> csRestrictions = sqlTranslator.getCsRestrictions();
			
			//generate aliases for CSs
			Map<NewCS, String> csAliases = new HashMap<NewCS, String>();
			
			int csAliasIdx = 0; for(NewCS nextCS : csMatches.keySet()){csAliases.put(nextCS, "c"+(csAliasIdx++));}
			
			Map<String, String> csIdAliases = new HashMap<String, String>();
			
			csAliasIdx = 0; 
			for(NewCS nextCS : csMatches.keySet()){
				for(String nextCSId : nextCS.getMatches())
					csIdAliases.put(nextCSId, "c"+(csAliasIdx));
				csAliasIdx++;
			}
			
			for(NewCS nextCS : csMatches.keySet()){
				
				if(csRestrictions.containsKey(nextCS)){
					
					List<Triple> restrictions = csRestrictions.get(nextCS);
					
					for(Triple nextRes : restrictions){
						
						//we need the CS alias here
						String restriction = csAliases.get(nextCS) + ".p_" + 
								propMap.get("<" + nextRes.getPredicate().toString()+">") + " = " + 
								sqlTranslator.getObjectMap().get("<" + nextRes.getObject().toString()+">");
						
						if(!where.equals(" WHERE "))
							where += " AND ";
						
						where += restriction ;
						
					}
				}
				else if(sqlTranslator.getSubjectMap().containsKey(nextCS)){
					String restriction = csAliases.get(nextCS) + ".s = " + 
							sqlTranslator.getSubjectMap().get(nextCS);
					
					if(!where.equals(" WHERE "))
						where += " AND ";
					
					where += restriction ;
				}
				else {
					continue;
				}
				
			}
			
			System.out.println(where);
			
			//find permutations of cs
			List<List<String>> csAsList = new ArrayList<List<String>>();
			List<NewCS> csMatchesOrdered = new ArrayList<NewCS>(csMatches.keySet());
			
			Map<NewCS, Integer> csListIndexMap = new HashMap<NewCS, Integer>();
			
			int csIndexMap = 0;
			for(NewCS nextCS : csMatchesOrdered){
				csAsList.add(new ArrayList<String>(nextCS.getMatches()));
				csListIndexMap.put(nextCS, csIndexMap++);
			}
			System.out.println("CS list index map: " + csListIndexMap.toString());
			List<List<String>> perms = cartesianProduct(csAsList);
			
			Map<NewCS, List<Triple>> vars = sqlTranslator.getCsVars();
			System.out.println("vars " + vars.toString()) ;
			List<String> csProjectionsOrdered = new ArrayList<String>();
			for(NewCS nextCS : csMatchesOrdered){
				
				String csProjection = "";
				if(!vars.containsKey(nextCS)) {
					csProjection += csAliases.get(nextCS) + ".s";
					csProjectionsOrdered.add(csProjection);
					continue;
				}
				List<Triple> nextCSVars = vars.get(nextCS);
				//System.out.println(nextCSVars.toString());
				
				for(Triple t : nextCSVars){
					csProjection += csAliases.get(nextCS) + ".p_"+propMap.get("<" + t.getPredicate().toString()+">") + ", ";
				}
				csProjection = csProjection.substring(0, csProjection.length()-2);
				csProjectionsOrdered.add(csProjection);
			}
			System.out.println(csProjectionsOrdered.toString());
			//are there any joins? 
			String noJoins = ""; boolean joinsExist = false;
			
			for(NewCS nextCSS : csJoinMap.keySet()){
				if(csJoinMap.get(nextCSS) != null){
					joinsExist = true;
					break;
				}
			}
			
			
			//Map<List<Integer>, List<Integer>> permToListMap = new HashMap<List<Integer>, List<Integer>>();
			
			
			//how do we build a query graph representation?
			DirectedGraph<NewCS> queryGraph = sqlTranslator.getQueryGraph();
			Iterator<NewCS> it = queryGraph.iterator();
			
			while(it.hasNext()){
				
				NewCS node = it.next();
				System.out.println("Next node: " + node.toString());
				System.out.println("\tChildren: " + queryGraph.edgesFrom(node).toString());
				
			}
			int permIndex = 0;
			
			/*for(List<String> nextPerm : perms){
			
				List<Integer> asIntList = new ArrayList<Integer>();
				for(String nextCS : nextPerm){
					asIntList.add(Integer.parseInt(nextCS));
				}
				permToListMap.put(asIntList, csMatchesOrdered.get(permIndex++));
			}*/
			if(!joinsExist){
				//one query for each permutation
				for(List<String> nextPerm : perms){
														
					int idx = 0;
					List<Integer> asIntList = new ArrayList<Integer>();
					for(String nextCS : nextPerm){
						asIntList.add(Integer.parseInt(nextCS));
					}
					for(Integer nextCS : asIntList){
						
						String varList = csProjectionsOrdered.get(idx++);
						if(!noJoins.equals("")){
							noJoins += " UNION ";			
						}
						noJoins += " (SELECT " + varList + " FROM cs_" + nextCS + " AS " + csIdAliases.get(nextCS+"")+ " " + where + ") ";	
					}
										
				}	
				
				finalQuery = noJoins;// + where ;
				//one query is enough
				System.out.println(finalQuery);
			}
			else{
				
				Set<NewCS> graphRoots = queryGraph.findRoots() ;
				
				if(graphRoots.isEmpty()) // no roots, must be a cyclic query -- let's iterate through every node!
					graphRoots = queryGraph.getmGraph().keySet();
				
				Stack<NewCS> stack = new Stack<NewCS>();								
				
				Map<NewCS, LinkedHashSet<NewCS>> joinQueues = new HashMap<NewCS, LinkedHashSet<NewCS>>();
				
				for(NewCS root : graphRoots){
					
					stack.push(root);
					
					Set<NewCS> visited = new HashSet<NewCS>();
					
					LinkedHashSet<NewCS> currentQueue = joinQueues.getOrDefault(root, new LinkedHashSet<NewCS>()) ;
					
					while(!stack.isEmpty()){
						
						NewCS currentCS = stack.pop();												
						
						//System.out.println("next popped : " + currentCS.toString());
						
						visited.add(currentCS);
						
						if(queryGraph.isSink(currentCS))
							continue;
						
						currentQueue.add(currentCS);
						
						for(NewCS child : queryGraph.edgesFrom(currentCS)){
							
							//if(!visited.contains(child)){
								
								stack.push(child);
								
								if(joinQueues.containsKey(child)){
									currentQueue = joinQueues.get(child) ;
									currentQueue.add(currentCS) ;
									joinQueues.remove(currentCS);
									joinQueues.remove(child);
									joinQueues.put(currentCS, currentQueue);
									joinQueues.put(child, currentQueue);
								}
								else{
									currentQueue.add(child);
									joinQueues.remove(currentCS);
									joinQueues.put(currentCS, currentQueue);
									joinQueues.put(child, currentQueue);
								}
							//}
							
						}
						
						
					}
				}
				System.out.println("\n");
				System.out.println(joinQueues.toString());
				Set<LinkedHashSet<NewCS>> uniqueQueues = new HashSet<LinkedHashSet<NewCS>>();
				for(NewCS nextCS : joinQueues.keySet()){
					uniqueQueues.add(joinQueues.get(nextCS));
				}
				System.out.println(uniqueQueues.toString());
				
				
				
				String varList = "";
				System.out.println(" projections " + csProjectionsOrdered.toString()) ;
				for(int ig = 0; ig < csProjectionsOrdered.size(); ig++){
					varList += csProjectionsOrdered.get(ig) + ", ";
				}
				varList = varList.substring(0, varList.length()-2) ;
				Map<NewCS, Set<NewCS>> reverseJoinMap = new HashMap<NewCS, Set<NewCS>>();
				
				//reverse the join map...
				for(NewCS key : csJoinMap.keySet()){
					if(!csJoinMap.containsKey(key) || csJoinMap.get(key) == null) continue;
					for(NewCS nextValue : csJoinMap.get(key)){
						
						Set<NewCS> values = reverseJoinMap.getOrDefault(nextValue, new HashSet<NewCS>());
						values.add(key) ;
						reverseJoinMap.put(nextValue, values) ;				
						
					}
					
				}
				
				
				
				for(LinkedHashSet<NewCS> nextQueue : uniqueQueues){

					List<NewCS> qAsList = new ArrayList<NewCS>(nextQueue) ;
					int jid = 0;
					String nextQS = "";
					
					HashSet<NewCS> visited = new HashSet<NewCS>();
					
					for(int i = 0; i < qAsList.size(); i++){
					
						NewCS nextCS = qAsList.get(i);
						//System.out.println("next CS: " + nextCS + " alias " + csAliases.get(nextCS));
						visited.add(nextCS) ;
						
						//NewCS toJoinCS = qAsList.get(i+1);
						
						if(jid++ == 0){
							nextQS = " SELECT DISTINCT " + varList + " FROM cs_ AS " +csAliases.get(nextCS) + " " ;  
						}
						else {
							if(reverseJoinMap.get(nextCS) != null) {
								//System.out.println("here!") ;
								nextQS += " INNER JOIN cs_ AS " + csAliases.get(nextCS) + " ON " ;
								
								for(NewCS nextReverseJoin : reverseJoinMap.get(nextCS)){
									if(visited.contains(nextReverseJoin)){
										List<NewCS> joinKey = new ArrayList<NewCS>();
										
										joinKey.add(nextReverseJoin);
										
										joinKey.add(nextCS);
										
										//List<Triple> joinProps = sqlTranslator.getCsJoinProperties().get(joinKey);
										
										List<Integer> joinProps = new ArrayList<Integer>();
										System.out.println("joinKey: " + joinKey.toString());
										for(Triple nextJoinTriple : sqlTranslator.getCsJoinProperties().get(joinKey)){
										
											joinProps.add(propMap.get("<" + nextJoinTriple.getPredicate().toString()+">"));
										
										}
										
										for(int j = 0; j < joinProps.size(); j++){									
											nextQS += csAliases.get(nextReverseJoin)+".p_"+joinProps.get(j) 
													+ " = " + csAliases.get(nextCS)+".s ";
											if(j < joinProps.size()-1)
												nextQS += " AND " ;
										}
										
										
									}
								}
							}
							
							else if(csJoinMap.get(nextCS) != null) {
								//System.out.println("here2!") ;
								//if(!visited.contains(nextCS))
								nextQS += " INNER JOIN cs_ AS " + csAliases.get(nextCS) + " ON " ;
								
								for(NewCS nextReverseJoin : csJoinMap.get(nextCS)){
									if(visited.contains(nextReverseJoin)){
										List<NewCS> joinKey = new ArrayList<NewCS>();
										
										joinKey.add(nextCS);
										
										joinKey.add(nextReverseJoin);
										
										//List<Triple> joinProps = sqlTranslator.getCsJoinProperties().get(joinKey);
										
										List<Integer> joinProps = new ArrayList<Integer>();
										
										for(Triple nextJoinTriple : sqlTranslator.getCsJoinProperties().get(joinKey)){
										
											joinProps.add(propMap.get("<" + nextJoinTriple.getPredicate().toString()+">"));
										
										}
										
										for(int j = 0; j < joinProps.size(); j++){									
											nextQS += csAliases.get(nextCS)+".p_"+joinProps.get(j) 
													+ " = " + csAliases.get(nextReverseJoin)+".s ";
											if(j < joinProps.size()-1)
												nextQS += " AND " ;
										}
										
										
									}
								}
							}
							
						}
					}
					
					for(List<String> nextPerm : perms){
						String templateQ = nextQS ;	
						int idx = 0;
						System.out.println("nextPerm: " + nextPerm.toString()) ;						
						for(String nextIntString : nextPerm) {
							NewCS nextCSToTransform = csMatchesOrdered.get(idx++);							
							templateQ = templateQ.replaceAll("cs_ AS "+csAliases.get(nextCSToTransform), "cs_"+nextIntString+" AS "+csAliases.get(nextCSToTransform));
							
						}
						
						System.out.println("nextQS: " + templateQ + " " + where) ;
						try{
					    	
					    	Statement st2 = c.createStatement();
					    	String explain = "EXPLAIN ANALYZE " ;
					    	explain = "" ;
					    	if(!where.equals(" WHERE ")){
					    		templateQ = explain + templateQ + " " + where;	
							}
					    	else{
					    		templateQ = explain + templateQ + " ";
					    	}
					    	
					    	
					    	//templateQ = templateQ.replaceAll("p_0 = 20", "p_0 = 22");
					    	ResultSet rs2 = st2.executeQuery(templateQ); //
					    	long start = System.nanoTime();
						    while (rs2.next())
							{				    	
//							    System.out.println(rs2.getString(1));
//							    if(rs2.getString(1).contains("Execution time: ")){
//							    	String exec = rs2.getString(1).replaceAll("Execution time: ", "").replaceAll("ms", "").trim();
//							    	execTime += Double.parseDouble(exec);
//							    	
//							    }
//							    else if(rs2.getString(1).contains("Planning time: ")){
//							    	String plan = rs2.getString(1).replaceAll("Planning time: ", "").replaceAll("ms", "").trim();
//							    	execTime += Double.parseDouble(plan);
//							    	planTime += Double.parseDouble(plan);
//							    	
//							    }					   
							    
							    res++;
							}
						    rs2.close();
						    //System.out.println(execTime);
						    time += System.nanoTime() - start;
						    System.out.println(res);
						    System.out.println("time: " + time);
						    System.out.println("execTime: " + execTime + " ms ");
						    System.out.println("planTime: " + planTime + " ms ");
					    	
						}
						catch(SQLException e){
							e.printStackTrace();
						}
						System.out.println() ;
					}
											
				}												
												
			}							
			
			if(true) return ;
			
		}catch (Exception e){
			e.printStackTrace();
			return ;
		}
				
	}
	
	
	/*public static Set<Set<String>> cartesianProduct(Set<String>... sets) {
	    if (sets.length < 2)
	        throw new IllegalArgumentException(
	                "Can't have a product of fewer than two sets (got " +
	                sets.length + ")");

	    return _cartesianProduct(0, sets);
	}
	
	private static Set<Set<String>> _cartesianProduct(int index, Set<String>... sets) {
	    Set<Set<String>> ret = new HashSet<Set<String>>();
	    if (index == sets.length) {
	        ret.add(new HashSet<String>());
	    } else {
	        for (String obj : sets[index]) {
	            for (Set<String> set : _cartesianProduct(index+1, sets)) {
	                set.add(obj);
	                ret.add(set);
	            }
	        }
	    }
	    return ret;
	}*/

	private static HashMap<List<NewCS>, Integer> updateCardinality(List<NewCS> next,
			Map<NewCS, Integer> csSizes, HashMap<List<NewCS>, Integer> pathCosts) {
		int newCardinality = 0;
		
		for(NewCS innerCS : next){
			
			newCardinality += csSizes.get(innerCS);
			
		}
		pathCosts.put(next, newCardinality) ;
		
		return pathCosts;
		
	}


	protected static <T> List<List<T>> cartesianProduct(List<List<T>> lists) {
	    List<List<T>> resultLists = new ArrayList<List<T>>();
	    if (lists.size() == 0) {
	        resultLists.add(new ArrayList<T>());
	        return resultLists;
	    } else {
	        List<T> firstList = lists.get(0);
	        List<List<T>> remainingLists = cartesianProduct(lists.subList(1, lists.size()));
	        for (T condition : firstList) {
	            for (List<T> remainingList : remainingLists) {
	                ArrayList<T> resultList = new ArrayList<T>();
	                resultList.add(condition);
	                resultList.addAll(remainingList);
	                resultLists.add(resultList);
	            }
	        }
	    }
	    return resultLists;
	}
}
