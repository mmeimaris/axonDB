package com.athena.imis.models;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementPathBlock;

import com.athena.imis.tests.RelationalQuery;

public class SQLTranslator {

	
	String sparql ;
	
	DirectedGraph<NewCS> queryGraph ;
		
	Map<NewCS, List<NewCS>> csJoinMap ;
	
	Set<NewCS> csSet = new HashSet<NewCS>();
	
	Map<NewCS, List<Triple>> csRestrictions = new HashMap<NewCS, List<Triple>>();
	
	Map<String, Integer> objectMap ;
	
	Map<NewCS, Integer> subjectMap = new HashMap<NewCS, Integer>() ;
	
	public Map<NewCS, Integer> getSubjectMap() {
		return subjectMap;
	}

	public void setSubjectMap(Map<NewCS, Integer> subjectMap) {
		this.subjectMap = subjectMap;
	}

	Map<NewCS, List<Triple>> csVars = new HashMap<NewCS, List<Triple>>();
	
	Map<List<NewCS>, List<Triple>> csJoinProperties = new HashMap<List<NewCS>, List<Triple>>(); 
	


	public DirectedGraph<NewCS> getQueryGraph() {
		return queryGraph;
	}

	public void setQueryGraph(DirectedGraph<NewCS> queryGraph) {
		this.queryGraph = queryGraph;
	}

	public Map<NewCS, List<NewCS>> getCsJoinMap() {
		return csJoinMap;
	}

	public void setCsJoinMap(Map<NewCS, List<NewCS>> csJoinMap) {
		this.csJoinMap = csJoinMap;
	}

	public Set<NewCS> getCsSet() {
		return csSet;
	}

	public void setCsSet(Set<NewCS> csSet) {
		this.csSet = csSet;
	}

	public Map<NewCS, List<Triple>> getCsRestrictions() {
		return csRestrictions;
	}

	public void setCsRestrictions(Map<NewCS, List<Triple>> csRestrictions) {
		this.csRestrictions = csRestrictions;
	}

	public Map<String, Integer> getObjectMap() {
		return objectMap;
	}

	public void setObjectMap(Map<String, Integer> objectMap) {
		this.objectMap = objectMap;
	}

	public Map<NewCS, List<Triple>> getCsVars() {
		return csVars;
	}

	public void setCsVars(Map<NewCS, List<Triple>> csVars) {
		this.csVars = csVars;
	}

	public Map<List<NewCS>, List<Triple>> getCsJoinProperties() {
		return csJoinProperties;
	}

	public void setCsJoinProperties(Map<List<NewCS>, List<Triple>> csJoinProperties) {
		this.csJoinProperties = csJoinProperties;
	}

	Map<String, Integer> propertyMap ;

	public Map<String, Integer> getPropertyMap() {
		return propertyMap;
	}

	public void setPropertyMap(Map<String, Integer> propertyMap) {
		this.propertyMap = propertyMap;
	}

	public String getSparql() {
		return sparql;
	}

	public void setSparql(String sparql) {
		this.sparql = sparql;
	}
	
	public void parseSPARQL(){
		
		Query query = QueryFactory.create(sparql);		
		
		ElementGroup g = (ElementGroup) query.getQueryPattern() ;

		ElementPathBlock triplePathBlock = (ElementPathBlock) g.getElements().get(0);				
		
		Map<Node, List<Triple>> subjectTripleMap = new HashMap<Node, List<Triple>>();
		
		Set<Node> objects = new HashSet<Node>();
		
		List<Var> projectVars = query.getProjectVars() ;
		
		for(TriplePath triplePath : triplePathBlock.getPattern().getList()){
			
			List<Triple> triplesOfSubject = subjectTripleMap.getOrDefault(triplePath.getSubject(), new ArrayList<Triple>());
			triplesOfSubject.add(triplePath.asTriple());
			subjectTripleMap.put(triplePath.getSubject(), triplesOfSubject);
			if(triplePath.getObject().isLiteral() || triplePath.getObject().isURI())
				objects.add(triplePath.getObject());			
			
		}
		
		Map<Node, List<Node>> varJoins = new HashMap<Node, List<Node>>();
		
		Map<Node, List<Triple>> subjectRestrictions = new HashMap<Node, List<Triple>>();
		
		Map<NewCS, List<NewCS>> csJoinMap = new HashMap<NewCS, List<NewCS>>();
		
		Map<Node, List<Integer>> subjectCSMap = new HashMap<Node, List<Integer>>();
		
		for(Node nextSubject : subjectTripleMap.keySet()){
			
//			if(!nextSubject.isVariable()) {
//				
//				continue;
//			}
						
			//System.out.println("\t" + subjectTripleMap.get(nextSubject).toString());
			
			List<Integer> propertiesAsList = new ArrayList<Integer>();
			
			for(Triple nextTriple : subjectTripleMap.get(nextSubject)){
				
				Integer property = propertyMap.get("<" + nextTriple.getPredicate().toString()+">");
				
				propertiesAsList.add(property);
				
				//find joins											
				if(nextTriple.getObject().isVariable() && subjectTripleMap.containsKey(nextTriple.getObject())){
					
					List<Node> joins = varJoins.getOrDefault(nextSubject, new ArrayList<Node>());
					
					joins.add(nextTriple.getObject());
					
					varJoins.put(nextSubject, joins);
					
					
				}		
				else if(nextTriple.getObject().isURI() || nextTriple.getObject().isLiteral()){
					
					List<Triple> restrictions = subjectRestrictions.getOrDefault(nextSubject, new ArrayList<Triple>());
					
					restrictions.add(nextTriple);
					
					subjectRestrictions.put(nextSubject, restrictions);
					
				}
								
				
			}
			for(Triple nextTriple : subjectTripleMap.get(nextSubject)){
				if(nextTriple.getObject().isVariable() && !subjectTripleMap.containsKey(nextTriple.getObject())){
					
					NewCS cs = new NewCS(propertiesAsList);
															
					List<Triple> vars = csVars.getOrDefault(cs, new ArrayList<Triple>());
					
					vars.add(nextTriple);
					
					csVars.put(cs, vars);
					
				}	
				
					
			}
			
			
			//System.out.println(csVars.toString());
			
			subjectCSMap.put(nextSubject, propertiesAsList);
			System.out.println(subjectCSMap.toString()) ; 
			
			
			csSet.add(new NewCS(propertiesAsList));
						
			if(subjectRestrictions.containsKey(nextSubject))
				csRestrictions.put(new NewCS(propertiesAsList), subjectRestrictions.get(nextSubject));
			
			System.out.println("Subject: " + nextSubject.toString());
			
			System.out.println("\t"+subjectCSMap.get(nextSubject).toString());
									
		}
		
		for(Node nextSubject : subjectTripleMap.keySet()) {
			
			for(Triple nextTriple : subjectTripleMap.get(nextSubject)){				
				if(nextTriple.getObject().isVariable() && subjectTripleMap.containsKey(nextTriple.getObject())){
					
					List<NewCS> joinLists = new ArrayList<NewCS>();
					//System.out.println("left: " + propertiesAsList.toString());
					//System.out.println("right: " + subjectCSMap.get(nextTriple.getObject()).toString());
					joinLists.add(new NewCS(subjectCSMap.get(nextSubject)));
					joinLists.add(new NewCS(subjectCSMap.get(nextTriple.getObject())));
					List<Triple> joinTriples = csJoinProperties.getOrDefault(joinLists, new ArrayList<Triple>());
					
					joinTriples.add(nextTriple);
					
					csJoinProperties.put(joinLists, joinTriples);
					
				}
					
			}
			
		}
		for(List<NewCS> nextJoin : csJoinProperties.keySet()){
			System.out.println("next join: " + nextJoin);
		}
		System.out.println("csJoinProperties " + csJoinProperties.toString()) ;
		
		for(Node nextSubject : varJoins.keySet()){
						
			List<NewCS> joinedCS = new ArrayList<NewCS>();
				
			for(Node nextObject : varJoins.get(nextSubject)){
				joinedCS.add(new NewCS(subjectCSMap.get(nextObject)));
			}			
			
			csJoinMap.put(new NewCS(subjectCSMap.get(nextSubject)), joinedCS);
			
		}
		
		for(Node nextSubject : subjectCSMap.keySet()){
			
			if(csJoinMap.containsKey(new NewCS(subjectCSMap.get(nextSubject))))
				continue;
			csJoinMap.put(new NewCS(subjectCSMap.get(nextSubject)), null);
			
		}
		DirectedGraph<NewCS> queryGraph = new DirectedGraph<NewCS>();
		
		for(NewCS nextSubjectList : csJoinMap.keySet()){
		
			queryGraph.addNode(nextSubjectList);
			
			if(csJoinMap.get(nextSubjectList) != null)
			for(NewCS nextJoin : csJoinMap.get(nextSubjectList)){
				
				queryGraph.addNode(nextJoin);
				
				queryGraph.addEdge(nextSubjectList, nextJoin);
				
			}
//			System.out.println(nextSubjectList);
//			if(csJoinMap.get(nextSubjectList) != null)
//				System.out.println("\t"+csJoinMap.get(nextSubjectList).toString());
//			
			
			
		}
		
		System.out.println("Root nodes: " + queryGraph.findRoots().toString());
		
		this.queryGraph = queryGraph;
		
		this.csJoinMap = csJoinMap ;
		
		this.csSet = csSet ;
		
		objectMap = new HashMap<String, Integer>();
		
		for(Node nextObject : objects) {
			
			try{
				Statement st = RelationalQuery.c.createStatement();
				
				String value = nextObject.toString();
				if(!nextObject.isLiteral())									
					value = "<" + value + ">";
				String objectSetQuery = " SELECT id, label FROM dictionary WHERE label = " + value.hashCode() ;
				//System.out.println(objectSetQuery);
				ResultSet rsProps = st.executeQuery(objectSetQuery);
				
				while(rsProps.next()){
					objectMap.put(value, rsProps.getInt(1));
				}
				rsProps.close();
				st.close();
			}catch ( Exception e){
				e.printStackTrace();
			}
			
			//System.out.println(objectMap.toString()); 
			
		}
		System.out.println("subject CS Map: " + subjectCSMap.toString()) ;
		for(Node nextSubject : subjectCSMap.keySet()) {
			
			try{
				Statement st = RelationalQuery.c.createStatement();
				
				String value = nextSubject.toString();
				if(!nextSubject.isURI())									
					continue;
				value = "<" + value + ">";
				String objectSetQuery = " SELECT id, label FROM dictionary WHERE label = " + value.hashCode() ;
				System.out.println(objectSetQuery);
				ResultSet rsProps = st.executeQuery(objectSetQuery);
				
				while(rsProps.next()){
					
					subjectMap.put(new NewCS(subjectCSMap.get(nextSubject)), rsProps.getInt(1));
				}
				rsProps.close();
				st.close();
			}catch ( Exception e){
				e.printStackTrace();
			}
			
			//System.out.println(objectMap.toString()); 
			
		}
		
		
	}
	
	public void prepareRestrictions(){
		
		
		
	}
	
}
