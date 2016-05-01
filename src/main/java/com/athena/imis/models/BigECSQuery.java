package com.athena.imis.models;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Stack;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementPathBlock;

import com.athena.imis.tests.BigQueryTests;

public class BigECSQuery {

	Query query;
	
	HashMap<BigExtendedCharacteristicSet, HashSet<BigExtendedCharacteristicSet>> ecsLinks ;
	HashMap<BigExtendedCharacteristicSet, HashSet<BigExtendedCharacteristicSet>> ecsVerticalLinks ;
	public HashSet<BigExtendedCharacteristicSet> ecsSet ;
	
	static HashSet<BigECSTree> visited = new LinkedHashSet<BigECSTree>();
	static HashMap<Node, HashMap<Integer, Integer>> subjectBindsMap;
	static HashSet<LinkedHashSet<BigExtendedCharacteristicSet>> listSet = new HashSet<LinkedHashSet<BigExtendedCharacteristicSet>>();
	
	public static HashSet<LinkedHashSet<BigExtendedCharacteristicSet>> getListSet() {
		return listSet;
	}

	public static void setListSet(
			HashSet<LinkedHashSet<BigExtendedCharacteristicSet>> listSet) {
		BigECSQuery.listSet = listSet;
	}

	BigECSTree BigECSTree ;
		
	public HashMap<BigExtendedCharacteristicSet, HashSet<BigExtendedCharacteristicSet>> getEcsLinks() {
		return ecsLinks;
	}

	public void setEcsLinks(
			HashMap<BigExtendedCharacteristicSet, HashSet<BigExtendedCharacteristicSet>> ecsLinks) {
		this.ecsLinks = ecsLinks;
	}

	LinkedHashSet<ECSJoin> joinList;

	public BigECSQuery(Query query){
		this.query = query;
	}

	public Query getQuery() {
		return query;
	}

	public void setQuery(Query query) {
		this.query = query;
	}

	public LinkedHashSet<ECSJoin> getJoinList() {
		return joinList;
	}

	public void setJoinList(LinkedHashSet<ECSJoin> joinList) {
		this.joinList = joinList;
	}

	public void findJoins(){

		listSet = new LinkedHashSet<LinkedHashSet<BigExtendedCharacteristicSet>>();
		
		Element pattern = query.getQueryPattern();

		ElementGroup g = (ElementGroup) pattern ;

		ElementPathBlock triplePathBlock = (ElementPathBlock) g.getElements().get(0);

		HashMap<Node, HashSet<Resource>> subjectMap = new HashMap<Node, HashSet<Resource>>();
		
		HashMap<Node, HashSet<BigExtendedCharacteristicSet>> subjectECSMap = new HashMap<Node, HashSet<BigExtendedCharacteristicSet>>();

		HashMap<Node, HashSet<Resource>> objectMap = new HashMap<Node, HashSet<Resource>>();

		HashMap<Node, HashSet<TripleAsInt>> varBindings = new HashMap<Node, HashSet<TripleAsInt>>();
		subjectBindsMap = new HashMap<Node, HashMap<Integer,Integer>>();
		HashMap<Node, HashMap<Integer, Integer>> objectBindsMap = new HashMap<Node, HashMap<Integer,Integer>>();
		BigECSTree = new BigECSTree(null);
		
		for(TriplePath triplePath : triplePathBlock.getPattern().getList()){

			Triple triple = triplePath.asTriple();

			if(subjectMap.containsKey(triple.getSubject())){

				if(triple.getPredicate().isURI())
				subjectMap.get(triple.getSubject()).add(ResourceFactory.createResource(triple.getPredicate().getURI()));

			}
			else{
				HashSet<Resource> properties = new HashSet<Resource>();
				if(triple.getPredicate().isURI())
					properties.add(ResourceFactory.createResource(triple.getPredicate().getURI()));
				subjectMap.put(triple.getSubject(), properties);
			}			
			if(!triple.getObject().isVariable()){
				if(subjectBindsMap.containsKey(triple.getSubject())){
					subjectBindsMap.get(triple.getSubject())
						.put(BigQueryTests.propertiesSet.get(triple.getPredicate().toString()), 
								BigQueryTests.intMap.get(triple.getObject().toString()));
				}
				else{
					HashMap<Integer, Integer> d = new HashMap<Integer, Integer>();
					d.put(BigQueryTests.propertiesSet.get(triple.getPredicate().toString()), 
								BigQueryTests.intMap.get(triple.getObject().toString()));
					subjectBindsMap.put(triple.getSubject(), d);
				}
			}
			
		}
		
		System.out.println(subjectBindsMap.toString());

		ecsSet = new HashSet<BigExtendedCharacteristicSet>();
		
		HashMap<Triple, BigExtendedCharacteristicSet> reverseTripleMap = new HashMap<Triple, BigExtendedCharacteristicSet>();

		HashMap<BigExtendedCharacteristicSet, HashSet<Triple>> ecsTripleMap = new HashMap<BigExtendedCharacteristicSet, HashSet<Triple>>();

		HashMap<BigExtendedCharacteristicSet, BigECSTree> BigECSTreeMap = new HashMap<BigExtendedCharacteristicSet, BigECSTree>();
		
		for(TriplePath triplePath : triplePathBlock.getPattern().getList()){

			Triple triple = triplePath.asTriple();
		/*	TIntHashSet newh = new TIntHashSet();
			for(Resource propres : subjectMap.get(triple.getSubject())){
				
			}*/
			BigCharacteristicSet subjectCS = new BigCharacteristicSet(triple.getSubject(), subjectMap.get(triple.getSubject()));
			BigCharacteristicSet objectCS = null;
	 		if(subjectMap.containsKey(triple.getObject())){
	 			objectCS = new BigCharacteristicSet(triple.getObject(), subjectMap.get(triple.getObject()));
	 		}
	 		BigExtendedCharacteristicSet ecs = new BigExtendedCharacteristicSet(subjectCS, objectCS);	 		
	 		ecs.subject = triple.getSubject();
	 		ecs.predicate = triple.getPredicate();
	 		ecs.object = triple.getObject();
	 		ecsSet.add(ecs);
	 		/*BigECSTree thisTree = new BigECSTree(ecs);
	 		BigECSTree.addChild(thisTree);
	 		BigECSTreeMap.put(ecs, thisTree);*/
	 		reverseTripleMap.put(triple, ecs);
	 		if(subjectECSMap.containsKey(triple.getSubject())){
	 			subjectECSMap.get(triple.getSubject()).add(ecs);
	 		}
	 		else{
	 			HashSet<BigExtendedCharacteristicSet> dummy = new HashSet<BigExtendedCharacteristicSet>();
	 			dummy.add(ecs);
	 			subjectECSMap.put(triple.getSubject(), dummy);
	 		}
	 		if(ecsTripleMap.containsKey(ecs)){
	 			ecsTripleMap.get(ecs).add(triple);
	 		}
	 		else{
	 			HashSet<Triple> dummy = new HashSet<Triple>();
	 			dummy.add(triple);
	 			ecsTripleMap.put(ecs, dummy);
	 		}
	 		//System.out.println(triple.toString());
	 		//System.out.println(ecs.properties.toString());

		}						
 		
		//TripleTree tree = new TripleTree(null);	
		//TripleTree tree = new TripleTree(null, null);
 		
		//HashMap<Triple, TripleTree> tripleTreeMap = new HashMap<Triple, TripleTree>();
 		
		ecsLinks = new HashMap<BigExtendedCharacteristicSet, HashSet<BigExtendedCharacteristicSet>>();
		for(BigExtendedCharacteristicSet e : ecsSet){
			ecsLinks.put(e, new LinkedHashSet<BigExtendedCharacteristicSet>());
		}
		ecsVerticalLinks = new HashMap<BigExtendedCharacteristicSet, HashSet<BigExtendedCharacteristicSet>>(); 
		for(TriplePath triplePath : triplePathBlock.getPattern().getList()){
			
			Triple triple = triplePath.asTriple();	
			
			Node subject = triple.getSubject();
			
			Node object = triple.getObject();

	 		if(subjectMap.containsKey(object)){
	 			BigExtendedCharacteristicSet ecs = reverseTripleMap.get(triple);
	 			//TripleTree tripleTree = new TripleTree(triple);
	 			/*TripleTree tripleTree = new TripleTree(ecs, triple);
	 			tree.addChild(ecs, tripleTree);
	 			tripleTreeMap.put(triple, tripleTree);*/
	 			
	 			BigECSTree thisTree = new BigECSTree(ecs);
		 		BigECSTree.addChild(thisTree);
		 		BigECSTreeMap.put(ecs, thisTree);
	 			
	 		}
	 		BigExtendedCharacteristicSet subjectECS = reverseTripleMap.get(triple);
	 		if(subjectECSMap.containsKey(object)){
	 				
	 			HashSet<BigExtendedCharacteristicSet> objectECSSet = subjectECSMap.get(object);
	 			 				 				
	 			if(ecsLinks.containsKey(subjectECS)){
	 				ecsLinks.get(subjectECS).addAll(objectECSSet);
	 			}
	 			else{
	 				HashSet<BigExtendedCharacteristicSet> dummy = new HashSet<BigExtendedCharacteristicSet>();
	 				dummy.addAll(objectECSSet);
	 				ecsLinks.put(subjectECS, objectECSSet);
	 			} 				
	 		}
	 		
	 		if(subjectECSMap.containsKey(subject)){
 				
	 			HashSet<BigExtendedCharacteristicSet> subjectECSSet = subjectECSMap.get(subject);
	 			 				 				
	 			if(ecsVerticalLinks.containsKey(subjectECS)){
	 				ecsVerticalLinks.get(subjectECS).addAll(subjectECSSet);
	 			}
	 			else{
	 				HashSet<BigExtendedCharacteristicSet> dummy = new HashSet<BigExtendedCharacteristicSet>();
	 				dummy.addAll(subjectECSSet);
	 				ecsVerticalLinks.put(subjectECS, subjectECSSet);
	 			} 				
	 		}
			
		}
		
		for(BigExtendedCharacteristicSet ecs : ecsLinks.keySet()){
			
			BigECSTree leftECS = BigECSTreeMap.get(ecs);
			/*
			if(ecsLinks.get(ecs).isEmpty()){
				BigECSTree.addChild(new BigECSTree(ecs));
			}
			else*/
			//System.out.println("\ttriples: " + ecsTripleMap.get(ecs).toString());
			for(BigExtendedCharacteristicSet link : ecsLinks.get(ecs)){
				BigECSTree rightECS = BigECSTreeMap.get(link);
				leftECS.addChild(rightECS);
				BigECSTree.removeChild(rightECS);				
				/*System.out.println("\t: " + link.properties.toString());
				System.out.println(leftECS.children.toString());*/
			}
		}		

		/*if(BigECSTree.children.size() == 1){
			LinkedHashSet<BigExtendedCharacteristicSet> l = new LinkedHashSet<BigExtendedCharacteristicSet>();
			for(BigECSTree child : BigECSTree.children){
				l.add(child.root);
			}
			listSet.add(l);
		}
		else*/
		for(BigECSTree child : BigECSTree.children){
			//System.out.println("starting next");
			//iterativeDFS(child);
			recursiveDFS(child, new LinkedHashSet<BigExtendedCharacteristicSet>());
		}
	
		HashSet<LinkedHashSet<BigExtendedCharacteristicSet>> contained = new LinkedHashSet<LinkedHashSet<BigExtendedCharacteristicSet>>();
		for(LinkedHashSet<BigExtendedCharacteristicSet> list : listSet){
			
			for(LinkedHashSet<BigExtendedCharacteristicSet> list2 : listSet){
				if(list.containsAll(list2) && list!=list2)
			
					contained.add(list2);
				
			}
		}
		for(LinkedHashSet<BigExtendedCharacteristicSet> containedList : contained){
			listSet.remove(containedList);
		}
		for(LinkedHashSet<BigExtendedCharacteristicSet> list : listSet){
			//System.out.println("next list");
			for(BigExtendedCharacteristicSet next : list){
				next.subjectBinds = subjectBindsMap.get(next.subject);
				next.objectBinds = subjectBindsMap.get(next.object);
				
			}
		}		
		
	}
	
	
	public BigECSTree getBigECSTree() {
		return BigECSTree;
	}

	public void setBigECSTree(BigECSTree BigECSTree) {
		this.BigECSTree = BigECSTree;
	}

	static public void recursiveDFS(BigECSTree cur, LinkedHashSet<BigExtendedCharacteristicSet> list){
		
		
		if(cur == null || cur.children.isEmpty() || visited.contains(cur)){
			listSet.add(list);
			return;
		}
		
		visited.add(cur);
		list.add(cur.root);
		for(BigECSTree child : cur.children){
			
			LinkedHashSet<BigExtendedCharacteristicSet> dummy = new LinkedHashSet<BigExtendedCharacteristicSet>();
			dummy.addAll(list);
			recursiveDFS(child, dummy);
			
		}
		
	}
	
	static public void iterativeDFS(BigECSTree tree){
		
		Stack<BigECSTree> stack = new Stack<BigECSTree>();
		stack.push(tree);
		HashSet<BigECSTree> visited  = new HashSet<BigECSTree>();		
		while(!stack.empty()){
			tree = stack.pop();
			if(tree == null ) {
				//System.out.println("end of chain" ); 
				continue;
				}
			if(tree.root != null){
				//System.out.println("printing current root");
				//System.out.println(tree.toString());
			}
			visited.add(tree);			
			for(BigECSTree child : tree.children){
				//System.out.println("\t"+child.root.properties.toString() );
				if(!visited.contains(child))
					stack.push(child);
			}
		}
		
	}

}