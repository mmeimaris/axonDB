package com.athena.imis.models;

import gnu.trove.set.hash.TIntHashSet;

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

import com.athena.imis.tests.QueryTests;

public class ECSQuery {

	Query query;
	
	HashMap<ExtendedCharacteristicSet, HashSet<ExtendedCharacteristicSet>> ecsLinks ;
	HashMap<ExtendedCharacteristicSet, HashSet<ExtendedCharacteristicSet>> ecsVerticalLinks ;
	
	static HashSet<ECSTree> visited = new LinkedHashSet<ECSTree>();
	static HashMap<Node, HashMap<Integer, Integer>> subjectBindsMap;
	static HashSet<LinkedHashSet<ExtendedCharacteristicSet>> listSet = new HashSet<LinkedHashSet<ExtendedCharacteristicSet>>();
	
	public static HashSet<LinkedHashSet<ExtendedCharacteristicSet>> getListSet() {
		return listSet;
	}

	public static void setListSet(
			HashSet<LinkedHashSet<ExtendedCharacteristicSet>> listSet) {
		ECSQuery.listSet = listSet;
	}

	ECSTree ecsTree ;
		
	public HashMap<ExtendedCharacteristicSet, HashSet<ExtendedCharacteristicSet>> getEcsLinks() {
		return ecsLinks;
	}

	public void setEcsLinks(
			HashMap<ExtendedCharacteristicSet, HashSet<ExtendedCharacteristicSet>> ecsLinks) {
		this.ecsLinks = ecsLinks;
	}

	LinkedHashSet<ECSJoin> joinList;

	public ECSQuery(Query query){
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

		listSet = new LinkedHashSet<LinkedHashSet<ExtendedCharacteristicSet>>();
		
		Element pattern = query.getQueryPattern();

		ElementGroup g = (ElementGroup) pattern ;

		ElementPathBlock triplePathBlock = (ElementPathBlock) g.getElements().get(0);

		HashMap<Node, HashSet<Resource>> subjectMap = new HashMap<Node, HashSet<Resource>>();
		
		HashMap<Node, HashSet<ExtendedCharacteristicSet>> subjectECSMap = new HashMap<Node, HashSet<ExtendedCharacteristicSet>>();

		HashMap<Node, HashSet<Resource>> objectMap = new HashMap<Node, HashSet<Resource>>();

		HashMap<Node, HashSet<TripleAsInt>> varBindings = new HashMap<Node, HashSet<TripleAsInt>>();
		subjectBindsMap = new HashMap<Node, HashMap<Integer,Integer>>();
		HashMap<Node, HashMap<Integer, Integer>> objectBindsMap = new HashMap<Node, HashMap<Integer,Integer>>();
		ecsTree = new ECSTree(null);
		
		for(TriplePath triplePath : triplePathBlock.getPattern().getList()){

			Triple triple = triplePath.asTriple();

			if(subjectMap.containsKey(triple.getSubject())){

				subjectMap.get(triple.getSubject()).add(ResourceFactory.createResource(triple.getPredicate().getURI()));

			}
			else{
				HashSet<Resource> properties = new HashSet<Resource>();
				properties.add(ResourceFactory.createResource(triple.getPredicate().getURI()));
				subjectMap.put(triple.getSubject(), properties);
			}			
			if(!triple.getObject().isVariable()){
				if(subjectBindsMap.containsKey(triple.getSubject())){
					subjectBindsMap.get(triple.getSubject())
						.put(QueryTests.propertiesSet.get(triple.getPredicate().toString()), 
								QueryTests.intMap.get(triple.getObject().toString()));
				}
				else{
					HashMap<Integer, Integer> d = new HashMap<Integer, Integer>();
					d.put(QueryTests.propertiesSet.get(triple.getPredicate().toString()), 
								QueryTests.intMap.get(triple.getObject().toString()));
					subjectBindsMap.put(triple.getSubject(), d);
				}
			}
			
		}
		
		System.out.println(subjectBindsMap.toString());

		HashSet<ExtendedCharacteristicSet> ecsSet = new HashSet<ExtendedCharacteristicSet>();
		
		HashMap<Triple, ExtendedCharacteristicSet> reverseTripleMap = new HashMap<Triple, ExtendedCharacteristicSet>();

		HashMap<ExtendedCharacteristicSet, HashSet<Triple>> ecsTripleMap = new HashMap<ExtendedCharacteristicSet, HashSet<Triple>>();

		HashMap<ExtendedCharacteristicSet, ECSTree> ecsTreeMap = new HashMap<ExtendedCharacteristicSet, ECSTree>();
		
		for(TriplePath triplePath : triplePathBlock.getPattern().getList()){

			Triple triple = triplePath.asTriple();
		/*	TIntHashSet newh = new TIntHashSet();
			for(Resource propres : subjectMap.get(triple.getSubject())){
				
			}*/
			CharacteristicSet subjectCS = new CharacteristicSet(triple.getSubject(), subjectMap.get(triple.getSubject()));
			CharacteristicSet objectCS = null;
	 		if(subjectMap.containsKey(triple.getObject())){
	 			objectCS = new CharacteristicSet(triple.getObject(), subjectMap.get(triple.getObject()));
	 		}
	 		ExtendedCharacteristicSet ecs = new ExtendedCharacteristicSet(subjectCS, objectCS);	 		
	 		ecs.subject = triple.getSubject();
	 		ecs.predicate = triple.getPredicate();
	 		ecs.object = triple.getObject();
	 		ecsSet.add(ecs);
	 		/*ECSTree thisTree = new ECSTree(ecs);
	 		ecsTree.addChild(thisTree);
	 		ecsTreeMap.put(ecs, thisTree);*/
	 		reverseTripleMap.put(triple, ecs);
	 		if(subjectECSMap.containsKey(triple.getSubject())){
	 			subjectECSMap.get(triple.getSubject()).add(ecs);
	 		}
	 		else{
	 			HashSet<ExtendedCharacteristicSet> dummy = new HashSet<ExtendedCharacteristicSet>();
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
 		
		ecsLinks = new HashMap<ExtendedCharacteristicSet, HashSet<ExtendedCharacteristicSet>>();
		for(ExtendedCharacteristicSet e : ecsSet){
			ecsLinks.put(e, new LinkedHashSet<ExtendedCharacteristicSet>());
		}
		ecsVerticalLinks = new HashMap<ExtendedCharacteristicSet, HashSet<ExtendedCharacteristicSet>>(); 
		for(TriplePath triplePath : triplePathBlock.getPattern().getList()){
			
			Triple triple = triplePath.asTriple();	
			
			Node subject = triple.getSubject();
			
			Node object = triple.getObject();

	 		if(subjectMap.containsKey(object)){
	 			ExtendedCharacteristicSet ecs = reverseTripleMap.get(triple);
	 			//TripleTree tripleTree = new TripleTree(triple);
	 			/*TripleTree tripleTree = new TripleTree(ecs, triple);
	 			tree.addChild(ecs, tripleTree);
	 			tripleTreeMap.put(triple, tripleTree);*/
	 			
	 			ECSTree thisTree = new ECSTree(ecs);
		 		ecsTree.addChild(thisTree);
		 		ecsTreeMap.put(ecs, thisTree);
	 			
	 		}
	 		ExtendedCharacteristicSet subjectECS = reverseTripleMap.get(triple);
	 		if(subjectECSMap.containsKey(object)){
	 				
	 			HashSet<ExtendedCharacteristicSet> objectECSSet = subjectECSMap.get(object);
	 			 				 				
	 			if(ecsLinks.containsKey(subjectECS)){
	 				ecsLinks.get(subjectECS).addAll(objectECSSet);
	 			}
	 			else{
	 				HashSet<ExtendedCharacteristicSet> dummy = new HashSet<ExtendedCharacteristicSet>();
	 				dummy.addAll(objectECSSet);
	 				ecsLinks.put(subjectECS, objectECSSet);
	 			} 				
	 		}
	 		
	 		if(subjectECSMap.containsKey(subject)){
 				
	 			HashSet<ExtendedCharacteristicSet> subjectECSSet = subjectECSMap.get(subject);
	 			 				 				
	 			if(ecsVerticalLinks.containsKey(subjectECS)){
	 				ecsVerticalLinks.get(subjectECS).addAll(subjectECSSet);
	 			}
	 			else{
	 				HashSet<ExtendedCharacteristicSet> dummy = new HashSet<ExtendedCharacteristicSet>();
	 				dummy.addAll(subjectECSSet);
	 				ecsVerticalLinks.put(subjectECS, subjectECSSet);
	 			} 				
	 		}
			
		}
		
		for(ExtendedCharacteristicSet ecs : ecsLinks.keySet()){
			
			ECSTree leftECS = ecsTreeMap.get(ecs);
			/*
			if(ecsLinks.get(ecs).isEmpty()){
				ecsTree.addChild(new ECSTree(ecs));
			}
			else*/
			//System.out.println("\ttriples: " + ecsTripleMap.get(ecs).toString());
			for(ExtendedCharacteristicSet link : ecsLinks.get(ecs)){
				ECSTree rightECS = ecsTreeMap.get(link);
				leftECS.addChild(rightECS);
				ecsTree.removeChild(rightECS);				
				/*System.out.println("\t: " + link.properties.toString());
				System.out.println(leftECS.children.toString());*/
			}
		}		

		/*if(ecsTree.children.size() == 1){
			LinkedHashSet<ExtendedCharacteristicSet> l = new LinkedHashSet<ExtendedCharacteristicSet>();
			for(ECSTree child : ecsTree.children){
				l.add(child.root);
			}
			listSet.add(l);
		}
		else*/
		for(ECSTree child : ecsTree.children){
			//System.out.println("starting next");
			//iterativeDFS(child);
			recursiveDFS(child, new LinkedHashSet<ExtendedCharacteristicSet>());
		}
	
		HashSet<LinkedHashSet<ExtendedCharacteristicSet>> contained = new LinkedHashSet<LinkedHashSet<ExtendedCharacteristicSet>>();
		for(LinkedHashSet<ExtendedCharacteristicSet> list : listSet){
			
			for(LinkedHashSet<ExtendedCharacteristicSet> list2 : listSet){
				if(list.containsAll(list2) && list!=list2)
			
					contained.add(list2);
				
			}
		}
		for(LinkedHashSet<ExtendedCharacteristicSet> containedList : contained){
			listSet.remove(containedList);
		}
		for(LinkedHashSet<ExtendedCharacteristicSet> list : listSet){
			//System.out.println("next list");
			for(ExtendedCharacteristicSet next : list){
				next.subjectBinds = subjectBindsMap.get(next.subject);
				next.objectBinds = subjectBindsMap.get(next.object);
				
			}
		}		
		
	}
	
	
	public ECSTree getEcsTree() {
		return ecsTree;
	}

	public void setEcsTree(ECSTree ecsTree) {
		this.ecsTree = ecsTree;
	}

	static public void recursiveDFS(ECSTree cur, LinkedHashSet<ExtendedCharacteristicSet> list){
		
		
		if(cur == null || cur.children.isEmpty() || visited.contains(cur)){
			listSet.add(list);
			return;
		}
		
		visited.add(cur);
		list.add(cur.root);
		for(ECSTree child : cur.children){
			
			LinkedHashSet<ExtendedCharacteristicSet> dummy = new LinkedHashSet<ExtendedCharacteristicSet>();
			dummy.addAll(list);
			recursiveDFS(child, dummy);
			
		}
		
	}
	
	static public void iterativeDFS(ECSTree tree){
		
		Stack<ECSTree> stack = new Stack<ECSTree>();
		stack.push(tree);
		HashSet<ECSTree> visited  = new HashSet<ECSTree>();		
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
			for(ECSTree child : tree.children){
				//System.out.println("\t"+child.root.properties.toString() );
				if(!visited.contains(child))
					stack.push(child);
			}
		}
		
	}

}