package com.athena.imis.models;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class TripleTree  implements Serializable {

	

		//public ArrayList<TripleTree> children;
		  
		  //public HashMap<Node, Set<TripleTree>> dummy2;
		  
		  //public HashSet<TripleTree> dummy1; 



		/**
	 * 
	 */
	private static final long serialVersionUID = 9133265273928499635L;

		public Map<ExtendedCharacteristicSet, ArrayList<TripleTree>> childrenMap;
		
		public Map<ExtendedCharacteristicSet, ArrayList<TripleTree>> parentsMap;
		  
		/*  public Map<ExtendedCharacteristicSet, Map<Node, Set<TripleTree>>> childrenSubjectIndex;
		  
		  //public Map<ExtendedCharacteristicSet, Map<Node, TripleTree>> childrenObjectIndex;
		  
		  public Map<ExtendedCharacteristicSet, Map<Node, Set<TripleTree>>> getChildrenSubjectIndex() {
			return childrenSubjectIndex;
		}

		public void setChildrenSubjectIndex(
				Map<ExtendedCharacteristicSet, Map<Node, Set<TripleTree>>> childrenSubjectIndex) {
			this.childrenSubjectIndex = childrenSubjectIndex;
		}

		public Map<ExtendedCharacteristicSet, Map<Node, TripleTree>> getChildrenObjectIndex() {
			return childrenObjectIndex;
		}

		public void setChildrenObjectIndex(
				Map<ExtendedCharacteristicSet, Map<Node, TripleTree>> childrenObjectIndex) {
			this.childrenObjectIndex = childrenObjectIndex;
		}
*/
		public Long root;
		  
		 /* public TripleTree(Triple root) {
			  
			  this.root = root;
			  children = new HashSet<TripleTree>();
		  }*/
		  
		  public TripleTree(ExtendedCharacteristicSet ecs, Long root) {
			  
			  this.root = root;
			  childrenMap = new HashMap<ExtendedCharacteristicSet, ArrayList<TripleTree>>();
			  parentsMap = new HashMap<ExtendedCharacteristicSet, ArrayList<TripleTree>>();
			  if(ecs != null){
				  ArrayList<TripleTree> dummy = new ArrayList<TripleTree>();			  
				  childrenMap.put(ecs, dummy);
				  dummy = new ArrayList<TripleTree>();
				  parentsMap.put(ecs, dummy);
			  }
			  //childrenSubjectIndex = new HashMap<ExtendedCharacteristicSet, Map<Node,Set<TripleTree>>>();
		  }

		  /*public boolean addChild(TripleTree n) {
		    return children.add(n);
		  }*/
		  
		  public boolean addChild(ExtendedCharacteristicSet ecs, TripleTree n) {
			    if(childrenMap.containsKey(ecs)){
			    	childrenMap.get(ecs).add(n);
			    	
			    }
			    else{
			    	ArrayList<TripleTree> dummy = new ArrayList<TripleTree>();
			    	dummy.add(n);
			    	childrenMap.put(ecs, dummy);			    	
			    }			  
			    return true;
			  }
		  
		  public boolean addParent(ExtendedCharacteristicSet ecs, TripleTree n) {
			    if(parentsMap.containsKey(ecs)){
			    	parentsMap.get(ecs).add(n);
			    	
			    }
			    else{
			    	ArrayList<TripleTree> dummy = new ArrayList<TripleTree>();
			    	dummy.add(n);
			    	parentsMap.put(ecs, dummy);			    	
			    }			  
			    return true;
			  }

		  /*public boolean removeChild(TripleTree n) {
		    return children.remove(n);
		  }*/

		  public boolean removeChild(ExtendedCharacteristicSet ecs, TripleTree n) {
			  if(childrenMap.containsKey(ecs)){
				  return childrenMap.get(ecs).remove(n);  
			  }
			  else return false;
			    
			  }
		  
		  public boolean removeParent(ExtendedCharacteristicSet ecs, TripleTree n) {
			  if(parentsMap.containsKey(ecs)){
				  return parentsMap.get(ecs).remove(n);  
			  }
			  else return false;
			    
			  }
		  
		 
		  		
}
