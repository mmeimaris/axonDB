package com.athena.imis.models;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class ECSTree implements Iterable<ECSTree> {

	  public Set<ECSTree> children;

	  public ExtendedCharacteristicSet root;
	  
	  public ECSTree(ExtendedCharacteristicSet root) {
		  
		  this.root = root;
		  children = new HashSet<ECSTree>();
	  }

	  public boolean addChild(ECSTree n) {
	    return children.add(n);
	  }

	  public boolean removeChild(ECSTree n) {
	    return children.remove(n);
	  }

	  public Iterator<ECSTree> iterator() {
	    return children.iterator();
	  }
	
	 
	  		
}
