package com.athena.imis.models;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class BigECSTree implements Iterable<BigECSTree> {

	  public Set<BigECSTree> children;

	  public BigExtendedCharacteristicSet root;
	  
	  public BigECSTree(BigExtendedCharacteristicSet root) {
		  
		  this.root = root;
		  children = new HashSet<BigECSTree>();
	  }

	  public boolean addChild(BigECSTree n) {
	    return children.add(n);
	  }

	  public boolean removeChild(BigECSTree n) {
	    return children.remove(n);
	  }

	  public Iterator<BigECSTree> iterator() {
	    return children.iterator();
	  }
	
	 
	  		
}
