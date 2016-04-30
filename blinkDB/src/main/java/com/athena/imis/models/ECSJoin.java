package com.athena.imis.models;

import java.util.HashSet;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class ECSJoin {

	private HashSet<TripleTree> treeSet = new HashSet<TripleTree>();
	
	private ExtendedCharacteristicSet leftECS;
	
	private ExtendedCharacteristicSet rightECS;

	public HashSet<TripleTree> getTreeSet() {
		return treeSet;
	}

	public void setTreeSet(HashSet<TripleTree> treeSet) {
		this.treeSet = treeSet;
	}

	public ExtendedCharacteristicSet getLeftECS() {
		return leftECS;
	}

	public void setLeftECS(ExtendedCharacteristicSet leftECS) {
		this.leftECS = leftECS;
	}

	public ExtendedCharacteristicSet getRightECS() {
		return rightECS;
	}

	public void setRightECS(ExtendedCharacteristicSet rightECS) {
		this.rightECS = rightECS;
	}
	
	public void addTripleTree(TripleTree tree){
		this.treeSet.add(tree);
	}
	
	public void removeTripleTree(TripleTree tree){
		this.treeSet.remove(tree);
	}
	
	@Override
    public int hashCode() {		
        return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers
            // if deriving: appendSuper(super.hashCode()).
            //append(properties).
        	append(leftECS).
        	append(rightECS).
            toHashCode();
    }
	
	@Override
    public boolean equals(Object obj) {
       if (!(obj instanceof ECSJoin))
            return false;
        if (obj == this)
            return true;

        ECSJoin rhs = (ECSJoin) obj;
        return new EqualsBuilder().
            // if deriving: appendSuper(super.equals(obj)).
            //append(properties, rhs.properties).
        	append(leftECS, rhs.leftECS).
        	append(rightECS, rhs.rightECS).
            isEquals();
    }
	
}
