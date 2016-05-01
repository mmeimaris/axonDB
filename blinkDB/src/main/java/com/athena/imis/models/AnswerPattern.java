package com.athena.imis.models;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class AnswerPattern {

	
	public ECSTuple root ;
	
	public QueryPattern queryPattern ;
	
	public Set<AnswerPattern> children ;
	
	public AnswerPattern(ECSTuple root, QueryPattern queryPattern){
		this.root = root;
		this.queryPattern = queryPattern;
		children = new HashSet<AnswerPattern>();
	}
			
	public void addChild(AnswerPattern child){
		children.add(child);		
	}
	
	@Override
    public int hashCode() {		
        return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers            
            append(root).        	
            toHashCode();
    }
	
	@Override
    public boolean equals(Object obj) {
       if (!(obj instanceof AnswerPattern))
            return false;
        if (obj == this)
            return true;

        AnswerPattern rhs = (AnswerPattern) obj;
        return new EqualsBuilder().            
        	append(root, rhs.root).
            isEquals();
    }
	
	@Override
	public String toString(){
		return root.toString();
	}
	
}
