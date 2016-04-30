package com.athena.imis.models;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class BigAnswerPattern {

	
	public BigECSTuple root ;
	
	public BigQueryPattern BigQueryPattern ;
	
	public Set<BigAnswerPattern> children ;
	
	public BigAnswerPattern(BigECSTuple root, BigQueryPattern BigQueryPattern){
		this.root = root;
		this.BigQueryPattern = BigQueryPattern;
		children = new HashSet<BigAnswerPattern>();
	}
			
	public void addChild(BigAnswerPattern child){
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
       if (!(obj instanceof BigAnswerPattern))
            return false;
        if (obj == this)
            return true;

        BigAnswerPattern rhs = (BigAnswerPattern) obj;
        return new EqualsBuilder().            
        	append(root, rhs.root).
            isEquals();
    }
	
	@Override
	public String toString(){
		return root.toString();
	}
	
}
