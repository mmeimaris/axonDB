package com.athena.imis.models;

import java.util.ArrayList;
import java.util.HashSet;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class BigQueryPattern {

	
	public ArrayList<BigECSTuple> queryPattern ;
	public int boundVars = -1;
	
	public BigQueryPattern(ArrayList<BigECSTuple> queryPattern){		
		this.queryPattern = queryPattern;
		HashSet<Integer> bound = new HashSet<Integer>();
		for(BigECSTuple tuple : queryPattern){
			if(tuple.triplePattern.s >=0)
				bound.add(tuple.triplePattern.s);
			if(tuple.triplePattern.o >=0)
				bound.add(tuple.triplePattern.o);
		}
		boundVars = bound.size();
	}
			
	@Override
    public int hashCode() {		
        return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers            
            append(queryPattern).        	
            toHashCode();
    }
	
	@Override
    public boolean equals(Object obj) {
       if (!(obj instanceof BigQueryPattern))
            return false;
        if (obj == this)
            return true;

        BigQueryPattern rhs = (BigQueryPattern) obj;
        return new EqualsBuilder().            
        	append(queryPattern, rhs.queryPattern).
            isEquals();
    }
	
	@Override
	
	public String toString(){
		return this.queryPattern.toString();
	}
}
