package com.athena.imis.models;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.set.hash.TIntHashSet;

import java.io.Serializable;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Resource;

import com.athena.imis.tests.BigQueryTests;
import com.athena.imis.tests.BigQueryTests;

public class BigCharacteristicSet implements Serializable {



	//Resource resource;
	
	//HashSet<Resource> properties;
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 325939322215525801L;
	/**
	 * 
	 */
	

	transient Node node;
	transient int nodeInt;
	public BitSet longRep;
	
	public BitSet getLongRep() {
		return longRep;
	}

	public void setLongRep(BitSet longRep) {
		this.longRep = longRep;
	}

	public BigCharacteristicSet(Resource node, HashSet<Resource> properties){
		
		//this.resource = node;
		//this.properties = properties;
		BitSet bits = new BitSet();
		for(Resource p : properties){
			
			bits.set(BigQueryTests.propertiesSet.get(p.getURI()));
			
		}
		setLongRep(bits);
		
	}
	
	public BigCharacteristicSet(Node node, HashSet<Node> properties, boolean b){
		
		//this.resource = node;
		//this.properties = properties;
		BitSet bits = new BitSet();
		for(Node p : properties){
			
			//bits.set(BigQueryTests.propertiesSet.get(p.getURI()));
			bits.set(BigQueryTests.propertiesSet.get(p.getURI()));
			
		}
		//setLongRep(convert(bits));
		setLongRep(bits);
		
	}
	
	public BigCharacteristicSet(HashSet<Integer> properties, boolean b){
				
		BitSet bits = new BitSet();
		for(Integer p : properties){

			bits.set(p);
			
		}		
		setLongRep(bits);
		
	}
	
	public BigCharacteristicSet(TIntHashSet properties, boolean b){
		
		BitSet bits = new BitSet();
		TIntIterator it = properties.iterator();
		while(it.hasNext()){

			bits.set(it.next());
			
		}			
		setLongRep(bits); 
		
	}
	
	public BigCharacteristicSet(Node node, TIntHashSet properties){
		
		this.node = node;
		//this.properties = properties;
		BitSet bits = new BitSet();
		TIntIterator it = properties.iterator();
		while(it.hasNext()){

			bits.set(it.next());
			
		}			
		setLongRep(bits); 
		
	}
	
	public BigCharacteristicSet(Node node, HashSet<Resource> properties){
		
		this.node = node;
		//this.properties = properties;
		BitSet bits = new BitSet();
		for(Resource p : properties){
						
			//bits.set(BigQueryTests.propertiesSet.get(p.getURI()));
			/*if(p.getURI().contains("swat"))				
				bits.set(BigQueryTests.propertiesSet.get(p.getURI().replaceAll("#", "##")));
			else
				bits.set(BigQueryTests.propertiesSet.get(p.getURI()));*/
			bits.set(BigQueryTests.propertiesSet.get(p.getURI()));
		}
		//setLongRep(convert(bits));
		setLongRep(bits);
		
	}
	
	@Override
    public int hashCode() {		
        return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers
            // if deriving: appendSuper(super.hashCode()).
            //append(properties).
        	append(getLongRep()).
            toHashCode();
    }
	
	@Override
    public boolean equals(Object obj) {
       if (!(obj instanceof BigCharacteristicSet))
            return false;
        if (obj == this)
            return true;

        BigCharacteristicSet rhs = (BigCharacteristicSet) obj;
        return new EqualsBuilder().
            // if deriving: appendSuper(super.equals(obj)).
            //append(properties, rhs.properties).
        	append(getLongRep(), rhs.getLongRep()).
            isEquals();
    }
	
	public static long convert(BitSet bitset) {
        long value = 0L;
        for (int i = 0; i < bitset.length(); ++i) {
          value += bitset.get(i) ? (1L << i) : 0L;
        }
        return value;
      }
}
