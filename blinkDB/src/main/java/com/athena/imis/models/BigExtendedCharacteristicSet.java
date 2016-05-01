package com.athena.imis.models;

import java.io.Serializable;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Resource;

public class BigExtendedCharacteristicSet implements Serializable {
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 5635769376268161667L;
	/**
	 * 
	 */
	
	public transient Node subject;//, predicate;
	public transient Node predicate;
	public transient Node object;
	public transient HashMap<Integer, Integer> subjectBinds;
	public transient HashMap<Integer, Integer> objectBinds;
	public BigCharacteristicSet subjectCS ;
	public BigCharacteristicSet objectCS;
	BitSet longRep;
	public BitSet getLongRep() {
		return longRep;
	}

	public void setLongRep(BitSet longRep) {
		this.longRep = longRep;
	}

	//HashSet<Resource> properties;
	
	
	public BigExtendedCharacteristicSet(Node subject, Resource predicate, Node object, HashSet<Resource> properties){
		
			this.subject = subject;
			//this.predicate = predicate;
			this.object = object;
			//this.properties = properties;
		
	}
	
	public BigExtendedCharacteristicSet(Resource predicate, HashSet<Resource> properties, 
			BigCharacteristicSet subjectCS, BigCharacteristicSet objectCS){
				
		//this.predicate = predicate;
		//this.properties = properties;
		this.subjectCS = subjectCS;
		this.objectCS = objectCS;
		if(objectCS!=null){
			BitSet subjectCSs = (BitSet) subjectCS.getLongRep().clone();
			subjectCSs.or(objectCS.getLongRep());
			setLongRep(subjectCSs);
		}
		else
			setLongRep(subjectCS.getLongRep());
		
	
	}
	
	public BigExtendedCharacteristicSet(BigCharacteristicSet subjectCS, BigCharacteristicSet objectCS) {		
		
		this.subjectCS = subjectCS;
		this.objectCS = objectCS;
		/*this.properties = new HashSet<Resource>();
		properties.addAll(subjectCS.properties);
		if(objectCS!=null){
			properties.addAll(objectCS.properties);
		}*/
		if(objectCS!=null){
			BitSet subjectCSs = (BitSet) subjectCS.getLongRep().clone();
			subjectCSs.or(objectCS.getLongRep());
			setLongRep(subjectCSs);			
		}
		else
			setLongRep(subjectCS.getLongRep());
		
	}
	public void initBinds(){
		this.subjectBinds = new HashMap<Integer, Integer>();
		this.objectBinds = new HashMap<Integer, Integer>();
	}
	@Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers
            // if deriving: appendSuper(super.hashCode()).
            append(subjectCS).
            //append(predicate).
            append(objectCS).
        	/*append(getLongRep()).
        	append(predicate).*/
            toHashCode();
    }
	
	@Override
    public boolean equals(Object obj) {
       if (!(obj instanceof BigExtendedCharacteristicSet))
            return false;
        if (obj == this)
            return true;

        BigExtendedCharacteristicSet rhs = (BigExtendedCharacteristicSet) obj;
        return new EqualsBuilder().
            // if deriving: appendSuper(super.equals(obj)).
            append(subjectCS, rhs.subjectCS).
            //append(predicate, rhs.predicate).
            append(objectCS, rhs.objectCS).
        	//append(getLongRep(), rhs.getLongRep()).
        	//append(predicate, rhs.predicate).
            isEquals();
    }
	
	public void print(){
		
		/*if(objectCS!=null){
			for(int i = 0; i < Long.numberOfLeadingZeros((long)subjectCS.getLongRep()); i++) {
			      System.out.print('0');
			}
			System.out.println(Long.toBinaryString((long)subjectCS.getLongRep()));
			for(int i = 0; i < Long.numberOfLeadingZeros((long)objectCS.getLongRep()); i++) {
			      System.out.print('0');
			}
			System.out.println(Long.toBinaryString((long)objectCS.getLongRep()));
			for(int i = 0; i < Long.numberOfLeadingZeros((long)getLongRep()); i++) {
			      System.out.print('0');
			}
			System.out.println(Long.toBinaryString((long)getLongRep()));
			System.out.println("--------------------------------");
		}
		else{
			for(int i = 0; i < Long.numberOfLeadingZeros((long)subjectCS.getLongRep()); i++) {
			      System.out.print('0');
			}
			System.out.println(Long.toBinaryString((long)subjectCS.getLongRep()));			
			for(int i = 0; i < Long.numberOfLeadingZeros((long)getLongRep()); i++) {
			      System.out.print('0');
			}
			System.out.println(Long.toBinaryString((long)getLongRep()));
			System.out.println("--------------------------------");
		}*/
		
	}

}
