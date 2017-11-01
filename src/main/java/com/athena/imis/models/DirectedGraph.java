package com.athena.imis.models;

/*****************************************************************************
 * File: DirectedGraph.java
 * Author: Keith Schwarz (htiek@cs.stanford.edu)
 *
 * A class representing a directed graph.  Internally, the class is represented
 * by an adjacency list.
 */
import java.util.*; // For HashMap, HashSet

public final class DirectedGraph<T> implements Iterable<T> {
    /* A map from nodes in the graph to sets of outgoing edges.  Each
     * set of edges is represented by a map from edges to doubles.
     */
    Map<T, Set<T>> mGraph = new HashMap<T, Set<T>>();

    public Map<T, Set<T>> getmGraph() {
		return mGraph;
	}

	public void setmGraph(Map<T, Set<T>> mGraph) {
		this.mGraph = mGraph;
	}

	/**
     * Adds a new node to the graph.  If the node already exists, this
     * function is a no-op.
     *
     * @param node The node to add.
     * @return Whether or not the node was added.
     */
    public boolean addNode(T node) {
        /* If the node already exists, don't do anything. */
        if (mGraph.containsKey(node))
            return false;

        /* Otherwise, add the node with an empty set of outgoing edges. */
        mGraph.put(node, new HashSet<T>());
        return true;
    }

    public boolean isSink(T node) {
    	if(mGraph.containsKey(node)){
    		if(edgesFrom(node).isEmpty())
    			return true;
    	}
    	return false;
    }
    
    public Set<T> findRoots(){
    	Set<T> candidates = new HashSet<T>(mGraph.keySet());
    	for(T nextNode : mGraph.keySet()){
    		for(T nextConnectedNode : mGraph.get(nextNode)){
    			candidates.remove(nextConnectedNode);
    		}
    	}
    	return candidates ;
    }
    /**
     * Given a start node, and a destination, adds an arc from the start node 
     * to the destination.  If an arc already exists, this operation is a 
     * no-op.  If either endpoint does not exist in the graph, throws a 
     * NoSuchElementException.
     *
     * @param start The start node.
     * @param dest The destination node.
     * @throws NoSuchElementException If either the start or destination nodes
     *                                do not exist.
     */
    public void addEdge(T start, T dest) {
        /* Confirm both endpoints exist. */
        if (!mGraph.containsKey(start) || !mGraph.containsKey(dest))
            throw new NoSuchElementException("Both nodes must be in the graph.");

        /* Add the edge. */
        mGraph.get(start).add(dest);
    }

    /**
     * Removes the edge from start to dest from the graph.  If the edge does
     * not exist, this operation is a no-op.  If either endpoint does not
     * exist, this throws a NoSuchElementException.
     *
     * @param start The start node.
     * @param dest The destination node.
     * @throws NoSuchElementException If either node is not in the graph.
     */
    public void removeEdge(T start, T dest) {
        /* Confirm both endpoints exist. */
        if (!mGraph.containsKey(start) || !mGraph.containsKey(dest))
            throw new NoSuchElementException("Both nodes must be in the graph.");

        mGraph.get(start).remove(dest);
    }

    /**
     * Given two nodes in the graph, returns whether there is an edge from the
     * first node to the second node.  If either node does not exist in the
     * graph, throws a NoSuchElementException.
     *
     * @param start The start node.
     * @param end The destination node.
     * @return Whether there is an edge from start to end.
     * @throws NoSuchElementException If either endpoint does not exist.
     */
    public boolean edgeExists(T start, T end) {
        /* Confirm both endpoints exist. */
        if (!mGraph.containsKey(start) || !mGraph.containsKey(end))
            throw new NoSuchElementException("Both nodes must be in the graph.");

        return mGraph.get(start).contains(end);
    }

    /**
     * Given a node in the graph, returns an immutable view of the edges
     * leaving that node as a set of endpoints.
     *
     * @param node The node whose edges should be queried.
     * @return An immutable view of the edges leaving that node.
     * @throws NoSuchElementException If the node does not exist.
     */
    public Set<T> edgesFrom(T node) {
        /* Check that the node exists. */
        Set<T> arcs = mGraph.get(node);
        if (arcs == null)
            throw new NoSuchElementException("Source node does not exist.");

        return Collections.unmodifiableSet(arcs);
    }

    /**
     * Returns an iterator that can traverse the nodes in the graph.
     *
     * @return An iterator that traverses the nodes in the graph.
     */
    public Iterator<T> iterator() {
        return mGraph.keySet().iterator();
    }
}