package se.liu.ida.jenaext.optplus.graph;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.graph.GraphWrapper;
import org.apache.jena.util.iterator.ExtendedIterator;

/**
 * 
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
public class ExperimentGraph extends GraphWrapper
{
	protected long readAccessCounter = 0L;

	public ExperimentGraph( Graph wrappedGraph ) { super(wrappedGraph); } 

	public long getReadAccessCounter() { return readAccessCounter; }

	public void resetReadAccessCounter() { readAccessCounter = 0L; }

    @Override
    public ExtendedIterator<Triple> find( Triple triple )
    {
    	readAccessCounter++;
    	return super.find(triple);
    }

    @Override
    public ExtendedIterator<Triple> find( Node s, Node p, Node o )
    {
    	readAccessCounter++;
    	return super.find(s, p, o);
    }

    @Override
    public boolean contains( Node s, Node p, Node o )
    {
    	readAccessCounter++;
    	return super.contains(s, p, o);
    }

    @Override
    public boolean contains( Triple t )
    {
    	readAccessCounter++;
    	return super.contains(t);
    }

}
