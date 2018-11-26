package se.liu.ida.jenaext.optplus;

import java.io.IOException;

import jena.cmd.ArgDecl;
import jena.cmd.CmdGeneral;

import org.apache.jena.Jena;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.riot.RIOT;
import org.apache.jena.shared.JenaException;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.engine.iterator.QueryIteratorBase;
import org.apache.jena.system.JenaSystem;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdtjena.HDTGraph;

import se.liu.ida.jenaext.optplus.graph.ExperimentGraph;
import se.liu.ida.jenaext.optplus.sparql.engine.main.QueryEnginePlus;
import arq.cmdline.ModContext;

/**
 * 
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
public class RunExperiment extends CmdGeneral
{
	static { JenaSystem.init(); }

    final protected ModContext modContext = new ModContext();
    final protected ArgDecl argQueryFile  = new ArgDecl(ArgDecl.HasValue, "query", "queryfile");
    final protected ArgDecl argHDTFile    = new ArgDecl(ArgDecl.HasValue, "hdt", "hdtfile");
    final protected ArgDecl argWarmupsPerQuery    = new ArgDecl(ArgDecl.HasValue, "warmupsPerQuery");

    protected Query query; 
    protected ExperimentGraph instrumentedGraph;
    protected Dataset dataset;
    protected int warmupsPerQuery = 1;

    public static void main( String... argv )
    {
        new RunExperiment(argv).mainRun();
    }

    public RunExperiment( String[] argv )
    {
    	super(argv);

        modVersion.addClass(Jena.class);
        modVersion.addClass(ARQ.class);
        modVersion.addClass(RIOT.class);

        addModule(modContext);

        super.getUsage().startCategory("Experiment options");
        super.add( argQueryFile, "--queryfile", "File with the SPARQL query to be used for the experiment" );
        super.add( argHDTFile, "--hdtfile", "HDT file with the dataset to be used for the experiment" );
        super.add( argWarmupsPerQuery, "--warmupsPerQuery", "Number of warm-up runs for each query (optional, default is " + warmupsPerQuery + ")" );

        QueryEnginePlus.register();
    }

    @Override
    protected String getCommandName()
    {
        return Lib.className(this);
    }

    @Override
    protected String getSummary()
    {
        return getCommandName() + " --hdtfile=file --queryfile=file";
    }

    @Override
    protected void processModulesAndArgs()
    {
    	super.processModulesAndArgs();

        if ( modGeneral.debug )
        	QueryIteratorBase.traceIterators = true;

        if ( hasArg(argWarmupsPerQuery) ) {
        	try {
        		warmupsPerQuery = Integer.parseInt( getValue(argWarmupsPerQuery) );
        	}
        	catch ( Exception e ) {
        		cmdError("Parsing the given warmupsPerQuery failed: " + e.getMessage() );
        	}
        }

        if ( ! hasArg(argQueryFile) ) {
        	cmdError("No query file specified");
        }

        final String queryFileName = getValue(argQueryFile);
        try {
        	query = QueryFactory.read(queryFileName);
        }
        catch ( JenaException e ) {
        	cmdError( "Reading the query failed: " + e.getMessage() );
        }

        if ( ! hasArg(argHDTFile) ) {
        	cmdError("No HDT file specified");
        }

        final String hdtFileName = getValue(argHDTFile);
        final HDT hdt;
        try {
        	hdt = HDTManager.mapIndexedHDT(hdtFileName, null);
        }
        catch ( IOException e ) {
        	cmdError( "Accessing the HDT file failed: " + e.getMessage() );
        	return;
        }

        instrumentedGraph = new ExperimentGraph( new HDTGraph(hdt) );
        final DatasetGraph dsg = DatasetGraphFactory.create(instrumentedGraph);
        dataset = DatasetFactory.wrap(dsg);
    }

    @Override
    protected void exec()
    {
        QueryEnginePlus.register();
//    	ARQ.getContext().set(QueryEnginePlus.classnameOptPlusIterator, "QueryIterHashJoinPlusMaterializeLeftOnTheFly" );
    	ARQ.getContext().set(QueryEnginePlus.classnameOptPlusIterator, "QueryIterNestedLoopJoinPlus" );

    	final Query q = query;
    	final String queryID = "1";

    	ARQ.getContext().setTrue(QueryEnginePlus.useOptPlusSemantics);
        runQuery(q, queryID);

    	ARQ.getContext().setFalse(QueryEnginePlus.useOptPlusSemantics);
        runQuery(q, queryID);
    }

    protected void runQuery( Query q, String queryID )
    {
    	int solutionCounter = 0;
    	for ( int i=0; i < warmupsPerQuery; ++i )
    		solutionCounter = warmupQueryExec(q);

    	measureQueryExec(q, queryID, solutionCounter);
    }

    protected int warmupQueryExec( Query q )
    {
        final QueryExecution qe = QueryExecutionFactory.create(q, dataset);
        final ResultSet rs = qe.execSelect();
        int solutionCounter = 0;
        while ( rs.hasNext() ) {
        	rs.next();
        	solutionCounter++;
        }
        return solutionCounter;
    }

    protected void measureQueryExec( Query q, String queryID, int solutionCounter )
    {
    	instrumentedGraph.resetReadAccessCounter();

    	final long[] timesUntilSolutions    = new long[solutionCounter];
    	final long[] accessesUntilSolutions = new long[solutionCounter];
    	int i = 0;

    	final long startTime = System.nanoTime();
        final QueryExecution qe = QueryExecutionFactory.create(query, dataset);
    	final long timeAfterCreate = System.nanoTime();
        final ResultSet rs = qe.execSelect();
        while ( rs.hasNext() )
        {
        	rs.next();

        	timesUntilSolutions[i]    = ( System.nanoTime() - startTime );
        	accessesUntilSolutions[i] = instrumentedGraph.getReadAccessCounter();

        	i++; 
        }

    	final long endTime = System.nanoTime();

    	final long overallTime  = endTime - startTime;
    	final long creationTime = timeAfterCreate - startTime;
    	final long execTime     = endTime - timeAfterCreate;
    	final long overallAccesses = instrumentedGraph.getReadAccessCounter();

    	final String csv = queryID
    	                   + ", " + solutionCounter
    	                   + ", " + overallAccesses
    	                   + ", " + overallTime/1000000d
    	                   + ", " + creationTime/1000000d
    	                   + ", " + execTime/1000000d;

    	String csvTimesUntilSolutions    = queryID + ", " + overallTime/1000000d;
    	String csvAccessesUntilSolutions = queryID + ", " + overallAccesses;
    	for ( int j=0; j < solutionCounter; ++j ) {
    		csvTimesUntilSolutions    += "\n , , " + timesUntilSolutions[j]/1000000d;
    		csvAccessesUntilSolutions += "\n , , " + accessesUntilSolutions[j];
    	}
System.out.println( csv );
System.err.println( csvTimesUntilSolutions );
System.out.println( csvAccessesUntilSolutions );
System.out.println();
    }

}
