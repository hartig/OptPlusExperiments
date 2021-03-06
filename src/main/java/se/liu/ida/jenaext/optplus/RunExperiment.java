package se.liu.ida.jenaext.optplus;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
	static final public long TIMEOUT_IN_SECONDS = 10L;

	static { JenaSystem.init(); }

    final protected ModContext modContext = new ModContext();
    final protected ArgDecl argQueryIDFile      = new ArgDecl(ArgDecl.HasValue, "queryids");
    final protected ArgDecl argQueryDir         = new ArgDecl(ArgDecl.HasValue, "querydir");
    final protected ArgDecl argHDTFile          = new ArgDecl(ArgDecl.HasValue, "hdt", "hdtfile");
    final protected ArgDecl argWarmupsPerQuery  = new ArgDecl(ArgDecl.HasValue, "warmupsPerQuery");

    protected File queryidFile;
    protected File queriesDir;
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
        super.add( argQueryIDFile, "--queryids", "CSV file whose first column lists the IDs of the SPARQL queries to be used for the experiment" );
        super.add( argQueryDir, "--querydir", "Directory that contains the subdirectories with the query files" );
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
        return getCommandName() + " --hdtfile=file --queryids=file";
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

        if ( ! hasArg(argQueryIDFile) ) {
        	cmdError("No query ID file specified");
        }

        queryidFile = new File( getValue(argQueryIDFile) );
        if ( ! queryidFile.exists() ) {
        	cmdError("The specified query ID file does not exist");
        }
        if ( ! queryidFile.canRead() ) {
        	cmdError("The specified query ID file cannot be read");
        }

        if ( ! hasArg(argQueryDir) ) {
        	cmdError("No query directory specified");
        }

        queriesDir = new File( getValue(argQueryDir) );
        if ( ! queriesDir.exists() ) {
        	cmdError("The specified query directory does not exist");
        }
        if ( ! queriesDir.canRead() ) {
        	cmdError("The specified query directory cannot be read");
        }
        if ( ! queriesDir.isDirectory() ) {
        	cmdError("The specified query directory is not a directory");
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

    	performExperiment("Opt", false, null);
    	performExperiment("Opt", true, "QueryIterNLJPlusWithOuterLoopOverMaterializedLeft");    	
    	performExperiment("OptP", false, null);
    	performExperiment("Opt", true, "QueryIterNestedLoopJoinPlus");
    	performExperiment("Opt", true, "QueryIterHashJoinPlusMaterializeLeftOnTheFly");
    	performExperiment("Opt", true, "QueryIterHashJoinPlusMaterializeRightFirst");
//    	performExperiment("Opt", true, "QueryIterHashJoinPlusMaterializeLeftFirst");
//    	performExperiment("Opt", true, "QueryIterNestedLoopJoinPlusMaterializeLeftOnTheFly");
//    	performExperiment("Opt", true, "QueryIterNestedLoopJoinPlusMaterializeRightFirst");
//    	performExperiment("Opt", true, "QueryIterSubsumptionAwareJoinPlus");
    }

    protected void performExperiment( String filenamePrefix, boolean useOptPlusSemantics, String classnameOptPlusIterator )
    {
    	ARQ.getContext().set(QueryEnginePlus.useOptPlusSemantics, useOptPlusSemantics);
    	ARQ.getContext().set(QueryEnginePlus.classnameOptPlusIterator, classnameOptPlusIterator);

    	final String outfileName = filenamePrefix + "-" + useOptPlusSemantics + "-" + classnameOptPlusIterator + ".csv";
    	final File csvOutputFile = new File( "measurements-" + outfileName );

    	System.out.println(outfileName);

        BufferedReader br = null;
        PrintWriter w = null;

        try
        {
        	w = new PrintWriter(csvOutputFile);

        	final FileReader fr = new FileReader(queryidFile);
            br = new BufferedReader(fr);
            for ( String line; (line = br.readLine()) != null; )
            {
            	final String[] splittedLine = line.split(",");
            	final String queryID = splittedLine[0].trim();

            	final String csv = runQuery(filenamePrefix, queryID, outfileName);
            	w.println(csv);
            	w.flush();
            }
        }
        catch ( IOException e ) {
        	System.err.println( e.getMessage() );
        }
        finally
        {
        	if ( w != null ) {
        		w.flush();
        		w.close();
        	}

        	if ( br != null ) {
        		try {
        			br.close();
        		}
        		catch ( IOException e ) {
                	System.err.println( e.getMessage() );
        		}
        	}
        }
    }

    protected String runQuery( String filenamePrefix, String queryID, String outfileName )
    {
    	final File queryDir = new File(queriesDir, queryID);
//    	if ( ! queryDir.exists() || queryDir.canRead() )
//    		return queryID + ", ERROR: directory with the query files does not exist or cannot be read " + queryDir.toString();

    	final File queryFile = new File(queryDir, filenamePrefix + "_file" + queryID + ".txt");
//    	if ( ! queryFile.exists() || queryFile.canRead() )
//    		return queryID + ", ERROR: the query file does not exist or cannot be read";

    	final Query query;
    	try
    	{
    		query = QueryFactory.read( queryFile.getPath() );
    	}
    	catch ( JenaException e )
    	{
    		return queryID + ", ERROR: reading the query failed (" + e.getMessage() + ")";
    	}

    	final File csvfileTimesUntilSolutions    = new File( queryDir, "TimesUntilSolutions-" + outfileName );
    	final File csvfileAccessesUntilSolutions = new File( queryDir, "AccessesUntilSolutions-" + outfileName );

    	try {
    		return runQuery(query, queryID, csvfileTimesUntilSolutions, csvfileAccessesUntilSolutions);
    	}
    	catch ( Throwable e )
    	{
    		if ( e instanceof OutOfMemoryError )
    			System.gc();

    		return queryID + ", ERROR: caught " + e.getClass().getName() + " when running query (" + e.getMessage() + ")";
    	}
    }

    protected String runQuery( Query q,
                               String queryID,
                               File csvfileTimesUntilSolutions,
                               File csvfileAccessesUntilSolutions )
    {
    	try
    	{
        	int solutionCounter = 0;
        	for ( int i=0; i < warmupsPerQuery; ++i )
        		solutionCounter = warmupQueryExecWithTimeout(q);

        	return measureQueryExecWithTimeout(q, queryID, solutionCounter, csvfileTimesUntilSolutions, csvfileAccessesUntilSolutions);
    	}
    	catch ( InterruptedException e ) {
    		return queryID + ", ERROR: timeout thread interrupted (" + e.getMessage() + ")";
    	}
    	catch ( ExecutionException e ) {
    		return queryID + ", ERROR: caught " + e.getClass().getName() + " when executing timeout thread (" + e.getMessage() + ")";
    	}
    	catch ( TimeoutException e ) {
    		return queryID + ", TIMEOUT, " + TIMEOUT_IN_SECONDS + " seconds";
    	}
    }

    protected int warmupQueryExecWithTimeout( final Query q ) throws InterruptedException, ExecutionException, TimeoutException
    {
    	final Callable<Integer> c = new Callable<Integer>() {
    		@Override
	        public Integer call() throws Exception {
	            return warmupQueryExec(q);
	        }
    	};

    	return runWithTimeout(c);
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

    protected String measureQueryExecWithTimeout( final Query q,
                                                  final String queryID,
                                                  final int solutionCounter,
                                                  final File csvfileTimesUntilSolutions,
                                                  final File csvfileAccessesUntilSolutions )
                                    		   throws InterruptedException, ExecutionException, TimeoutException
    {
    	final Callable<String> c = new Callable<String>() {
    		@Override
	        public String call() throws Exception {
	            return measureQueryExec(q, queryID, solutionCounter, csvfileTimesUntilSolutions, csvfileAccessesUntilSolutions);
	        }
    	};

    	return runWithTimeout(c);
    }

    protected String measureQueryExec( Query q,
                                       String queryID,
                                       int solutionCounter,
                                       File csvfileTimesUntilSolutions,
                                       File csvfileAccessesUntilSolutions )
    {
    	instrumentedGraph.resetReadAccessCounter();

    	final long[] timesUntilSolutions    = new long[solutionCounter];
    	final long[] accessesUntilSolutions = new long[solutionCounter];
    	final long[] triplesUntilSolutions  = new long[solutionCounter];
    	int i = 0;

    	System.gc();

    	final long startTime = System.nanoTime();
        final QueryExecution qe = QueryExecutionFactory.create(q, dataset);
    	final long timeAfterCreate = System.nanoTime();
        final ResultSet rs = qe.execSelect();
        while ( rs.hasNext() )
        {
        	rs.next();

        	timesUntilSolutions[i]    = ( System.nanoTime() - startTime );
        	accessesUntilSolutions[i] = instrumentedGraph.getReadAccessCounter();
        	triplesUntilSolutions[i]  = instrumentedGraph.getTriplesCounter();

        	i++; 
        }

    	final long endTime = System.nanoTime();

    	System.gc();

    	final long overallAccesses = instrumentedGraph.getReadAccessCounter();
    	final long overallTriples  = instrumentedGraph.getTriplesCounter();
    	final long overallTime  = endTime - startTime;
    	final long creationTime = timeAfterCreate - startTime;
    	final long execTime     = endTime - timeAfterCreate;

    	final long[] timeToPercentageOfResult     = new long[10];
    	final long[] accessesToPercentageOfResult = new long[10];
    	final long[] triplesToPercentageOfResult  = new long[10];
    	if ( solutionCounter > 0 )
    	{
    		for ( int j=1; j<11; ++j ) {
    			final int tmp = (int) Math.ceil( (j*solutionCounter)/10d );
        		final int arrayIndex = Math.min(tmp, solutionCounter-1);
        		timeToPercentageOfResult[j-1]     = timesUntilSolutions[arrayIndex];
        		accessesToPercentageOfResult[j-1] = accessesUntilSolutions[arrayIndex];
        		triplesToPercentageOfResult[j-1]  = triplesUntilSolutions[arrayIndex];
        	}
    	}
    	else
    	{
    		for ( int j=1; j<11; ++j ) {
        		timeToPercentageOfResult[j-1]     = 0L;
        		accessesToPercentageOfResult[j-1] = 0L;
        		triplesToPercentageOfResult[j-1]  = 0L;
        	}
    	}

    	final long[] timeTo10Mappings     = new long[10];
    	final long[] accessesTo10Mappings = new long[10];
    	final long[] triplesTo10Mappings  = new long[10];
    	if ( solutionCounter >= 100 )
    	{
    		for ( int j=1; j<11; ++j ) {
        		timeTo10Mappings[j-1]     = timesUntilSolutions[j*10];
        		accessesTo10Mappings[j-1] = accessesUntilSolutions[j*10];
        		triplesTo10Mappings[j-1]  = triplesUntilSolutions[j*10];
        	}
    	}
    	else
    	{
    		for ( int j=1; j<11; ++j ) {
    			timeTo10Mappings[j-1]     = 0L;
    			accessesTo10Mappings[j-1] = 0L;
    			triplesTo10Mappings[j-1]  = 0L;
        	}
    	}

    	final String csv = queryID
    	                   + ", " + solutionCounter
    	                   + ", " + overallAccesses
    	                   + ", " + overallTriples
    	                   + ", " + overallTime/1000000d
    	                   + ", " + creationTime/1000000d
    	                   + ", " + execTime/1000000d
    	                   + ", "
    	                   + ", " + timeToPercentageOfResult[0]/1000000d
    	                   + ", " + timeToPercentageOfResult[1]/1000000d
    	                   + ", " + timeToPercentageOfResult[2]/1000000d
    	                   + ", " + timeToPercentageOfResult[3]/1000000d
    	                   + ", " + timeToPercentageOfResult[4]/1000000d
    	                   + ", " + timeToPercentageOfResult[5]/1000000d
    	                   + ", " + timeToPercentageOfResult[6]/1000000d
    	                   + ", " + timeToPercentageOfResult[7]/1000000d
    	                   + ", " + timeToPercentageOfResult[8]/1000000d
    	                   + ", " + timeToPercentageOfResult[9]/1000000d
    	                   + ", "
    	                   + ", " + accessesToPercentageOfResult[0]
    	                   + ", " + accessesToPercentageOfResult[1]
    	                   + ", " + accessesToPercentageOfResult[2]
    	                   + ", " + accessesToPercentageOfResult[3]
    	                   + ", " + accessesToPercentageOfResult[4]
    	                   + ", " + accessesToPercentageOfResult[5]
    	                   + ", " + accessesToPercentageOfResult[6]
    	                   + ", " + accessesToPercentageOfResult[7]
    	                   + ", " + accessesToPercentageOfResult[8]
    	                   + ", " + accessesToPercentageOfResult[9]
    	                   + ", "
    	                   + ", " + triplesToPercentageOfResult[0]
    	                   + ", " + triplesToPercentageOfResult[1]
    	                   + ", " + triplesToPercentageOfResult[2]
    	                   + ", " + triplesToPercentageOfResult[3]
    	                   + ", " + triplesToPercentageOfResult[4]
    	                   + ", " + triplesToPercentageOfResult[5]
    	                   + ", " + triplesToPercentageOfResult[6]
    	                   + ", " + triplesToPercentageOfResult[7]
    	                   + ", " + triplesToPercentageOfResult[8]
    	                   + ", " + triplesToPercentageOfResult[9]
    	                   + ", "
    	                   + ", " + timeTo10Mappings[0]/1000000d
    	                   + ", " + timeTo10Mappings[1]/1000000d
    	                   + ", " + timeTo10Mappings[2]/1000000d
    	                   + ", " + timeTo10Mappings[3]/1000000d
    	                   + ", " + timeTo10Mappings[4]/1000000d
    	                   + ", " + timeTo10Mappings[5]/1000000d
    	                   + ", " + timeTo10Mappings[6]/1000000d
    	                   + ", " + timeTo10Mappings[7]/1000000d
    	                   + ", " + timeTo10Mappings[8]/1000000d
    	                   + ", " + timeTo10Mappings[9]/1000000d
    	                   + ", "
    	                   + ", " + accessesTo10Mappings[0]
    	                   + ", " + accessesTo10Mappings[1]
    	                   + ", " + accessesTo10Mappings[2]
    	                   + ", " + accessesTo10Mappings[3]
    	                   + ", " + accessesTo10Mappings[4]
    	                   + ", " + accessesTo10Mappings[5]
    	                   + ", " + accessesTo10Mappings[6]
    	                   + ", " + accessesTo10Mappings[7]
    	                   + ", " + accessesTo10Mappings[8]
    	                   + ", " + accessesTo10Mappings[9]
    	                   + ", "
    	                   + ", " + triplesTo10Mappings[0]
    	                   + ", " + triplesTo10Mappings[1]
    	                   + ", " + triplesTo10Mappings[2]
    	                   + ", " + triplesTo10Mappings[3]
    	                   + ", " + triplesTo10Mappings[4]
    	                   + ", " + triplesTo10Mappings[5]
    	                   + ", " + triplesTo10Mappings[6]
    	                   + ", " + triplesTo10Mappings[7]
    	                   + ", " + triplesTo10Mappings[8]
    	                   + ", " + triplesTo10Mappings[9];

/*
		PrintWriter w1 = null;
		PrintWriter w2 = null;

    	try
    	{
    		w1 = new PrintWriter(csvfileTimesUntilSolutions);
    		w2 = new PrintWriter(csvfileAccessesUntilSolutions);

    		w1.println( queryID + ", " + overallTime/1000000d );
    		w2.println( queryID + ", " + overallAccesses );

    		for ( int j=0; j < solutionCounter; ++j )
    		{
    			w1.println( " , , " + timesUntilSolutions[j]/1000000d );
    			w2.println( " , , " + accessesUntilSolutions[j] );
    		}
    	}
    	catch ( Exception e ) {
        	System.err.println( e.getMessage() );
        }
        finally
        {
        	if ( w1 != null ) {
        		w1.flush();
        		w1.close();
        	}

        	if ( w2 != null ) {
        		w2.flush();
        		w2.close();
        	}
        }
*/

    	return csv;
    }


    @SuppressWarnings("deprecation")
    static protected <T> T runWithTimeout( Callable<T> c ) throws InterruptedException, ExecutionException, TimeoutException
    {
    	final FutureTask<T> timeoutTask = new FutureTask<T>(c);
    	final Thread thread = new Thread(timeoutTask);
    	thread.start();

    	T result;
    	try
    	{
        	result = timeoutTask.get( TIMEOUT_IN_SECONDS, TimeUnit.SECONDS );
    	}
    	finally
    	{
    		timeoutTask.cancel(true);

    		if ( thread.isAlive() )
    			thread.stop();
    	}

    	return result;
    }

}
