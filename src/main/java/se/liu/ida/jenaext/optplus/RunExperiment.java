package se.liu.ida.jenaext.optplus;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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

// Stats CSV file: 1st column is query ID,
// query files are in subdirs named exactly like the query ID
// inside this subdir are three files, one of which is Opt_file<queryID>.txt and the other is OptP_file<queryID>.txt  

//    	performExperiment(true, "QueryIterHashJoinPlusMaterializeLeftOnTheFly");
    	performExperiment(true, "QueryIterNestedLoopJoinPlus");
    	performExperiment(false, null);
    }

    protected void performExperiment( boolean useOptPlusSemantics, String classnameOptPlusIterator )
    {
    	ARQ.getContext().set(QueryEnginePlus.useOptPlusSemantics, useOptPlusSemantics);
    	ARQ.getContext().set(QueryEnginePlus.classnameOptPlusIterator, classnameOptPlusIterator);

        BufferedReader br = null;

        try
        {
        	final FileReader fr = new FileReader(queryidFile);
            br = new BufferedReader(fr);
            for ( String line; (line = br.readLine()) != null; )
            {
            	final String[] splittedLine = line.split(",");
            	final String queryID = splittedLine[0].trim();

            	final String csv = runQuery(queryID);
System.out.println(csv);
            }
        }
        catch ( IOException e ) {
        	System.err.println( e.getMessage() );
        }
        finally
        {
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

    protected String runQuery( String queryID )
    {
    	final File queryDir = new File(queriesDir, queryID);
//    	if ( ! queryDir.exists() || queryDir.canRead() )
//    		return queryID + ", ERROR: directory with the query files does not exist or cannot be read " + queryDir.toString();

    	final File queryFile = new File(queryDir, "Opt_file" + queryID + ".txt");
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

    	return runQuery(query, queryID);
    }

    protected String runQuery( Query q, String queryID )
    {
    	int solutionCounter = 0;
    	for ( int i=0; i < warmupsPerQuery; ++i )
    		solutionCounter = warmupQueryExec(q);

    	return measureQueryExec(q, queryID, solutionCounter);
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

    protected String measureQueryExec( Query q, String queryID, int solutionCounter )
    {
    	instrumentedGraph.resetReadAccessCounter();

    	final long[] timesUntilSolutions    = new long[solutionCounter];
    	final long[] accessesUntilSolutions = new long[solutionCounter];
    	int i = 0;

    	final long startTime = System.nanoTime();
        final QueryExecution qe = QueryExecutionFactory.create(q, dataset);
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

    	final long overallAccesses = instrumentedGraph.getReadAccessCounter();
    	final long overallTime  = endTime - startTime;
    	final long creationTime = timeAfterCreate - startTime;
    	final long execTime     = endTime - timeAfterCreate;

    	final long[] timeToPercentageOfResult     = new long[10];
    	final long[] accessesToPercentageOfResult = new long[10];
    	if ( solutionCounter > 0 )
    	{
    		for ( int j=1; j<11; ++j ) {
    			final int tmp = (int) Math.ceil( (j*solutionCounter)/10d );
        		final int arrayIndex = Math.min(tmp, solutionCounter-1);
        		timeToPercentageOfResult[j-1]     = timesUntilSolutions[arrayIndex];
        		accessesToPercentageOfResult[j-1] = accessesUntilSolutions[arrayIndex];
        	}
    	}
    	else
    	{
    		for ( int j=1; j<11; ++j ) {
        		timeToPercentageOfResult[j-1]     = 0L;
        		accessesToPercentageOfResult[j-1] = 0L;
        	}
    	}
    	

    	final String csv = queryID
    	                   + ", " + solutionCounter
    	                   + ", " + overallAccesses
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
    	                   + ", " + accessesToPercentageOfResult[9];

/*
    	String csvTimesUntilSolutions    = queryID + ", " + overallTime/1000000d;
    	String csvAccessesUntilSolutions = queryID + ", " + overallAccesses;
    	for ( int j=0; j < solutionCounter; ++j ) {
    		csvTimesUntilSolutions    += "\n , , " + timesUntilSolutions[j]/1000000d;
    		csvAccessesUntilSolutions += "\n , , " + accessesUntilSolutions[j];
    	}
System.err.println( csvTimesUntilSolutions );
System.out.println( csvAccessesUntilSolutions );
*/
    	return csv;
    }

}
