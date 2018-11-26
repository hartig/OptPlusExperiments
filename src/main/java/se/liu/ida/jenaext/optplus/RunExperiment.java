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
import org.apache.jena.query.QuerySolution;
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

    protected Query query; 
    protected Dataset dataset;

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
        super.add( argQueryFile, "--queryfile", "file with the SPARQL query to be used for the experiment" );
        super.add( argHDTFile, "--hdtfile", "HDT file with the dataset to be used for the experiment" );

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
//        	hdt = HDTManager.mapIndexedHDT(hdtFileName, null);
        	hdt = HDTManager.loadIndexedHDT(hdtFileName, null);
        }
        catch ( IOException e ) {
        	cmdError( "Accessing the HDT file failed: " + e.getMessage() );
        	return;
        }

        final HDTGraph graph = new HDTGraph(hdt);
        final DatasetGraph dsg = DatasetGraphFactory.create(graph);
        dataset = DatasetFactory.wrap(dsg);
    }

    @Override
    protected void exec()
    {
        QueryEnginePlus.register();
//    	ARQ.getContext().set(QueryEnginePlus.classnameOptPlusIterator, "QueryIterHashJoinPlusMaterializeLeftOnTheFly" );
    	ARQ.getContext().set(QueryEnginePlus.classnameOptPlusIterator, "QueryIterNestedLoopJoinPlus" );

    	ARQ.getContext().setTrue(QueryEnginePlus.useOptPlusSemantics);
        execQuery();

    	ARQ.getContext().setFalse(QueryEnginePlus.useOptPlusSemantics);
        execQuery();

    	ARQ.getContext().setTrue(QueryEnginePlus.useOptPlusSemantics);
        execQuery();
    }

    protected void execQuery()
    {
    	final long t1 = System.nanoTime();
        final QueryExecution qe = QueryExecutionFactory.create(query, dataset);
    	final long t2 = System.nanoTime();
        final ResultSet rs = qe.execSelect();
        int cnt = 0;
        long t1st = 0L;
        while ( rs.hasNext() ) {
        	cnt++;
        	if ( cnt == 1 )
        		t1st = System.nanoTime();

        	final QuerySolution s = rs.next();
//        	System.err.println( s.toString() );
        }
    	final long t3 = System.nanoTime();
    	System.out.printf( "Result size: %d \n", cnt );
    	System.out.printf( "Overall time: %d \n", (t3-t1)/1000000 );
    	System.out.printf( "\t Creation time: %d \n", (t2-t1)/1000000 );
    	System.out.printf( "\t\t Exec time: %d \n", (t3-t2)/1000000 );
    	System.out.printf( "\t\t Overall time to first: %d \n", (t1st-t1)/1000000 );
    }

}
