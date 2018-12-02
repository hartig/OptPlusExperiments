package se.liu.ida.jenaext.optplus;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;

public class RankMeasurements
{
	static public void main( String... argv )
    {
        final int numberOfFiles = argv.length;

        final File[] inFiles  = new File[numberOfFiles];
        final File[] outFiles = new File[numberOfFiles];
        final BufferedReader[] readers = new BufferedReader[numberOfFiles];
        final PrintWriter[] writers    = new PrintWriter[numberOfFiles];

        for ( int i=0; i < numberOfFiles; ++i )
        {
        	inFiles[i]  = new File( argv[i] );

        	final String outFileName = argv[i] + ".ranked.csv";
        	outFiles[i] = new File( outFileName );

        	if ( ! inFiles[i].exists() ) 
        		exitWitError("The file " + argv[i] + " does not exist.");
        	if ( ! inFiles[i].canRead() )
        		exitWitError("The file " + argv[i] + " cannot be read.");
//        	if ( ! outFiles[i].canWrite() )
//        		exitWitError("The file " + outFileName + " cannot be written.");

        	try {
        		final FileReader fr = new FileReader( inFiles[i] );
            	readers[i] = new BufferedReader(fr);
            	writers[i] = new PrintWriter( outFiles[i] );
        	}
        	catch ( IOException e ) {
        		exitWitError( "IOException: " + e.getMessage() );
        	}
        }

        try
        {
        	final String line1 = readers[0].readLine();;
        	final int numberOfColumns = line1.split(",").length;

            final String[] currentLines = new String[numberOfFiles];
            currentLines[0] = line1;
            for ( int i=1; i < numberOfFiles; ++i )
            {
            	currentLines[i] = readers[i].readLine();
            }

            processLine(currentLines, numberOfColumns, writers);

            for ( String line; (line = readers[0].readLine()) != null; )
            {
                currentLines[0] = line;

            	for ( int i=1; i < numberOfFiles; ++i )
            	{
            		final String l = readers[i].readLine();
            		if ( l == null )
            			exitWitError( (i+1) + "-th file has too few lines." );

            		currentLines[i] = l;
            	}

                processLine(currentLines, numberOfColumns, writers);
            }
        }
    	catch ( IOException e ) {
    		exitWitError( "IOException: " + e.getMessage() );
    	}

        for ( int i=0; i < numberOfFiles; ++i )
        {
        	try {
            	readers[i].close();
            	writers[i].close();
        	}
        	catch ( IOException e ) {
        		exitWitError( "IOException: " + e.getMessage() );
        	}
        }
    }

    static public void exitWitError( String errMsg )
    {
    	System.err.println(errMsg);
    	System.exit(-1);
    }

    static public void processLine( String[] currentLines, int numberOfColumns, PrintWriter[] writers )
    {
    	final String[] resultLines = processLine(currentLines, numberOfColumns);

    	for ( int i=0; i < currentLines.length; ++i )
    	{
			writers[i].println( resultLines[i] );
			writers[i].flush();
    	}
    }

    static public String[] processLine( String[] currentLines, int numberOfColumns )
    {
        final String[][] tuples = new String[ currentLines.length ][];
        tuples[0] = currentLines[0].split(",");
        final String queryIdOfThisLine = tuples[0][0].trim();

    	for ( int i=1; i < currentLines.length; ++i )
    	{
    		tuples[i] = currentLines[i].split(",");

    		final String queryID = tuples[i][0].trim();
    		if ( ! queryID.equals(queryIdOfThisLine) )
    			exitWitError( "Inconsistent query IDs in the current line (" + queryID + " versus " + queryIdOfThisLine + ")" );
    	}

    	final String[][] resultTuples = processTuple(tuples, numberOfColumns);

    	final String[] resultLines = new String[currentLines.length];
    	for ( int i=0; i < currentLines.length; ++i )
    	{
    		resultLines[i] = "";
    		for ( int j=0; j<resultTuples[i].length; ++j )
    		{
    			resultLines[i] += resultTuples[i][j];
    			resultLines[i] += ", ";
    		}
    	}

    	return resultLines;
    }

    static public String[][] processTuple( String[][] tuples, int numberOfColumns )
    {
    	final int highestRank = tuples.length - 1;

    	int numberOfCorrectTuples = 0;
    	final String[][] resultTuples = new String[ tuples.length ][];
    	for ( int i=0; i < tuples.length; ++i )
    	{
    		if ( tuples[i].length == numberOfColumns ) {
    			numberOfCorrectTuples++;
    			resultTuples[i] = new String[numberOfColumns];
    		}
    		else
    			resultTuples[i] = new String[1];

    		resultTuples[i][0] = tuples[i][0].trim();
    	}

    	final int[] indexesOfCorrectTuples = new int[numberOfCorrectTuples];
    	int k = 0;
    	for ( int i=0; i < tuples.length; ++i )
    	{
    		if ( tuples[i].length == numberOfColumns )
    			indexesOfCorrectTuples[k++] = i;
    	}

    	for ( int j=1; j < numberOfColumns; ++j )
    	{
    		final String value = tuples[ indexesOfCorrectTuples[0] ][j].trim();
    		if ( value.isEmpty() )
    		{
    			for ( k=0; k < numberOfCorrectTuples; ++k ) {
    				final int i = indexesOfCorrectTuples[k];
    				resultTuples[i][j] = "";
    			}
    		}
    		else //if ( value.contains(".") )
    		{
    			final List<IndexedDoubleValue> l = new ArrayList<>();
    			for ( k=0; k < numberOfCorrectTuples; ++k ) {
    				final int i = indexesOfCorrectTuples[k];
    				l.add( createIndexedDoubleValue(i, tuples[i][j]) );
    			}

    			l.sort( myIndexedDoubleValueComparator );

    			final Iterator<IndexedDoubleValue> it = l.iterator();

    			final IndexedDoubleValue first = it.next();
    			int currentRank      = highestRank;
    			int previousRank     = currentRank;
    			double previousValue = first.value;

    			resultTuples[first.index][j] = String.valueOf(currentRank);

    			while ( it.hasNext() ) {
    				currentRank--;
    				final IndexedDoubleValue x = it.next();
    				if ( x.value != previousValue ) {
    					previousValue = x.value;
    					previousRank = currentRank;
    				}
        			resultTuples[x.index][j] = String.valueOf(previousRank);
    			}
    		}
    	}

    	return resultTuples;
    }

    static public class IndexedIntValue
    {
    	final int index;
    	final int value;
    	public IndexedIntValue( int index, int value ) { this.index = index; this.value = value; }
    }

    static public IndexedIntValue createIndexedIntValue( int index, String value )
    {
    	return new IndexedIntValue( index, Integer.parseInt(value) );
    }

    static public ToIntFunction<IndexedIntValue> myToIntFunction = new ToIntFunction<IndexedIntValue>() {
    	@Override
    	public int applyAsInt( IndexedIntValue x ) { return x.value; }
    };

    static public Comparator<IndexedIntValue> myIndexedIntValueComparator = Comparator.comparingInt(myToIntFunction);

    static public class IndexedDoubleValue
    {
    	final int index;
    	final double value;
    	public IndexedDoubleValue( int index, double value ) { this.index = index; this.value = value; }
    }

    static public IndexedDoubleValue createIndexedDoubleValue( int index, String value )
    {
    	return new IndexedDoubleValue( index, Double.parseDouble(value) );
    }

    static public ToDoubleFunction<IndexedDoubleValue> myToDoubleFunction = new ToDoubleFunction<IndexedDoubleValue>() {
    	@Override
    	public double applyAsDouble( IndexedDoubleValue x ) { return x.value; }
    };

    static public Comparator<IndexedDoubleValue> myIndexedDoubleValueComparator = Comparator.comparingDouble(myToDoubleFunction);

}
