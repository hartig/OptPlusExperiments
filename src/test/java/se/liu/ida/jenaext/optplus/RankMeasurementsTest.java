package se.liu.ida.jenaext.optplus;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class RankMeasurementsTest
{
	@Test
	public void twoLines()
	{
		final String[] lines = {"1, 1, 3.0", "1, 2, 1.1"};
		final String[] result = RankMeasurements.processLine(lines, 3);

		assertEquals( 2, result.length );
		assertEquals( "1, 1, 0, ", result[0] );
		assertEquals( "1, 0, 1, ", result[1] );
	}

	@Test
	public void threeLines()
	{
		final String[] lines = {"1, 1, 3.0", "1, 2, 1.1", "1, 3, 2.1"};
		final String[] result = RankMeasurements.processLine(lines, 3);

		assertEquals( 3, result.length );
		assertEquals( "1, 2, 0, ", result[0] );
		assertEquals( "1, 1, 2, ", result[1] );
		assertEquals( "1, 0, 1, ", result[2] );
	}

	@Test
	public void threeLinesTie1()
	{
		final String[] lines = {"1, 1", "1, 2", "1, 1"};
		final String[] result = RankMeasurements.processLine(lines, 2);

		assertEquals( 3, result.length );
		assertEquals( "1, 2, ", result[0] );
		assertEquals( "1, 0, ", result[1] );
		assertEquals( "1, 2, ", result[2] );
	}

	@Test
	public void threeLinesTie2()
	{
		final String[] lines = {"1, 2", "1, 2", "1, 1"};
		final String[] result = RankMeasurements.processLine(lines, 2);

		assertEquals( 3, result.length );
		assertEquals( "1, 1, ", result[0] );
		assertEquals( "1, 1, ", result[1] );
		assertEquals( "1, 2, ", result[2] );
	}

	@Test
	public void threeLinesTie3()
	{
		final String[] lines = {"1, 1", "1, 1", "1, 1"};
		final String[] result = RankMeasurements.processLine(lines, 2);

		assertEquals( 3, result.length );
		assertEquals( "1, 2, ", result[0] );
		assertEquals( "1, 2, ", result[1] );
		assertEquals( "1, 2, ", result[2] );
	}

	@Test
	public void threeLinesIncorrectTuple()
	{
		final String[] lines = {"1, 1, 3.0", "1, ERROR", "1, 3, 2.1"};
		final String[] result = RankMeasurements.processLine(lines, 3);

		assertEquals( 3, result.length );
		assertEquals( "1, 2, 1, ", result[0] );
		assertEquals( "1, ",       result[1] );
		assertEquals( "1, 1, 2, ", result[2] );
	}
}
