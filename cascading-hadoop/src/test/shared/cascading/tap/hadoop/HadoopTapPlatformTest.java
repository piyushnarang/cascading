/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.tap.hadoop;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.PlatformTestCase;
import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowElement;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.planner.HadoopPlanner;
import cascading.flow.planner.Scope;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexParser;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.platform.hadoop.BaseHadoopPlatform;
import cascading.property.ConfigDef;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.MultiSourceTap;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.fs.DistributedCacheFileSystem;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import data.InputData;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static data.InputData.*;

/**
 *
 */
public class HadoopTapPlatformTest extends PlatformTestCase implements Serializable
  {
  private static final Logger LOG = LoggerFactory.getLogger( HadoopTapPlatformTest.class );
  public static final String NONHFS_INPUT_PATHS = "nonhfs.input.paths";

  public HadoopTapPlatformTest()
    {
    super( true );
    }

  @Test
  public void testDfs() throws URISyntaxException, IOException
    {
    if( !getPlatform().isUseCluster() )
      return;

    // Dfs only runs on hdfs://, not just any distributed filesystem. if unavailable, skip test
    if( !( (BaseHadoopPlatform) getPlatform() ).isHDFSAvailable() )
      {
      LOG.warn( "skipped Dfs tests, HDFS is unavailable on current platform" );
      return;
      }

    Tap tap = new Dfs( new Fields( "foo" ), "some/path" );

    String path = tap.getFullIdentifier( HadoopPlanner.createJobConf( getProperties() ) );
    assertFalse( "wrong scheme", new Path( path ).toUri().getScheme().equalsIgnoreCase( "file" ) );

    new Dfs( new Fields( "foo" ), "hdfs://localhost:5001/some/path" );
    new Dfs( new Fields( "foo" ), new URI( "hdfs://localhost:5001/some/path" ) );

    try
      {
      new Dfs( new Fields( "foo" ), "s3://localhost:5001/some/path" );
      fail( "not valid url" );
      }
    catch( Exception exception )
      {
      }

    try
      {
      new Dfs( new Fields( "foo" ), new URI( "s3://localhost:5001/some/path" ) );
      fail( "not valid url" );
      }
    catch( Exception exception )
      {
      }
    }

  @Test
  public void testLfs() throws URISyntaxException, IOException
    {
    Tap tap = new Lfs( new Fields( "foo" ), "some/path" );

    String path = tap.getFullIdentifier( HadoopPlanner.createJobConf( getProperties() ) );
    assertTrue( "wrong scheme", new Path( path ).toUri().getScheme().equalsIgnoreCase( "file" ) );

    new Lfs( new Fields( "foo" ), "file:///some/path" );

    try
      {
      new Lfs( new Fields( "foo" ), "s3://localhost:5001/some/path" );
      fail( "not valid url" );
      }
    catch( Exception exception )
      {
      }
    }

  public class CommentScheme extends TextLine
    {
    public CommentScheme( Fields sourceFields )
      {
      super( sourceFields );
      }

    @Override
    public boolean source( FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall ) throws IOException
      {
      boolean success = sourceCall.getInput().next( sourceCall.getContext()[ 0 ], sourceCall.getContext()[ 1 ] );

      if( !success )
        return false;

      if( sourceCall.getContext()[ 1 ].toString().matches( "^\\s*#.*$" ) )
        return source( flowProcess, sourceCall );

      sourceHandleInput( sourceCall );

      return true;
      }
    }

  @Test
  public void testNullsFromScheme() throws IOException
    {
    getPlatform().copyFromLocal( inputFileComments );

    Tap source = new Hfs( new CommentScheme( new Fields( "line" ) ), inputFileComments );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Identity() );

    Tap sink = new Hfs( new TextLine( 1 ), getOutputPath( "testnulls" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector( getProperties() ).connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, null );

    TupleEntryIterator iterator = flow.openSink();

    assertEquals( "not equal: tuple.get(1)", "1 a", iterator.next().getObject( 1 ) );

    iterator.close();

    // confirm the tuple iterator can handle nulls from the source
    validateLength( flow.openSource(), 5 );
    }

  public class ResolvedScheme extends TextLine
    {
    private final Fields expectedFields;

    public ResolvedScheme( Fields expectedFields )
      {
      this.expectedFields = expectedFields;
      }

    @Override
    public void sinkPrepare( FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall ) throws IOException
      {
      Fields found = sinkCall.getOutgoingEntry().getFields();

      if( !found.equals( expectedFields ) )
        throw new RuntimeException( "fields to not match, expect: " + expectedFields + ", found: " + found );

      super.sinkPrepare( flowProcess, sinkCall );
      }

    @Override
    public void sink( FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall ) throws IOException
      {
      Fields found = sinkCall.getOutgoingEntry().getFields();

      if( !found.equals( expectedFields ) )
        throw new RuntimeException( "fields to not match, expect: " + expectedFields + ", found: " + found );

      super.sink( flowProcess, sinkCall );
      }
    }

  @Test
  public void testResolvedSinkFields() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLower );

    Tap source = new Hfs( new TextLine( new Fields( "line" ) ), inputFileLower );

    Pipe pipe = new Pipe( "test" );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );
    pipe = new Each( pipe, new Fields( "line" ), splitter );

    Tap sink = new Hfs( new ResolvedScheme( new Fields( "num", "char" ) ), getOutputPath( "resolvedfields" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector( getProperties() ).connect( source, sink, pipe );

    flow.complete();

    List<Tuple> tuples = asList( flow, sink );
    List<Object> values = new ArrayList<Object>();
    for( Tuple tuple : tuples )
      values.add( tuple.getObject( 1 ) );

    assertTrue( values.contains( "1\ta" ) );
    assertTrue( values.contains( "2\tb" ) );
    assertTrue( values.contains( "3\tc" ) );
    assertTrue( values.contains( "4\td" ) );
    assertTrue( values.contains( "5\te" ) );

    assertEquals( 5, tuples.size() );

    // confirm the tuple iterator can handle nulls from the source
    assertEquals( 5, asList( flow, source ).size() );
    }

  @Test
  public void testGlobHfs() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    GlobHfs source = new GlobHfs( new TextLine( new Fields( "offset", "line" ) ), InputData.inputPath + "?{ppe[_r],owe?}.txt" );

    assertEquals( 2, source.getTaps().length );

    // show globhfs will just match a directory if ended with a /
    assertEquals( 1, new GlobHfs( new TextLine( new Fields( "offset", "line" ) ), InputData.inputPath + "../?ata/" ).getTaps().length );

    Tap sink = new Hfs( new TextLine(), getOutputPath( "glob" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), "\\s" );
    Pipe concatPipe = new Each( new Pipe( "concat" ), new Fields( "line" ), splitter );

    Flow concatFlow = getPlatform().getFlowConnector( getProperties() ).connect( "first", source, sink, concatPipe );

    Tap nextSink = new Hfs( new TextLine(), getOutputPath( "glob2" ), SinkMode.REPLACE );

    Flow nextFlow = getPlatform().getFlowConnector( getProperties() ).connect( "second", sink, nextSink, concatPipe );

    Cascade cascade = new CascadeConnector().connect( concatFlow, nextFlow );

    cascade.complete();

    validateLength( concatFlow, 10 );
    }

  @Test
  public void testNestedMultiSourceGlobHfs() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    GlobHfs source1 = new GlobHfs( new TextLine( new Fields( "offset", "line" ) ), InputData.inputPath + "?{ppe[_r]}.txt" );
    GlobHfs source2 = new GlobHfs( new TextLine( new Fields( "offset", "line" ) ), InputData.inputPath + "?{owe?}.txt" );

    MultiSourceTap source = new MultiSourceTap( source1, source2 );

    assertEquals( 2, source.getNumChildTaps() );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), getOutputPath( "globmultisource" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), "\\s" );
    Pipe concatPipe = new Each( new Pipe( "concat" ), new Fields( "line" ), splitter );

    Flow concatFlow = getPlatform().getFlowConnector( getProperties() ).connect( "first", source, sink, concatPipe );

    Tap nextSink = new Hfs( new TextLine(), getOutputPath( "globmultiource2" ), SinkMode.REPLACE );

    Flow nextFlow = getPlatform().getFlowConnector( getProperties() ).connect( "second", sink, nextSink, concatPipe );

    Cascade cascade = new CascadeConnector().connect( concatFlow, nextFlow );

    cascade.complete();

    validateLength( concatFlow, 10 );
    }

  @Test
  public void testMultiSourceIterator() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    GlobHfs source1 = new GlobHfs( new TextLine( new Fields( "offset", "line" ) ), InputData.inputPath + "?{ppe[_r]}.txt" );
    GlobHfs source2 = new GlobHfs( new TextLine( new Fields( "offset", "line" ) ), InputData.inputPath + "?{owe?}.txt" );

    MultiSourceTap source = new MultiSourceTap( source1, source2 );

    validateLength( source.openForRead( getPlatform().getFlowProcess() ), 10 );

    GlobHfs sourceMulti = new GlobHfs( new TextLine( new Fields( "offset", "line" ) ), InputData.inputPath + "?{ppe[_r],owe?}.txt" );

    source = new MultiSourceTap( sourceMulti );

    validateLength( source.openForRead( getPlatform().getFlowProcess() ), 10, null );
    }

  @Test
  public void testCommitResource() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    final int[] count = {0};
    Tap sink = new Hfs( new TextDelimited( Fields.ALL ), getOutputPath( "committap" ), SinkMode.REPLACE )
    {
    @Override
    public boolean commitResource( JobConf conf ) throws IOException
      {
      count[ 0 ] = count[ 0 ] + 1;
      return true;
      }
    };

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    assertEquals( 1, count[ 0 ] );
    validateLength( flow, 8, null );
    }

  @Test
  public void testCommitResourceFails() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = new Hfs( new TextDelimited( Fields.ALL ), getOutputPath( "committapfail" ), SinkMode.REPLACE )
    {
    @Override
    public boolean commitResource( JobConf conf ) throws IOException
      {
      throw new IOException( "failed intentionally" );
      }
    };

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    try
      {
      flow.complete();
      fail();
      }
    catch( Exception exception )
      {
      exception.printStackTrace();
      // success
      }
    }

  @Test
  public void testHfsAsterisk() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Hfs sourceExists = new Hfs( new TextLine( new Fields( "offset", "line" ) ), InputData.inputPath + "*" );

    assertTrue( sourceExists.resourceExists( ( (BaseHadoopPlatform) getPlatform() ).getJobConf() ) );

    TupleEntryIterator iterator = sourceExists.openForRead( new HadoopFlowProcess( ( (BaseHadoopPlatform) getPlatform() ).getJobConf() ) );
    assertTrue( iterator.hasNext() );
    iterator.close();

    try
      {
      Hfs sourceNotExists = new Hfs( new TextLine( new Fields( "offset", "line" ) ), InputData.inputPath + "/blah/" );
      iterator = sourceNotExists.openForRead( new HadoopFlowProcess( ( (BaseHadoopPlatform) getPlatform() ).getJobConf() ) );
      fail();
      }
    catch( IOException exception )
      {
      // do nothing
      }
    }

  @Test
  public void testHfsBracketAsterisk() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Hfs sourceExists = new Hfs( new TextLine( new Fields( "offset", "line" ) ), InputData.inputPath + "{*}" );

    assertTrue( sourceExists.resourceExists( ( (BaseHadoopPlatform) getPlatform() ).getJobConf() ) );

    TupleEntryIterator iterator = sourceExists.openForRead( new HadoopFlowProcess( ( (BaseHadoopPlatform) getPlatform() ).getJobConf() ) );
    assertTrue( iterator.hasNext() );
    iterator.close();

    try
      {
      Hfs sourceNotExists = new Hfs( new TextLine( new Fields( "offset", "line" ) ), InputData.inputPath + "/blah/" );
      iterator = sourceNotExists.openForRead( new HadoopFlowProcess( ( (BaseHadoopPlatform) getPlatform() ).getJobConf() ) );
      fail();
      }
    catch( IOException exception )
      {
      // do nothing
      }
    }

  public class DupeConfigScheme extends TextLine
    {
    public DupeConfigScheme( Fields sourceFields )
      {
      super( sourceFields );
      }

    @Override
    public void sourceConfInit( FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf )
      {
      if( conf.get( "this.is.a.dupe" ) != null )
        throw new IllegalStateException( "has dupe config value" );

      conf.set( "this.is.a.dupe", "dupe" );

      super.sourceConfInit( flowProcess, tap, conf );
      }

    @Override
    public void sourcePrepare( FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall )
      {
      if( flowProcess.getStringProperty( "this.is.a.dupe" ) == null )
        throw new IllegalStateException( "has no dupe config value" );

      super.sourcePrepare( flowProcess, sourceCall );
      }
    }

  @Test
  public void testDupeConfigFromScheme() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTap( new DupeConfigScheme( new Fields( "offset", "line" ) ), inputFileUpper, SinkMode.KEEP );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );
    assertTrue("Should be distCacheable!", DistributedCacheFileSystem.distCacheable( sourceUpper) );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "dupeconfig" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new HashJoin( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

    Map<Object, Object> properties = getProperties();

    Flow flow = getPlatform().getFlowConnector( properties ).connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 5 );

    List<Tuple> values = getSinkAsList( flow );

    assertTrue( values.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( values.contains( new Tuple( "2\tb\t2\tB" ) ) );
    }

  @Test
  public void testMultiHfsHashJoin() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );
    getPlatform().copyFromLocal( inputFileUpper2 );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );
    Tap sourceUpper2 = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper2 );
    Tap multiTap = new MultiSourceTap(sourceUpper, sourceUpper2);

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", multiTap );
    assertTrue( "Should be distCacheable!", DistributedCacheFileSystem.distCacheable( multiTap ));

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "hashjoin1" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new HashJoin( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

    Map<Object, Object> properties = getProperties();

    Flow flow = getPlatform().getFlowConnector( properties ).connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 6 );

    List<Tuple> values = getSinkAsList( flow );

    assertTrue( values.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( values.contains( new Tuple( "2\tb\t2\tB" ) ) );
    assertTrue( values.contains( new Tuple( "3\tc\t3\tC" ) ) );
    assertTrue( values.contains( new Tuple( "4\td\t4\tD" ) ) );
    assertTrue( values.contains( new Tuple( "5\te\t5\tE" ) ) );
    assertTrue( values.contains( new Tuple( "5\te\t5\tF" ) ) );
    }

  @Test
  public void testGlobHfsHashJoin() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );
    getPlatform().copyFromLocal( inputFileUpper2 );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = new GlobHfs(new TextLine(new Fields( "offset", "line" ), Fields.ALL),
        inputPath + "{upper.txt,upper2.txt}");

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );
    assertTrue( "Should be distCacheable!", DistributedCacheFileSystem.distCacheable( sourceUpper ));

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "hashjoin2" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new HashJoin( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

    Map<Object, Object> properties = getProperties();

    Flow flow = getPlatform().getFlowConnector( properties ).connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 6 );

    List<Tuple> values = getSinkAsList( flow );

    assertTrue( values.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( values.contains( new Tuple( "2\tb\t2\tB" ) ) );
    assertTrue( values.contains( new Tuple( "3\tc\t3\tC" ) ) );
    assertTrue( values.contains( new Tuple( "4\td\t4\tD" ) ) );
    assertTrue( values.contains( new Tuple( "5\te\t5\tE" ) ) );
    assertTrue( values.contains( new Tuple( "5\te\t5\tF" ) ) );
    }

  @Test
  public void testMultiGlobHfsHashJoin() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );
    getPlatform().copyFromLocal( inputFileUpper2 );
    getPlatform().copyFromLocal( inputFileUpper3 );


    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper12 = new GlobHfs(new TextLine(new Fields( "offset", "line" ), Fields.ALL),
        inputPath + "{upper.txt,upper2.txt}");
    Tap sourceUpper3 = new GlobHfs(new TextLine(new Fields( "offset", "line" ), Fields.ALL),
        inputPath + "up?er3.txt");
    Tap multiTap = new MultiSourceTap( sourceUpper12, sourceUpper3 );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", multiTap );
    assertTrue( "Should be distCacheable!", DistributedCacheFileSystem.distCacheable( multiTap ));

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "hashjoin3" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new HashJoin( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

    Map<Object, Object> properties = getProperties();

    Flow flow = getPlatform().getFlowConnector( properties ).connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 7 );

    List<Tuple> values = getSinkAsList( flow );

    assertTrue( values.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( values.contains( new Tuple( "2\tb\t2\tB" ) ) );
    assertTrue( values.contains( new Tuple( "3\tc\t3\tC" ) ) );
    assertTrue( values.contains( new Tuple( "4\td\t4\tD" ) ) );
    assertTrue( values.contains( new Tuple( "5\te\t5\tE" ) ) );
    assertTrue( values.contains( new Tuple( "5\te\t5\tF" ) ) );
    assertTrue( values.contains( new Tuple( "5\te\t5\tG" ) ) );
    }

  // Not extending Hfs to make it uncacheable
  public class NonHfs extends Tap<JobConf,RecordReader,OutputCollector>
    {
    private final Tap<JobConf,RecordReader,OutputCollector> realTap;

    public NonHfs( Tap<JobConf,RecordReader,OutputCollector> realTap )
      {
      this.realTap = realTap;
      }

    @Override
    public Scheme<JobConf,RecordReader,OutputCollector,?,?> getScheme()
      {
      return realTap.getScheme();
      }

    @Override
    public String getTrace()
      {
      return realTap.getTrace();
      }

    @Override
    public void flowConfInit( Flow<JobConf> flow )
      {
      realTap.flowConfInit(flow);
      }

    @Override
    public void sourceConfInit( FlowProcess<JobConf> flowProcess, JobConf conf )
      {
      String[] oldVal = StringUtils.getStrings( NONHFS_INPUT_PATHS );
      if (oldVal != null)
        {
        FileInputFormat.setInputPaths( conf, StringUtils.arrayToString( oldVal ) );
        }
      realTap.sourceConfInit( flowProcess, conf );
      conf.setStrings( NONHFS_INPUT_PATHS,
          StringUtils.getStrings( "mapreduce.input.fileinputformat.inputdir" ) );
      // make it break the DistCacheFS
      FileInputFormat.setInputPaths( conf, "nonhfs:///nonsense/path" );
      }

    @Override
    public void sinkConfInit( FlowProcess<JobConf> flowProcess, JobConf conf )
      {
      realTap.sinkConfInit( flowProcess, conf );
      }

    @Override
    public String getIdentifier()
      {
      return realTap.getIdentifier();
      }

    @Override
    public Fields getSourceFields()
      {
      return realTap.getSourceFields();
      }

    @Override
    public Fields getSinkFields()
      {
      return realTap.getSinkFields();
      }

    @Override
    public TupleEntryIterator openForRead( FlowProcess<JobConf> flowProcess, RecordReader input ) throws IOException
      {
      return realTap.openForRead( flowProcess, input );
      }

    @Override
    public TupleEntryCollector openForWrite( FlowProcess<JobConf> flowProcess, OutputCollector output )
        throws IOException
      {
      return realTap.openForWrite( flowProcess, output );
      }

    @Override
    public Scope outgoingScopeFor( Set<Scope> incomingScopes )
      {
      return realTap.outgoingScopeFor( incomingScopes );
      }

    @Override
    public Fields retrieveSourceFields( FlowProcess<JobConf> flowProcess )
      {
      return realTap.retrieveSourceFields( flowProcess );
      }

    @Override
    public void presentSourceFields( FlowProcess<JobConf> flowProcess, Fields fields )
      {
      realTap.presentSourceFields( flowProcess, fields );
      }

    @Override
    public Fields retrieveSinkFields( FlowProcess<JobConf> flowProcess )
      {
      return realTap.retrieveSinkFields( flowProcess );
      }

    @Override
    public void presentSinkFields( FlowProcess<JobConf> flowProcess, Fields fields )
      {
      realTap.presentSinkFields( flowProcess, fields );
      }

    @Override
    public Fields resolveIncomingOperationArgumentFields( Scope incomingScope )
      {
      return realTap.resolveIncomingOperationArgumentFields( incomingScope );
      }

    @Override
    public Fields resolveIncomingOperationPassThroughFields( Scope incomingScope )
      {
      return realTap.resolveIncomingOperationPassThroughFields( incomingScope );
      }

    @Override
    public String getFullIdentifier( FlowProcess<JobConf> flowProcess )
      {
      return realTap.getFullIdentifier( flowProcess );
      }

    @Override
    public String getFullIdentifier( JobConf conf )
      {
      return realTap.getFullIdentifier( conf );
      }

    @Override
    public boolean createResource( FlowProcess<JobConf> flowProcess ) throws IOException
      {
      return realTap.createResource( flowProcess );
      }

    @Override
    public boolean createResource( JobConf conf ) throws IOException
      {
      return realTap.createResource(conf);
      }

    @Override
    public boolean deleteResource( FlowProcess<JobConf> flowProcess ) throws IOException
      {
      return realTap.deleteResource( flowProcess );
      }

    @Override
    public boolean deleteResource( JobConf conf ) throws IOException
      {
      return realTap.deleteResource( conf );
      }

    @Override
    public boolean commitResource( JobConf conf ) throws IOException
      {
      return realTap.commitResource( conf );
      }

    @Override
    public boolean rollbackResource( JobConf conf ) throws IOException
      {
      return realTap.rollbackResource( conf );
      }

    @Override
    public boolean resourceExists( JobConf conf ) throws IOException
      {
      return realTap.resourceExists( conf );
      }

    @Override
    public long getModifiedTime( JobConf conf ) throws IOException
      {
      return realTap.getModifiedTime( conf );
      }

    @Override
    public cascading.tap.SinkMode getSinkMode()
      {
      return realTap.getSinkMode();
      }

    @Override
    public boolean isKeep()
      {
      return realTap.isKeep();
      }

    @Override
    public boolean isReplace()
      {
      return realTap.isReplace();
      }

    @Override
    public boolean isUpdate()
      {
      return realTap.isUpdate();
      }

    @Override
    public boolean isSink()
      {
      return realTap.isSink();
      }

    @Override
    public boolean isSource()
      {
      return realTap.isSource();
      }

    @Override
    public boolean isTemporary()
      {
      return realTap.isTemporary();
      }

    @Override
    public ConfigDef getConfigDef()
      {
      return realTap.getConfigDef();
      }

    @Override
    public boolean hasConfigDef()
      {
      return realTap.hasConfigDef();
      }

    @Override
    public ConfigDef getStepConfigDef()
      {
      return realTap.getStepConfigDef();
      }

    @Override
    public boolean hasStepConfigDef()
      {
      return realTap.hasStepConfigDef();
      }

    @Override
    public boolean isEquivalentTo(FlowElement element)
      {
      return realTap.isEquivalentTo(element);
      }
    }

  // FakeHfs HashJoin should not use CDCFS optimization
  @Test
  public void testMultiFakeHfsHashJoin() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields("offset", "line"), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );
    Tap nonhfsUpper = new NonHfs( sourceUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", nonhfsUpper );
    assertTrue( !DistributedCacheFileSystem.distCacheable( nonhfsUpper ));

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "hashjoin4" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new HashJoin( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

    Map<Object, Object> properties = getProperties();

    Flow flow = getPlatform().getFlowConnector( properties ).connect(sources, sink, splice);

    flow.complete();

    validateLength( flow, 5 );

    List<Tuple> values = getSinkAsList( flow );

    assertTrue( values.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( values.contains( new Tuple( "2\tb\t2\tB" ) ) );
    assertTrue( values.contains( new Tuple( "3\tc\t3\tC" ) ) );
    assertTrue( values.contains( new Tuple( "4\td\t4\tD" ) ) );
    assertTrue( values.contains( new Tuple( "5\te\t5\tE" ) ) );
    }

  public void testCombinedHfs() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Hfs sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), InputData.inputFileLower );
    Hfs sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), InputData.inputFileUpper );

    // create a CombinedHfs instance on these files
    Tap source = new MultiSourceTap<Hfs, JobConf, RecordReader>( sourceLower, sourceUpper );

    FlowProcess<JobConf> process = getPlatform().getFlowProcess();
    JobConf conf = process.getConfigCopy();

    // set the combine flag
    conf.setBoolean( HfsProps.COMBINE_INPUT_FILES, true );

    // test the input format and the split
    source.sourceConfInit( process, conf );

    InputFormat inputFormat = conf.getInputFormat();

    assertEquals( Hfs.CombinedInputFormat.class, inputFormat.getClass() );
    InputSplit[] splits = inputFormat.getSplits( conf, 1 );

    assertEquals( 1, splits.length );

    validateLength( source.openForRead( process ), 10 );
    }

  @Test
  public void testMissingInputFormat() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextDelimited( new Fields( "offset", "line" ) ), inputFileApache )
    {
    @Override
    public void sourceConfInit( FlowProcess<JobConf> process, JobConf conf )
      {
      // don't set input format
      //super.sourceConfInit( process, conf );
      }
    };

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = new Hfs( new TextDelimited( Fields.ALL ), getOutputPath( "missinginputformat" ), SinkMode.REPLACE );

    try
      {
      Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );
      flow.complete();
      fail( "did not test for missing input format" );
      }
    catch( Exception exception )
      {
      // ignore
      }
    }

  @Test
  public void testChildIdentifiers() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    JobConf jobConf = ( (BaseHadoopPlatform) getPlatform() ).getJobConf();

    Hfs tap = new Hfs( new TextLine( new Fields( "offset", "line" ) ), getOutputPath( "multifiles" ) );

    tap.deleteResource( getPlatform().getFlowProcess() );

    assertEqualsSize( "missing", 0, tap.getChildIdentifiers( jobConf ) );
    assertEqualsSize( "missing", 0, tap.getChildIdentifiers( jobConf, 2, true ) );
    assertEqualsSize( "missing", 0, tap.getChildIdentifiers( jobConf, 2, false ) );
    assertEqualsSize( "missing", 0, tap.getChildIdentifiers( jobConf, 1, true ) );
    assertEqualsSize( "missing", 0, tap.getChildIdentifiers( jobConf, 1, false ) );
    assertEqualsSize( "missing", 0, tap.getChildIdentifiers( jobConf, 0, true ) );
    assertEqualsSize( "missing", 0, tap.getChildIdentifiers( jobConf, 0, false ) );

    tap.createResource( getPlatform().getFlowProcess() );

    assertEqualsSize( "no children", 0, tap.getChildIdentifiers( jobConf ) );
    assertEqualsSize( "no children", 0, tap.getChildIdentifiers( jobConf, 2, true ) );
    assertEqualsSize( "no children", 0, tap.getChildIdentifiers( jobConf, 2, false ) );
    assertEqualsSize( "no children", 0, tap.getChildIdentifiers( jobConf, 1, true ) );
    assertEqualsSize( "no children", 0, tap.getChildIdentifiers( jobConf, 1, false ) );
    assertEqualsSize( "no children", 1, tap.getChildIdentifiers( jobConf, 0, true ) );
    assertEqualsSize( "no children", 1, tap.getChildIdentifiers( jobConf, 0, false ) );

    writeFileTo( "multifiles/A" );
    writeFileTo( "multifiles/B" );

    assertEqualsSize( "children", 2, tap.getChildIdentifiers( jobConf ) );
    assertEqualsSize( "children", 2, tap.getChildIdentifiers( jobConf, 2, true ) );
    assertEqualsSize( "children", 2, tap.getChildIdentifiers( jobConf, 2, false ) );
    assertEqualsSize( "children", 2, tap.getChildIdentifiers( jobConf, 1, true ) );
    assertEqualsSize( "children", 2, tap.getChildIdentifiers( jobConf, 1, false ) );
    assertEqualsSize( "children", 1, tap.getChildIdentifiers( jobConf, 0, true ) );
    assertEqualsSize( "children", 1, tap.getChildIdentifiers( jobConf, 0, false ) );

    tap = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "/" );

    assertEqualsSize( "root", -1, tap.getChildIdentifiers( jobConf ) );
    assertEqualsSize( "root", -1, tap.getChildIdentifiers( jobConf, 2, true ) );
    assertEqualsSize( "root", -1, tap.getChildIdentifiers( jobConf, 2, false ) );
    assertEqualsSize( "root", -1, tap.getChildIdentifiers( jobConf, 1, true ) );
    assertEqualsSize( "root", -1, tap.getChildIdentifiers( jobConf, 1, false ) );
    assertEqualsSize( "root", 1, tap.getChildIdentifiers( jobConf, 0, true ) );
    assertEqualsSize( "root", 1, tap.getChildIdentifiers( jobConf, 0, false ) );

    tap = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "./" );

    assertEqualsSize( "current", -1, tap.getChildIdentifiers( jobConf ) );
    assertEqualsSize( "current", -1, tap.getChildIdentifiers( jobConf, 2, true ) );
    assertEqualsSize( "current", -1, tap.getChildIdentifiers( jobConf, 2, false ) );
    assertEqualsSize( "current", -1, tap.getChildIdentifiers( jobConf, 1, true ) );
    assertEqualsSize( "current", -1, tap.getChildIdentifiers( jobConf, 1, false ) );
    assertEqualsSize( "current", -1, tap.getChildIdentifiers( jobConf, 0, true ) );
    assertEqualsSize( "current", -1, tap.getChildIdentifiers( jobConf, 0, false ) );

    tap = new Hfs( new TextLine( new Fields( "offset", "line" ) ), getOutputPath( "hiddenfiles" ) );

    writeFileTo( "hiddenfiles/A" );
    writeFileTo( "hiddenfiles/B" );
    writeFileTo( "hiddenfiles/.hidden" );

    assertEqualsSize( "children", 2, tap.getChildIdentifiers( jobConf ) );
    assertEqualsSize( "children", 2, tap.getChildIdentifiers( jobConf, 2, true ) );
    assertEqualsSize( "children", 2, tap.getChildIdentifiers( jobConf, 2, false ) );
    assertEqualsSize( "children", 2, tap.getChildIdentifiers( jobConf, 1, true ) );
    assertEqualsSize( "children", 2, tap.getChildIdentifiers( jobConf, 1, false ) );
    assertEqualsSize( "children", 1, tap.getChildIdentifiers( jobConf, 0, true ) );
    assertEqualsSize( "children", 1, tap.getChildIdentifiers( jobConf, 0, false ) );
    }

  public void assertEqualsSize( String message, int expected, String[] actual )
    {
//    System.out.println( message + ": " + Arrays.toString( actual ) );

    if( expected == -1 )
      return;

    assertEquals( expected, actual.length );
    }

  private void writeFileTo( String path ) throws IOException
    {
    Hfs tap = new Hfs( new TextLine( new Fields( "offset", "line" ) ), getOutputPath( path ) );

    TupleEntryCollector collector = tap.openForWrite( getPlatform().getFlowProcess() );

    collector.add( new Tuple( 1, "1" ) );

    collector.close();
    }
  }
