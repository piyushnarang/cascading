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

package cascading.flow.hadoop;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import cascading.flow.FlowException;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.planner.HadoopFlowStepJob;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.planner.BaseFlowStep;
import cascading.flow.planner.FlowStepJob;
import cascading.flow.planner.Scope;
import cascading.property.ConfigDef;
import cascading.tap.Tap;
import cascading.tap.hadoop.io.MultiInputFormat;
import cascading.tap.hadoop.util.Hadoop18TapUtil;
import cascading.tap.hadoop.util.TempHfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.hadoop.TupleSerialization;
import cascading.tuple.hadoop.util.CoGroupingComparator;
import cascading.tuple.hadoop.util.CoGroupingPartitioner;
import cascading.tuple.hadoop.util.GroupingComparator;
import cascading.tuple.hadoop.util.GroupingPartitioner;
import cascading.tuple.hadoop.util.GroupingSortingComparator;
import cascading.tuple.hadoop.util.GroupingSortingPartitioner;
import cascading.tuple.hadoop.util.IndexTupleCoGroupingComparator;
import cascading.tuple.hadoop.util.ReverseGroupingSortingComparator;
import cascading.tuple.hadoop.util.ReverseTupleComparator;
import cascading.tuple.hadoop.util.TupleComparator;
import cascading.tuple.io.IndexTuple;
import cascading.tuple.io.TuplePair;
import cascading.util.Util;
import cascading.util.Version;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;

import static cascading.flow.hadoop.util.HadoopUtil.serializeBase64;
import static cascading.flow.hadoop.util.HadoopUtil.writeStateToDistCache;

/**
 *
 */
public class HadoopFlowStep extends BaseFlowStep<JobConf>
  {
  /** Field mapperTraps */
  private final Map<String, Tap> mapperTraps = new HashMap<String, Tap>();
  /** Field reducerTraps */
  private final Map<String, Tap> reducerTraps = new HashMap<String, Tap>();

  public HadoopFlowStep( String name, int stepNum )
    {
    super( name, stepNum );
    }

  public JobConf getInitializedConfig( FlowProcess<JobConf> flowProcess, JobConf parentConfig )
    {
    JobConf conf = parentConfig == null ? new JobConf() : HadoopUtil.copyJobConf( parentConfig );

    // disable warning
    conf.setBoolean( "mapred.used.genericoptionsparser", true );

    conf.setJobName( getStepDisplayName( conf.getInt( "cascading.step.display.id.truncate", Util.ID_LENGTH ) ) );

    conf.setOutputKeyClass( Tuple.class );
    conf.setOutputValueClass( Tuple.class );

    conf.setMapRunnerClass( FlowMapper.class );
    conf.setReducerClass( FlowReducer.class );

    // set for use by the shuffling phase
    TupleSerialization.setSerializations( conf );

    initFromSources( flowProcess, conf );

    initFromSink( flowProcess, conf );

    initFromTraps( flowProcess, conf );

    initFromProcessConfigDef( conf );

    if( getSink().getScheme().getNumSinkParts() != 0 )
      {
      // if no reducer, set num map tasks to control parts
      if( getGroup() != null )
        conf.setNumReduceTasks( getSink().getScheme().getNumSinkParts() );
      else
        conf.setNumMapTasks( getSink().getScheme().getNumSinkParts() );
      }

    conf.setOutputKeyComparatorClass( TupleComparator.class );

    if( getGroup() == null )
      {
      conf.setNumReduceTasks( 0 ); // disable reducers
      }
    else
      {
      // must set map output defaults when performing a reduce
      conf.setMapOutputKeyClass( Tuple.class );
      conf.setMapOutputValueClass( Tuple.class );
      conf.setPartitionerClass( GroupingPartitioner.class );

      // handles the case the groupby sort should be reversed
      if( getGroup().isSortReversed() )
        conf.setOutputKeyComparatorClass( ReverseTupleComparator.class );

      addComparators( conf, "cascading.group.comparator", getGroup().getKeySelectors() );

      if( getGroup().isGroupBy() )
        addComparators( conf, "cascading.sort.comparator", getGroup().getSortingSelectors() );

      if( !getGroup().isGroupBy() )
        {
        conf.setPartitionerClass( CoGroupingPartitioner.class );
        conf.setMapOutputKeyClass( IndexTuple.class ); // allows groups to be sorted by index
        conf.setMapOutputValueClass( IndexTuple.class );
        conf.setOutputKeyComparatorClass( IndexTupleCoGroupingComparator.class ); // sorts by group, then by index
        conf.setOutputValueGroupingComparator( CoGroupingComparator.class );
        }

      if( getGroup().isSorted() )
        {
        conf.setPartitionerClass( GroupingSortingPartitioner.class );
        conf.setMapOutputKeyClass( TuplePair.class );

        if( getGroup().isSortReversed() )
          conf.setOutputKeyComparatorClass( ReverseGroupingSortingComparator.class );
        else
          conf.setOutputKeyComparatorClass( GroupingSortingComparator.class );

        // no need to supply a reverse comparator, only equality is checked
        conf.setOutputValueGroupingComparator( GroupingComparator.class );
        }
      }

    // perform last so init above will pass to tasks
    String versionString = Version.getRelease();

    if( versionString != null )
      conf.set( "cascading.version", versionString );

    conf.set( CASCADING_FLOW_STEP_ID, getID() );
    conf.set( "cascading.flow.step.num", Integer.toString( getStepNum() ) );

    String stepState = pack( this, conf );

    // hadoop 20.2 doesn't like dist cache when using local mode
    int maxSize = Short.MAX_VALUE;
    if( isHadoopLocalMode( conf ) || stepState.length() < maxSize ) // seems safe
      conf.set( "cascading.flow.step", stepState );
    else
      conf.set( "cascading.flow.step.path", writeStateToDistCache( conf, getID(), stepState ) );

    return conf;
    }

  public boolean isHadoopLocalMode( JobConf conf )
    {
    return HadoopUtil.isLocal( conf );
    }

  private String pack( Object object, JobConf conf )
    {
    try
      {
      return serializeBase64( object, conf, true );
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to pack object: " + object.getClass().getCanonicalName(), exception );
      }
    }

  protected FlowStepJob<JobConf> createFlowStepJob( FlowProcess<JobConf> flowProcess, JobConf parentConfig )
    {
    JobConf initializedConfig = getInitializedConfig( flowProcess, parentConfig );

    setConf( initializedConfig );

    return new HadoopFlowStepJob( createClientState( flowProcess ), this, initializedConfig );
    }

  /**
   * Method clean removes any temporary files used by this FlowStep instance. It will log any IOExceptions thrown.
   *
   * @param config of type JobConf
   */
  public void clean( JobConf config )
    {
    String stepStatePath = config.get( "cascading.flow.step.path" );

    if( stepStatePath != null )
      {
      try
        {
        HadoopUtil.removeStateFromDistCache( config, stepStatePath );
        }
      catch( IOException exception )
        {
        logWarn( "unable to remove step state file: " + stepStatePath, exception );
        }
      }

    if( tempSink != null )
      {
      try
        {
        tempSink.deleteResource( config );
        }
      catch( Exception exception )
        {
        // sink all exceptions, don't fail app
        logWarn( "unable to remove temporary file: " + tempSink, exception );
        }
      }

    if( getSink() instanceof TempHfs &&
      ( getFlow().getFlowStats().isSuccessful() || getFlow().getRunID() == null ) )
      {
      try
        {
        getSink().deleteResource( config );
        }
      catch( Exception exception )
        {
        // sink all exceptions, don't fail app
        logWarn( "unable to remove temporary file: " + getSink(), exception );
        }
      }
    else
      {
      cleanTapMetaData( config, getSink() );
      }

    for( Tap tap : getMapperTraps().values() )
      cleanTapMetaData( config, tap );

    for( Tap tap : getReducerTraps().values() )
      cleanTapMetaData( config, tap );

    }

  private void cleanTapMetaData( JobConf jobConf, Tap tap )
    {
    try
      {
      Hadoop18TapUtil.cleanupTapMetaData( jobConf, tap );
      }
    catch( IOException exception )
      {
      // ignore exception
      }
    }

  private void addComparators( JobConf conf, String property, Map<String, Fields> map )
    {
    Iterator<Fields> fieldsIterator = map.values().iterator();

    if( !fieldsIterator.hasNext() )
      return;

    Fields fields = fieldsIterator.next();

    if( fields.hasComparators() )
      {
      conf.set( property, pack( fields, conf ) );
      return;
      }

    // use resolved fields if there are no comparators.
    Set<Scope> previousScopes = getPreviousScopes( getGroup() );

    fields = previousScopes.iterator().next().getOutValuesFields();

    if( fields.size() != 0 ) // allows fields.UNKNOWN to be used
      conf.setInt( property + ".size", fields.size() );
    }

  private void initFromTraps( FlowProcess<JobConf> flowProcess, JobConf conf, Map<String, Tap> traps )
    {
    if( !traps.isEmpty() )
      {
      JobConf trapConf = HadoopUtil.copyJobConf( conf );

      for( Tap tap : traps.values() )
        tap.sinkConfInit( flowProcess, trapConf );
      }
    }

  protected void initFromSources( FlowProcess<JobConf> flowProcess, JobConf conf )
    {
    // handles case where same tap is used on multiple branches
    // we do not want to init the same tap multiple times
    Set<Tap> uniqueSources = getUniqueStreamedSources();

    JobConf[] streamedJobs = new JobConf[ uniqueSources.size() ];
    int i = 0;

    for( Tap tap : uniqueSources )
      {
      if( tap.getIdentifier() == null )
        throw new IllegalStateException( "tap may not have null identifier: " + tap.toString() );

      streamedJobs[ i ] = flowProcess.copyConfig( conf );
      tap.sourceConfInit( flowProcess, streamedJobs[ i ] );
      streamedJobs[ i ].set( "cascading.step.source", Tap.id( tap ) );
      i++;
      }

    Set<Tap> accumulatedSources = getAllAccumulatedSources();

    for( Tap tap : accumulatedSources )
      {
      JobConf accumulatedJob = flowProcess.copyConfig( conf );
      tap.sourceConfInit( flowProcess, accumulatedJob );

      if( !HadoopUtil.isLocal( accumulatedJob ))
        distCacheHashJoinInputs(tap, accumulatedJob, conf);

      Map<String, String> map = flowProcess.diffConfigIntoMap( conf, accumulatedJob );
      conf.set( "cascading.step.accumulated.source.conf." + Tap.id( tap ), pack( map, conf ) );
      }

    MultiInputFormat.addInputFormat( conf, streamedJobs ); //must come last
    }

  public Tap getTapForID( Set<Tap> taps, String id )
    {
    for( Tap tap : taps )
      {
      if( Tap.id( tap ).equals( id ) )
        return tap;
      }

    return null;
    }

  private void initFromProcessConfigDef( final JobConf conf )
    {
    initConfFromProcessConfigDef( getSetterFor( conf ) );
    }

  private ConfigDef.Setter getSetterFor( final JobConf conf )
    {
    return new ConfigDef.Setter()
    {
    @Override
    public String set( String key, String value )
      {
      String oldValue = get( key );

      conf.set( key, value );

      return oldValue;
      }

    @Override
    public String update( String key, String value )
      {
      String oldValue = get( key );

      if( oldValue == null )
        conf.set( key, value );
      else if( !oldValue.contains( value ) )
        conf.set( key, oldValue + "," + value );

      return oldValue;
      }

    @Override
    public String get( String key )
      {
      String value = conf.get( key );

      if( value == null || value.isEmpty() )
        return null;

      return value;
      }
    };
    }

  /**
   * sources are specific to step, remove all known accumulated sources, if any
   *
   * @return
   */
  private Set<Tap> getUniqueStreamedSources()
    {
    HashSet<Tap> set = new HashSet<Tap>( sources.keySet() );

    set.removeAll( getAllAccumulatedSources() );

    return set;
    }

  protected void initFromSink( FlowProcess<JobConf> flowProcess, JobConf conf )
    {
    // init sink first so tempSink can take precedence
    if( getSink() != null )
      getSink().sinkConfInit( flowProcess, conf );

    if( FileOutputFormat.getOutputPath( conf ) == null )
      tempSink = new TempHfs( conf, "tmp:/" + new Path( getSink().getIdentifier() ).toUri().getPath(), true );

    // tempSink exists because sink is writeDirect
    if( tempSink != null )
      tempSink.sinkConfInit( flowProcess, conf );
    }

  protected void initFromTraps( FlowProcess<JobConf> flowProcess, JobConf conf )
    {
    initFromTraps( flowProcess, conf, getMapperTraps() );
    initFromTraps( flowProcess, conf, getReducerTraps() );
    }

  @Override
  public Set<Tap> getTraps()
    {
    Set<Tap> set = new HashSet<Tap>();

    set.addAll( mapperTraps.values() );
    set.addAll( reducerTraps.values() );

    return Collections.unmodifiableSet( set );
    }

  @Override
  public Tap getTrap( String name )
    {
    Tap trap = getMapperTrap( name );

    if( trap == null )
      trap = getReducerTrap( name );

    return trap;
    }

  public Map<String, Tap> getMapperTraps()
    {
    return mapperTraps;
    }

  public Map<String, Tap> getReducerTraps()
    {
    return reducerTraps;
    }

  public Tap getMapperTrap( String name )
    {
    return getMapperTraps().get( name );
    }

  public Tap getReducerTrap( String name )
    {
    return getReducerTraps().get( name );
    }

  private static void distCacheHashJoinInputs(Tap tap, JobConf accumulatedConf, JobConf stepConf)
    {
    String tapHashPrefix = "hashjointap" + tap.getIdentifier().hashCode();
    Path[] origPaths = FileInputFormat.getInputPaths( accumulatedConf );
    List<Path> cacheInput = new ArrayList<Path>( origPaths.length );
    for( Path p : origPaths )
      {
      URI uri = p.toUri();
      FileSystem fs;
      try
        {
        fs = FileSystem.get( uri, accumulatedConf );
        FileStatus[] stats = fs.listStatus( p );
        if( stats == null || stats.length == 0 )
          cacheInput.add(p);
        else
          for( FileStatus ifs : stats )
            {
            if( ifs.isDir() )
              continue;

            // hdfs://<host>/<path>#hashjointapNNNN-UUID
            String cacheUriStr = String.format("%s#%s-%s",
                fs.makeQualified( ifs.getPath() ).toString(),
                tapHashPrefix,
                Util.createUniqueID() );

            URI cacheUri = new URI(cacheUriStr);
            cacheInput.add( new Path( DistributedCacheFileSystem.DCFS_ROOT + cacheUri.getFragment()) );
            DistributedCache.addCacheFile( cacheUri, stepConf );
            }
        }
      catch( FileNotFoundException fnf)
        {
        // intermediate step will be handled later
        return;
        }
      catch( IOException infeasible )
        {
        throw new IllegalArgumentException( uri + ": fileSystem failure",
            infeasible );
        }
      catch (URISyntaxException infeasible)
        {
        throw new IllegalArgumentException( uri + ": URI formation failure",
            infeasible );
        }
      }
    FileInputFormat.setInputPaths(accumulatedConf, cacheInput.toArray(new Path[cacheInput.size()]));
    accumulatedConf.setClass(DistributedCacheFileSystem.DCFS_IMPL,
        DistributedCacheFileSystem.class, FileSystem.class);
    }
  /**
   * FileSystem API to DistributedCache-based Hfs
   */
  public static class DistributedCacheFileSystem extends FileSystem
    {
    public static final String DCFS_SCHEME = "cascadingdcfs";
    public static final String DCFS_IMPL =
        String.format( "fs.%s.impl", DCFS_SCHEME );

    private static final Path DCFS_ROOT = new Path( DCFS_SCHEME + ":///" );

    @Override
    public URI getUri()
      {
      return DCFS_ROOT.toUri();
      }

    @Override
    public FSDataInputStream open( Path f, int bufferSize )
        throws IOException
      {
      Path qualifiedPath = makeQualified( f );
      Path localPath = getLocalPath( qualifiedPath );
      if (localPath == null)
        throw new FileNotFoundException( qualifiedPath + ": not found." );
      else
        return FileSystem.getLocal( getConf() ).open( localPath );
      }

    @Override
    public FSDataOutputStream create(
      Path f,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException
      {
      throw new UnsupportedOperationException();
      }

    @Override
    public FSDataOutputStream append(
      Path f,
      int bufferSize,
      Progressable progress )
      throws IOException
      {
      throw new UnsupportedOperationException();
      }

    @Override
    public boolean rename( Path src, Path dst ) throws IOException
      {
      throw new UnsupportedOperationException();
      }

    @Override
    public boolean delete( Path f ) throws IOException
      {
      throw new UnsupportedOperationException();
      }

    @Override
    public boolean delete( Path f, boolean recursive ) throws IOException
      {
      throw new UnsupportedOperationException();
      }

    /**
     * If f is a cached file, return local file status, otherwise empty array
     * if f is the root dif, return all cached file statuses
     *
     * @param f
     * @return array of file statuses
     * @throws IOException
     */
    @Override
    public FileStatus[] listStatus( Path f ) throws IOException
      {
      Path qualifiedPath = makeQualified( f );
      if( qualifiedPath.getParent() == null )
        {
        Path[] localPaths = DistributedCache.getLocalCacheFiles( getConf() );
        if (localPaths == null && localPaths.length == 0)
          return new FileStatus[0];

        FileSystem lfs = FileSystem.getLocal( getConf() );
        FileStatus[] stats = new FileStatus[ localPaths.length ];
        for (int i = 0; i < localPaths.length; i++)
          stats[i] = lfs.getFileStatus( localPaths[ i ] );
        return stats;
        }
      else
        return new FileStatus[] { getFileStatus( qualifiedPath ) };
      }

    @Override
    public void setWorkingDirectory( Path new_dir )
      {
      if( makeQualified(new_dir).getParent() != null )
        throw new UnsupportedOperationException();
      }

    @Override
    public Path getWorkingDirectory()
      {
      return DCFS_ROOT;
      }

    @Override
    public boolean mkdirs( Path f, FsPermission permission ) throws IOException
      {
      throw new UnsupportedOperationException();
      }

    @Override
    public FileStatus getFileStatus( Path f ) throws IOException
      {
      Path qualifiedPath = makeQualified(f);

      if( qualifiedPath.getParent() == null )
        {
        // long length, boolean isdir, int block_replication,
        // long blocksize, long modification_time, Path path
        return new FileStatus( 0, true, 0, 0, 0, qualifiedPath );
        }

      Path localPath = getLocalPath( qualifiedPath );

      if( localPath == null )
        throw new FileNotFoundException( qualifiedPath + ": not found." );
      else
        return FileSystem.getLocal(getConf()).getFileStatus( localPath );

      }

    private Path getLocalPath( Path p ) throws IOException
      {
      Path[] localPaths = DistributedCache.getLocalCacheFiles( getConf() );

      if( LOG.isDebugEnabled() )
        LOG.debug("getLocalPath: Checking dist cache " + Arrays.toString(localPaths) + " for : " + p );

      if( localPaths != null )
        for( Path lp : localPaths )
          if( p.getName().equals(lp.getName()) )
            return lp;

      return null;
      }
    }
  }
