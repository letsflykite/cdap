/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.app.mapreduce;

import co.cask.cdap.api.dataset.lib.cube.TagValue;
import co.cask.cdap.api.dataset.lib.cube.TimeValue;
import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricSearchQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.api.metrics.MetricType;
import co.cask.cdap.app.metrics.MapReduceMetrics;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.MRJobInfo;
import co.cask.cdap.proto.MRTaskInfo;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.hadoop.mapreduce.TaskCounter;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Retrieves information/reports for a MapReduce run via the Metrics system.
 */
public class MRJobMetricsReporter {

  private final MetricStore metricStore;

  @Inject
  public MRJobMetricsReporter(MetricStore metricStore) {
    this.metricStore = metricStore;
  }

  /**
   * @param runId for which information will be returned.
   * @return a {@link MRJobInfo} containing information about a particular MapReduce program run.
   */
  public MRJobInfo getMRJobInfo(Id.Run runId) throws Exception {
    Preconditions.checkArgument(ProgramType.MAPREDUCE.equals(runId.getProgram().getType()));

    // baseTagValues has tag keys: ns.app.mr.runid
    List<TagValue> baseTagValues = Lists.newArrayList();
    baseTagValues.add(new TagValue(Constants.Metrics.Tag.NAMESPACE, runId.getNamespace().getId()));
    baseTagValues.add(new TagValue(Constants.Metrics.Tag.APP, runId.getProgram().getApplicationId()));
    baseTagValues.add(new TagValue(Constants.Metrics.Tag.MAPREDUCE, runId.getProgram().getId()));
    baseTagValues.add(new TagValue(Constants.Metrics.Tag.RUN_ID, runId.getId()));

    List<TagValue> mapTagValues = Lists.newArrayList(baseTagValues);
    mapTagValues.add(new TagValue(Constants.Metrics.Tag.MR_TASK_TYPE, MapReduceMetrics.TaskType.Mapper.getId()));

    List<TagValue> reduceTagValues = Lists.newArrayList(baseTagValues);
    reduceTagValues.add(new TagValue(Constants.Metrics.Tag.MR_TASK_TYPE, MapReduceMetrics.TaskType.Reducer.getId()));

    List<String> mapTaskIds = getTaskIds(mapTagValues);
    List<String> reduceTaskIds = getTaskIds(reduceTagValues);

    // map from RunId -> (CounterName -> CounterValue)
    Map<String, Map<String, Long>> mapTaskMetrics = Maps.newHashMap();
    for (String mapTaskId : mapTaskIds) {
      mapTaskMetrics.put(mapTaskId, Maps.<String, Long>newHashMap());
    }
    Map<String, Map<String, Long>> reduceTaskMetrics = Maps.newHashMap();
    for (String reduceTaskId : reduceTaskIds) {
      reduceTaskMetrics.put(reduceTaskId, Maps.<String, Long>newHashMap());
    }

    // Populate mapTaskMetrics and reduce Task Metrics via MetricStore. Used to construct MRTaskInfo below.
    Map<String, String> mapperContext = tagValuesToMap(mapTagValues);
    putMetrics(queryGroupedAggregates(mapperContext, MapReduceMetrics.METRIC_INPUT_RECORDS),
               mapTaskMetrics, TaskCounter.MAP_INPUT_RECORDS.name());
    putMetrics(queryGroupedAggregates(mapperContext, MapReduceMetrics.METRIC_OUTPUT_RECORDS),
               mapTaskMetrics, TaskCounter.MAP_OUTPUT_RECORDS.name());
    putMetrics(queryGroupedAggregates(mapperContext, MapReduceMetrics.METRIC_BYTES),
               mapTaskMetrics, TaskCounter.MAP_OUTPUT_BYTES.name());

    Map<String, Long> mapProgress = queryGroupedAggregates(mapperContext, MapReduceMetrics.METRIC_TASK_COMPLETION);


    Map<String, String> reducerContext = tagValuesToMap(reduceTagValues);
    putMetrics(queryGroupedAggregates(reducerContext, MapReduceMetrics.METRIC_INPUT_RECORDS),
               reduceTaskMetrics, TaskCounter.REDUCE_INPUT_RECORDS.name());
    putMetrics(queryGroupedAggregates(reducerContext, MapReduceMetrics.METRIC_OUTPUT_RECORDS),
               reduceTaskMetrics, TaskCounter.REDUCE_OUTPUT_RECORDS.name());

    Map<String, Long> reduceProgress = queryGroupedAggregates(reducerContext, MapReduceMetrics.METRIC_TASK_COMPLETION);


    // Construct MRTaskInfos from the information we can get from Metric system.
    List<MRTaskInfo> mapTaskInfos = Lists.newArrayList();
    for (String mapTaskId : mapTaskIds) {
      mapTaskInfos.add(new MRTaskInfo(mapTaskId, null, null, null,
                                      mapProgress.get(mapTaskId) / 100.0F, mapTaskMetrics.get(mapTaskId)));
    }

    List<MRTaskInfo> reduceTaskInfos = Lists.newArrayList();
    for (String reduceTaskId : reduceTaskIds) {
      reduceTaskInfos.add(new MRTaskInfo(reduceTaskId, null, null, null,
                                         reduceProgress.get(reduceTaskId) / 100.0F,
                                         reduceTaskMetrics.get(reduceTaskId)));
    }

    return getJobCounters(tagValuesToMap(baseTagValues), mapTaskInfos, reduceTaskInfos);
  }


  private Map<String, String> tagValuesToMap(List<TagValue> tagValues) {
    Map<String, String> context = Maps.newHashMap();
    for (TagValue tagValue : tagValues) {
      context.put(tagValue.getTagName(), tagValue.getValue());
    }
    return context;
  }

  /**
   * Copies metric values from {@param taskMetrics} to {@param allTaskMetrics}.
   * This is necessary because we read metrics from MetricStore in batch (one query for all map or reduce tasks
   * via the MetricStore GroupBy functionality).
   * @param taskMetrics mapping: TaskId -> metricValue
   * @param allTaskMetrics mapping: TaskId -> (CounterName -> CounterValue)
   * @param counterName the name of the key to copy over
   */
  private void putMetrics(Map<String, Long> taskMetrics, Map<String, Map<String, Long>> allTaskMetrics,
                          String counterName) {
    for (Map.Entry<String, Long> entry : taskMetrics.entrySet()) {
      allTaskMetrics.get(entry.getKey()).put(counterName, entry.getValue());
    }

    // tasks may not be returned from the above query if their metric values are 0.
    for (Map.Entry<String, Map<String, Long>> entry : allTaskMetrics.entrySet()) {
      Map<String, Long> metricsMap = entry.getValue();
      if (!metricsMap.containsKey(counterName)) {
        metricsMap.put(counterName, 0L);
      }
    }
  }

  // Context -> list of instances
  private List<String> getTaskIds(List<TagValue> tagValues) throws Exception {
    List<String> taskIds = Lists.newArrayList();
    MetricSearchQuery metricSearchQuery = new MetricSearchQuery(0, Integer.MAX_VALUE, Integer.MAX_VALUE, tagValues);
    Collection<TagValue> nextAvailableTags = metricStore.findNextAvailableTags(metricSearchQuery);
    for (TagValue tagValue : nextAvailableTags) {
      if (Constants.Metrics.Tag.INSTANCE_ID.equals(tagValue.getTagName())) {
        taskIds.add(tagValue.getValue());
      }
    }
    return taskIds;
  }

  private MRJobInfo getJobCounters(Map<String, String> jobContext, List<MRTaskInfo> mapTaskInfos,
                                   List<MRTaskInfo> reduceTaskInfos) throws Exception {
    Map<String, String> mapContext = Maps.newHashMap(jobContext);
    mapContext.put(Constants.Metrics.Tag.MR_TASK_TYPE, MapReduceMetrics.TaskType.Mapper.getId());
    Map<String, String> reduceContext = Maps.newHashMap(jobContext);
    reduceContext.put(Constants.Metrics.Tag.MR_TASK_TYPE, MapReduceMetrics.TaskType.Reducer.getId());

    HashMap<String, Long> metrics = Maps.newHashMap();

    metrics.put(TaskCounter.MAP_INPUT_RECORDS.name(),
                getAggregates(mapContext, MapReduceMetrics.METRIC_INPUT_RECORDS));
    metrics.put(TaskCounter.MAP_OUTPUT_RECORDS.name(),
                getAggregates(mapContext, MapReduceMetrics.METRIC_OUTPUT_RECORDS));
    metrics.put(TaskCounter.MAP_OUTPUT_BYTES.name(),
                getAggregates(mapContext, MapReduceMetrics.METRIC_BYTES));

    metrics.put(TaskCounter.REDUCE_INPUT_RECORDS.name(),
                getAggregates(reduceContext, MapReduceMetrics.METRIC_INPUT_RECORDS));
    metrics.put(TaskCounter.REDUCE_OUTPUT_RECORDS.name(),
                getAggregates(reduceContext, MapReduceMetrics.METRIC_OUTPUT_RECORDS));

    float mapProgress = getAggregates(mapContext, MapReduceMetrics.METRIC_COMPLETION) / 100.0F;
    float reduceProgress = getAggregates(reduceContext, MapReduceMetrics.METRIC_COMPLETION) / 100.0F;

    return new MRJobInfo(null, null, null, mapProgress, reduceProgress, metrics, mapTaskInfos, reduceTaskInfos);
  }

  private String prependSystem(String metric) {
    return "system." + metric;
  }

  private long getAggregates(Map<String, String> context, String metric) throws Exception {
    MetricDataQuery metricDataQuery =
      new MetricDataQuery(0, Integer.MAX_VALUE, Integer.MAX_VALUE, prependSystem(metric), MetricType.COUNTER, context,
                          ImmutableList.<String>of());
    Collection<MetricTimeSeries> query = metricStore.query(metricDataQuery);
    if (query.isEmpty()) {
      return 0;
    }
    MetricTimeSeries timeSeries = Iterables.getOnlyElement(query);
    List<TimeValue> timeValues = timeSeries.getTimeValues();
    TimeValue timeValue = Iterables.getOnlyElement(timeValues);
    return timeValue.getValue();
  }

  // queries MetricStore for one metric across all tasks of a certain TaskType, using GroupBy InstanceId
  private Map<String, Long> queryGroupedAggregates(Map<String, String> context, String metric) throws Exception {
    MetricDataQuery metricDataQuery =
      new MetricDataQuery(0, Integer.MAX_VALUE, Integer.MAX_VALUE, prependSystem(metric), MetricType.GAUGE, context,
                          ImmutableList.of(Constants.Metrics.Tag.INSTANCE_ID));
    Collection<MetricTimeSeries> query = metricStore.query(metricDataQuery);

    // runId -> metricValue
    Map<String, Long> taskMetrics = Maps.newHashMap();
    for (MetricTimeSeries metricTimeSeries : query) {
      List<TimeValue> timeValues = metricTimeSeries.getTimeValues();
      TimeValue timeValue = Iterables.getOnlyElement(timeValues);
      String taskId = metricTimeSeries.getTagValues().get(Constants.Metrics.Tag.INSTANCE_ID);
      taskMetrics.put(taskId, timeValue.getValue());
    }
    return taskMetrics;
  }
}
