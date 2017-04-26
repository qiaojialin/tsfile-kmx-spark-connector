package cn.edu.thu.tsfile.spark;

import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.thu.tsfile.spark.common.BasicOperator;
import cn.edu.thu.tsfile.spark.common.FilterOperator;
import cn.edu.thu.tsfile.spark.common.SingleQuery;
import cn.edu.thu.tsfile.spark.common.TSQueryPlan;
import cn.edu.thu.tsfile.spark.exception.QueryOperatorException;
import cn.edu.thu.tsfile.spark.exception.QueryProcessorException;
import cn.edu.thu.tsfile.spark.optimizer.DNFFilterOptimizer;
import cn.edu.thu.tsfile.spark.optimizer.MergeSingleFilterOptimizer;
import cn.edu.thu.tsfile.spark.optimizer.RemoveNotOptimizer;
import cn.edu.thu.tsfile.timeseries.read.metadata.SeriesSchema;
import cn.edu.thu.tsfile.timeseries.read.qp.SQLConstant;
import cn.edu.thu.tsfile.timeseries.read.query.QueryEngine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * This class is used to convert information given by sparkSQL to construct TSFile's query plans.
 * For TSFile's schema differ from SparkSQL's table schema
 * e.g.
 * TSFile's SQL: select s1,s2 from root.car.d1 where s1 = 10
 * SparkSQL's SQL: select s1,s2 from XXX where delta_object = d1
 *
 * @author QJL、KR
 */
public class QueryProcessor {

    //determine whether to query all delta_objects from TSFile. true means do query.
    private boolean flag;

    //construct logical query plans first, then convert them to physical ones
    public List<TSQueryPlan> generatePlans(FilterOperator filter, List<String> paths, TSRandomAccessFileReader in, Long start, Long end) throws QueryProcessorException, IOException {

        List<TSQueryPlan> queryPlans = new ArrayList<>();

        if(filter != null) {
            RemoveNotOptimizer removeNot = new RemoveNotOptimizer();
            filter = removeNot.optimize(filter);

            DNFFilterOptimizer dnf = new DNFFilterOptimizer();
            filter = dnf.optimize(filter);
            MergeSingleFilterOptimizer merge = new MergeSingleFilterOptimizer();
            filter = merge.optimize(filter);

            List<FilterOperator> filterOperators = splitFilter(filter);

            for (FilterOperator filterOperator : filterOperators) {
                SingleQuery singleQuery = constructSelectPlan(filterOperator);
                if (singleQuery != null) {
                    queryPlans.addAll(physicalOptimize(singleQuery, paths, in, start, end));
                }
            }
        } else {
            queryPlans.addAll(physicalOptimize(null, paths, in, start, end));
        }

        return queryPlans;
    }

    private List<FilterOperator> splitFilter(FilterOperator filterOperator) {
        if (filterOperator.isSingle() || filterOperator.getTokenIntType() != SQLConstant.KW_OR) {
            List<FilterOperator> ret = new ArrayList<FilterOperator>();
            ret.add(filterOperator);
            return ret;
        }
        // a list of conjunctions linked by or
        return filterOperator.childOperators;
    }

    private SingleQuery constructSelectPlan(FilterOperator filterOperator) throws QueryOperatorException {

        FilterOperator timeFilter = null;
        FilterOperator valueFilter = null;
        FilterOperator deltaObjectFilterOperator = null;
        List<FilterOperator> singleFilterList = null;

        if (filterOperator.isSingle()) {
            singleFilterList = new ArrayList<>();
            singleFilterList.add(filterOperator);

        } else if (filterOperator.getTokenIntType() == SQLConstant.KW_AND) {
            // original query plan has been dealt with merge optimizer, thus all nodes with same path have been
            // merged to one node
            singleFilterList = filterOperator.getChildren();
        }

        if(singleFilterList == null) {
            return null;
        }

        List<FilterOperator> valueList = new ArrayList<>();
        for (FilterOperator child : singleFilterList) {
            if (!child.isSingle()) {
                valueList.add(child);
            } else {
                switch (child.getSinglePath()) {
                    case SQLConstant.RESERVED_TIME:
                        if (timeFilter != null) {
                            throw new QueryOperatorException(
                                    "time filter has been specified more than once");
                        }
                        timeFilter = child;
                        break;
                    case SQLConstant.RESERVED_DELTA_OBJECT:
                        if (deltaObjectFilterOperator != null) {
                            throw new QueryOperatorException(
                                    "delta object filter has been specified more than once");
                        }
                        deltaObjectFilterOperator = child;
                        break;
                    default:
                        valueList.add(child);
                        break;
                }
            }
        }

        if (valueList.size() == 1) {
            valueFilter = valueList.get(0);

        } else if (valueList.size() > 1) {
            valueFilter = new FilterOperator(SQLConstant.KW_AND, false);
            valueFilter.childOperators = valueList;
        }

        return new SingleQuery(deltaObjectFilterOperator, timeFilter, valueFilter);
    }

    private List<TSQueryPlan> physicalOptimize(SingleQuery singleQuery, List<String> paths, TSRandomAccessFileReader in, Long start, Long end) throws IOException {
        List<String> actualDeltaObjects = new ArrayList<>();

        List<String> validPaths = new ArrayList<>();
        for(String path: paths) {
            if(!path.equals(SQLConstant.RESERVED_DELTA_OBJECT) && !path.equals(SQLConstant.RESERVED_TIME)) {
                validPaths.add(path);
            }
        }
        FilterOperator timeFilter = null;
        FilterOperator valueFilter = null;

        QueryEngine queryEngine = new QueryEngine(in);

        //get all series in current tsfile
        List<SeriesSchema> seriesList = queryEngine.getAllSeries();

        if(singleQuery != null) {
            timeFilter = singleQuery.getTimeFilterOperator();
            valueFilter = singleQuery.getValueFilterOperator();
            if (valueFilter != null) {
                List<String> filterPaths = valueFilter.getAllPaths();
                List<String> actualPaths = new ArrayList<>();
                for (SeriesSchema series : seriesList) {
                    actualPaths.add(series.name);
                }

                //if filter paths doesn't in tsfile, don't query
                if (!actualPaths.containsAll(filterPaths))
                    return new ArrayList<>();
            }

            flag = true;
            Set<String> selectDeltaObjects = mergeDeltaObject(singleQuery.getDeltaObjectFilterOperator());
            if(!flag) {
                return new ArrayList<>();
            }

            //if select deltaObject, then match with measurement
            if(selectDeltaObjects != null && selectDeltaObjects.size() >= 1) {
                actualDeltaObjects.addAll(selectDeltaObjects);
            } else {
                actualDeltaObjects.addAll(queryEngine.getAllDeltaObjectUIDByPartition(start, end));
            }
        } else {
            actualDeltaObjects.addAll(queryEngine.getAllDeltaObject());
        }

        List<SeriesSchema> fileSeries = queryEngine.getAllSeries();
        Set<String> seriesSet = new HashSet<>();
        for(SeriesSchema series: fileSeries) {
            seriesSet.add(series.name);
        }

        //query all measurements from TSFile
        if(validPaths.size() == 0) {
            for(SeriesSchema series: seriesList) {
                validPaths.add(series.name);
            }
        } else {
            //remove paths that doesn't exist in file
            validPaths.removeIf(path -> !seriesSet.contains(path));
        }


        List<TSQueryPlan> tsFileQueries = new ArrayList<>();
        for(String deltaObject: actualDeltaObjects) {
            List<String> newPaths = new ArrayList<>();
            for(String path: validPaths) {
                String newPath = deltaObject + SQLConstant.PATH_SEPARATOR + path;
                newPaths.add(newPath);
            }
            if(valueFilter == null) {
                tsFileQueries.add(new TSQueryPlan(newPaths, timeFilter, null));
            } else {
                FilterOperator newValueFilter = valueFilter.clone();
                newValueFilter.addHeadDeltaObjectPath(deltaObject);
                tsFileQueries.add(new TSQueryPlan(newPaths, timeFilter, newValueFilter));
            }
        }
        return tsFileQueries;
    }


    private Set<String> mergeDeltaObject(FilterOperator deltaFilterOperator) {
        if (deltaFilterOperator == null) {
            return null;
        }
        if (deltaFilterOperator.isLeaf()) {
            Set<String> r = new HashSet<>();
            r.add(((BasicOperator)deltaFilterOperator).getSeriesValue());
            return r;
        }
        List<FilterOperator> children = deltaFilterOperator.getChildren();
        if (children == null || children.isEmpty()) {
            return new HashSet<>();
        }
        Set<String> ret = mergeDeltaObject(children.get(0));
        if(ret == null){
            return null;
        }
        for (int i = 1; i < children.size(); i++) {
            Set<String> temp = mergeDeltaObject(children.get(i));
            if(temp == null) {
                return null;
            }
            switch (deltaFilterOperator.getTokenIntType()) {
                case SQLConstant.KW_AND:
                    ret.retainAll(temp);
                    //example: "where delta_object = d1 and delta_object = d2" should not query data
                    if(ret.isEmpty()) {
                        flag = false;
                    }
                    break;
                case SQLConstant.KW_OR:
                    ret.addAll(temp);
                    break;
                default:
                    throw new UnsupportedOperationException("given error token type:"+deltaFilterOperator.getTokenIntType());
            }
        }
        return ret;
    }
}
